from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import TYPE_CHECKING, cast

from dbos import DBOS, DBOSClient

from dr_platform._core.validation import validate_positive_integer
from dr_platform.admission.runner import (
    DEFAULT_ADMISSION_BATCH_SIZE,
    run_admission_pass,
)
from dr_platform.completion.barrier import (
    DEFAULT_RUN_BARRIER_BATCH_SIZE,
    DEFAULT_RUN_BARRIER_CANDIDATE_BUDGET,
    run_barrier_pass,
)
from dr_platform.execution._checkpoint import (
    _bind_ledger_checkpoint_executor,
    _LedgerCheckpointBinding,
    _LedgerCheckpointExecutor,
    _preflight_ledger_checkpoint_executor,
)
from dr_platform.execution.handoff import (
    _pipeline_checkpoint_workflows,
    is_pipeline_wrapped,
)
from dr_platform.recovery.sweep import (
    DEFAULT_SWEEP_BATCH_SIZE,
    sweep_abandoned_stages,
)
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    validate_database_colocation,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.pipeline.registry import PipelineRegistry

logger = logging.getLogger(__name__)

DEFAULT_DISPATCHER_CRON = "*/1 * * * * *"
DEFAULT_RUN_BARRIER_CRON = "*/1 * * * * *"
DISPATCHER_WORKFLOW_NAME = "dr_platform_staging_dispatcher"
RUN_BARRIER_WORKFLOW_NAME = "dr_platform_run_barrier"
SWEEP_WORKFLOW_NAME = "dr_platform_staging_sweep"

ScheduledWorkflow = Callable[[datetime, datetime], None]


class _DispatcherOwnership:
    def __init__(self) -> None:
        self._lock = Lock()
        self._token: object | None = None

    def reserve(self) -> object:
        token = object()
        with self._lock:
            if self._token is not None:
                raise RuntimeError(
                    "a live dispatcher registration already owns this process"
                )
            self._token = token
        return token

    def release(self, token: object) -> None:
        with self._lock:
            if self._token is not token:
                raise RuntimeError("dispatcher ownership token mismatch")
            self._token = None

    @property
    def live(self) -> bool:
        with self._lock:
            return self._token is not None


_DISPATCHER_OWNERSHIP = _DispatcherOwnership()


class UnwrappedPipelineError(RuntimeError):
    """A raw pipeline would bypass completion and remain ADMITTED."""

    def __init__(self, *, pipeline_key: str, pipeline_version: int) -> None:
        self.pipeline_key = pipeline_key
        self.pipeline_version = pipeline_version
        super().__init__(
            "registry contains an unwrapped pipeline; register "
            "wrap_pipeline_workflows(...)'s return value: "
            f"{pipeline_key!r} version {pipeline_version}"
        )


def _require_wrapped_registry(registry: PipelineRegistry) -> None:
    for pipeline in registry.pipelines():
        if not is_pipeline_wrapped(pipeline):
            raise UnwrappedPipelineError(
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
            )


def _registry_checkpoint_workflows(
    registry: PipelineRegistry,
) -> tuple[Callable[..., object], ...]:
    return tuple(
        workflow
        for pipeline in registry.pipelines()
        for workflow in _pipeline_checkpoint_workflows(pipeline)
    )


@dataclass(frozen=True, slots=True)
class DispatcherRegistration:
    """Owns dispatcher and ledger-checkpoint runtime resources."""

    client: DBOSClient
    workflow: ScheduledWorkflow
    barrier_workflow: ScheduledWorkflow
    sweep_workflow: ScheduledWorkflow | None = None
    _resources: _DispatcherResources | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _close_lock: Lock = field(
        default_factory=Lock,
        init=False,
        repr=False,
        compare=False,
    )
    _closed: bool = field(
        default=False,
        init=False,
        repr=False,
        compare=False,
    )

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            if self._resources is None:
                self.client.destroy()
            else:
                self._resources.close()
            object.__setattr__(self, "_closed", True)


class _DispatcherResources:
    def __init__(
        self,
        *,
        client: DBOSClient,
        checkpoint_executor: _LedgerCheckpointExecutor,
        checkpoint_binding: _LedgerCheckpointBinding,
        ownership_token: object,
    ) -> None:
        self.client = client
        self.checkpoint_executor = checkpoint_executor
        self.checkpoint_binding = checkpoint_binding
        self.ownership_token = ownership_token
        self._close_lock = Lock()
        self._closed = False

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self.checkpoint_executor.close()
            self.client.destroy()
            self.checkpoint_binding.release()
            _DISPATCHER_OWNERSHIP.release(self.ownership_token)
            self._closed = True


def register_scheduled_dispatcher(  # noqa: PLR0913, PLR0915
    *,
    config: PlatformDbosConfig,
    engine: Engine,
    registry: PipelineRegistry,
    cron: str = DEFAULT_DISPATCHER_CRON,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
    barrier_cron: str = DEFAULT_RUN_BARRIER_CRON,
    barrier_batch_size: int = DEFAULT_RUN_BARRIER_BATCH_SIZE,
    barrier_candidate_budget: int = DEFAULT_RUN_BARRIER_CANDIDATE_BUDGET,
    sweep_cron: str | None = None,
    sweep_batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
) -> DispatcherRegistration:
    """Register admission and optional single-sweeper workflows."""
    validate_positive_integer(batch_size, label="admission batch size")
    validate_positive_integer(
        barrier_batch_size, label="run barrier batch size"
    )
    validate_positive_integer(
        barrier_candidate_budget, label="run barrier candidate budget"
    )
    if barrier_candidate_budget < barrier_batch_size:
        raise ValueError(
            "run barrier candidate budget must be at least the batch size"
        )
    required_checkpoint_workers = max(batch_size, barrier_batch_size)
    if config.pool_size < required_checkpoint_workers:
        raise ValueError(
            "pool size must be at least the larger of admission batch size "
            "and run barrier batch size"
        )
    if sweep_cron is not None:
        validate_positive_integer(sweep_batch_size, label="sweep batch size")
    validate_database_colocation(
        database_url=engine.url.render_as_string(hide_password=False),
        system_database_url=config.system_database_url,
    )
    _require_wrapped_registry(registry)
    ownership_token = _DISPATCHER_OWNERSHIP.reserve()
    client: DBOSClient | None = None
    checkpoint_executor: _LedgerCheckpointExecutor | None = None
    checkpoint_binding: _LedgerCheckpointBinding | None = None
    resources: _DispatcherResources | None = None
    sweep_workflow: ScheduledWorkflow | None = None
    try:
        checkpoint_workflows = _registry_checkpoint_workflows(registry)
        _preflight_ledger_checkpoint_executor(checkpoint_workflows)
        client = DBOSClient(system_database_url=config.system_database_url)
        checkpoint_executor = _LedgerCheckpointExecutor(
            max_workers=max(batch_size, barrier_batch_size)
        )
        checkpoint_binding = _bind_ledger_checkpoint_executor(
            checkpoint_workflows,
            checkpoint_executor,
        )
        resources = _DispatcherResources(
            client=client,
            checkpoint_executor=checkpoint_executor,
            checkpoint_binding=checkpoint_binding,
            ownership_token=ownership_token,
        )

        @DBOS.scheduled(cron)
        @DBOS.workflow(name=DISPATCHER_WORKFLOW_NAME)
        def dispatch(
            _scheduled_time: datetime,
            _actual_time: datetime,
        ) -> None:
            summary = run_admission_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=batch_size,
            )
            if summary.unconfigured_stages:
                logger.warning(
                    "admission skipped stages without an empty-selector "
                    "control: %s",
                    ", ".join(
                        f"{stage.pipeline_key!r} version "
                        f"{stage.pipeline_version} stage "
                        f"{stage.stage_key.value!r}"
                        for stage in summary.unconfigured_stages
                    ),
                )
            if summary.failed_stages:
                logger.warning(
                    "admission failed for stages: %s",
                    ", ".join(
                        f"{stage.pipeline_key!r} version "
                        f"{stage.pipeline_version} stage "
                        f"{stage.stage_key.value!r}: "
                        f"{stage.error_type}: {stage.message}"
                        for stage in summary.failed_stages
                    ),
                )
            if summary.mismatched_stages:
                logger.error(
                    "admission found registry/data drift for stages: %s",
                    ", ".join(
                        f"{stage.pipeline_key!r} version "
                        f"{stage.pipeline_version} stage "
                        f"{stage.stage_key.value!r}: {stage.message}"
                        for stage in summary.mismatched_stages
                    ),
                )

        @DBOS.scheduled(barrier_cron)
        @DBOS.workflow(name=RUN_BARRIER_WORKFLOW_NAME)
        def reconcile_run_barriers(
            _scheduled_time: datetime,
            _actual_time: datetime,
        ) -> None:
            summary = run_barrier_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=barrier_batch_size,
                candidate_budget=barrier_candidate_budget,
            )
            if summary.failures:
                logger.error(
                    "run barrier failed for runs: %s",
                    ", ".join(
                        f"{failure.run_key.value!r}: "
                        f"{failure.error_type}: {failure.message}"
                        for failure in summary.failures
                    ),
                )

        if sweep_cron is not None:

            @DBOS.scheduled(sweep_cron)
            @DBOS.workflow(name=SWEEP_WORKFLOW_NAME)
            def sweep(
                _scheduled_time: datetime,
                _actual_time: datetime,
            ) -> None:
                sweep_abandoned_stages(
                    engine,
                    client=client,
                    batch_size=sweep_batch_size,
                )

            sweep_workflow = cast("ScheduledWorkflow", sweep)

        registration = DispatcherRegistration(
            client=client,
            workflow=cast("ScheduledWorkflow", dispatch),
            barrier_workflow=cast("ScheduledWorkflow", reconcile_run_barriers),
            sweep_workflow=sweep_workflow,
        )
        object.__setattr__(registration, "_resources", resources)
    except Exception:
        if resources is not None:
            resources.close()
        else:
            if checkpoint_executor is not None:
                checkpoint_executor.close()
            if client is not None:
                client.destroy()
            if checkpoint_binding is not None:
                checkpoint_binding.release()
            _DISPATCHER_OWNERSHIP.release(ownership_token)
        raise
    else:
        return registration
