from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import TYPE_CHECKING, cast

from dbos import DBOS, DBOSClient
from dr_store.object_store import ObjectStore
from dr_store.storage_backends.postgresql import PostgresBackend

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
from dr_platform.execution._object_store import (
    _bind_object_store,
    _ObjectStoreBinding,
)
from dr_platform.execution._recovery_cap import validate_registry_recovery_cap
from dr_platform.execution.handoff import (
    _pipeline_checkpoint_workflows,
    _pipeline_stage_workflows,
    is_pipeline_wrapped,
)
from dr_platform.recovery.sweep import (
    DEFAULT_SWEEP_BATCH_SIZE,
    sweep_abandoned_run_completions,
    sweep_abandoned_stages,
)
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    validate_database_colocation,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.admission.runner import AdmissionSummary
    from dr_platform.completion.barrier import RunBarrierSummary
    from dr_platform.pipeline.registry import PipelineRegistry
    from dr_platform.recovery.live_identity import LiveDbosIdentity

logger = logging.getLogger(__name__)

DEFAULT_DISPATCHER_CRON = "*/1 * * * * *"
DEFAULT_RUN_BARRIER_CRON = "*/1 * * * * *"
DEFAULT_SWEEP_CRON = "*/1 * * * * *"
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


def _registry_stage_workflows(
    registry: PipelineRegistry,
) -> tuple[Callable[..., object], ...]:
    return tuple(
        workflow
        for pipeline in registry.pipelines()
        for workflow in _pipeline_stage_workflows(pipeline)
    )


def _log_admission_summary(summary: AdmissionSummary) -> None:
    logger.info(
        "admission pass admitted=%s skipped_capacity=%s "
        "skipped_pause=%s unconfigured=%s failed=%s mismatched=%s",
        summary.admitted_total,
        summary.skipped_for_capacity,
        summary.skipped_for_pause,
        len(summary.unconfigured_stages),
        len(summary.failed_stages),
        len(summary.mismatched_stages),
    )
    if summary.unconfigured_stages:
        logger.warning(
            "admission skipped stages without an empty-selector control: %s",
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


def _log_barrier_summary(summary: RunBarrierSummary) -> None:
    logger.info(
        "run barrier pass cursor_acquired=%s examined=%s "
        "releases=%s failures=%s",
        summary.cursor_acquired,
        summary.candidates_examined,
        len(summary.releases),
        len(summary.failures),
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


def _validate_dispatcher_settings(  # noqa: PLR0913 -- explicit dispatcher settings
    *,
    config: PlatformDbosConfig,
    engine: Engine,
    batch_size: int,
    barrier_batch_size: int,
    barrier_candidate_budget: int,
    sweep_cron: str | None,
    sweep_batch_size: int,
) -> None:
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


@dataclass(frozen=True, slots=True)
class DispatcherRegistration:
    """Owns dispatcher and ledger-checkpoint runtime resources."""

    client: DBOSClient
    workflow: ScheduledWorkflow
    barrier_workflow: ScheduledWorkflow
    sweep_workflow: ScheduledWorkflow | None
    _resources: _DispatcherResources = field(repr=False, compare=False)

    def close(self) -> None:
        self._resources.close()


class _DispatcherResources:
    def __init__(
        self,
        *,
        client: DBOSClient,
        checkpoint_executor: _LedgerCheckpointExecutor,
        checkpoint_binding: _LedgerCheckpointBinding,
        object_store_binding: _ObjectStoreBinding,
        ownership_token: object,
    ) -> None:
        self.client = client
        self.checkpoint_executor = checkpoint_executor
        self.checkpoint_binding = checkpoint_binding
        self.object_store_binding = object_store_binding
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
            self.object_store_binding.release()
            _DISPATCHER_OWNERSHIP.release(self.ownership_token)
            self._closed = True


def register_scheduled_dispatcher(  # noqa: PLR0913
    *,
    config: PlatformDbosConfig,
    engine: Engine,
    registry: PipelineRegistry,
    live_dbos_identity: LiveDbosIdentity,
    cron: str = DEFAULT_DISPATCHER_CRON,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
    barrier_cron: str = DEFAULT_RUN_BARRIER_CRON,
    barrier_batch_size: int = DEFAULT_RUN_BARRIER_BATCH_SIZE,
    barrier_candidate_budget: int = DEFAULT_RUN_BARRIER_CANDIDATE_BUDGET,
    sweep_cron: str | None = DEFAULT_SWEEP_CRON,
    sweep_batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
) -> DispatcherRegistration:
    """Register admission, run-barrier, and abandoned-stage reconciliation."""
    _validate_dispatcher_settings(
        config=config,
        engine=engine,
        batch_size=batch_size,
        barrier_batch_size=barrier_batch_size,
        barrier_candidate_budget=barrier_candidate_budget,
        sweep_cron=sweep_cron,
        sweep_batch_size=sweep_batch_size,
    )
    _require_wrapped_registry(registry)
    validate_registry_recovery_cap(registry, config.max_recovery_attempts)
    ownership_token = _DISPATCHER_OWNERSHIP.reserve()
    client: DBOSClient | None = None
    checkpoint_executor: _LedgerCheckpointExecutor | None = None
    checkpoint_binding: _LedgerCheckpointBinding | None = None
    object_store_binding: _ObjectStoreBinding | None = None
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
        object_store = ObjectStore(PostgresBackend.open_sync(engine))
        object_store_binding = _bind_object_store(
            _registry_stage_workflows(registry),
            object_store,
        )
        resources = _DispatcherResources(
            client=client,
            checkpoint_executor=checkpoint_executor,
            checkpoint_binding=checkpoint_binding,
            object_store_binding=object_store_binding,
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
            _log_admission_summary(summary)

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
            _log_barrier_summary(summary)

        if sweep_cron is not None:

            @DBOS.scheduled(sweep_cron)
            @DBOS.workflow(name=SWEEP_WORKFLOW_NAME)
            def sweep(
                _scheduled_time: datetime,
                _actual_time: datetime,
            ) -> None:
                stage_summary = sweep_abandoned_stages(
                    engine,
                    client=client,
                    live_identity=live_dbos_identity,
                    batch_size=sweep_batch_size,
                )
                completion_summary = sweep_abandoned_run_completions(
                    engine,
                    client=client,
                    live_identity=live_dbos_identity,
                    batch_size=sweep_batch_size,
                )
                logger.info(
                    "abandoned-stage sweep inspected=%s projected=%s; "
                    "run-completion sweep inspected=%s projected=%s",
                    stage_summary.inspected_count,
                    stage_summary.projected_count,
                    completion_summary.inspected_count,
                    completion_summary.projected_count,
                )

            sweep_workflow = cast("ScheduledWorkflow", sweep)

        registration = DispatcherRegistration(
            client=client,
            workflow=cast("ScheduledWorkflow", dispatch),
            barrier_workflow=cast("ScheduledWorkflow", reconcile_run_barriers),
            sweep_workflow=sweep_workflow,
            _resources=resources,
        )
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
            if object_store_binding is not None:
                object_store_binding.release()
            _DISPATCHER_OWNERSHIP.release(ownership_token)
        raise
    else:
        return registration
