"""DBOS scheduled wiring for the single staging admission dispatcher.

The registration hook constructs and owns one ``DBOSClient`` from the
validated colocated system database URL.  Its returned registration handle
keeps that client alive and provides explicit cleanup.  The scheduled
workflow is deliberately only an adapter around ``run_admission_pass``.
A pass never aborts for one unhealthy stage: stages lacking an empty-selector
capacity control, and candidates whose ``args_for`` or enqueue raises, are
skipped and reported on the returned :class:`AdmissionSummary`.  This adapter
logs those signals as warnings so operators can act, and logs persisted-
position mismatches (registry/data drift) at ERROR, while healthy admission
still commits.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, cast

from dbos import DBOS, DBOSClient

from dr_platform.dbos_config import (
    PlatformDbosConfig,
    validate_database_colocation,
)
from dr_platform.staging.admission import (
    DEFAULT_ADMISSION_BATCH_SIZE,
    run_admission_pass,
)
from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.handoff import is_pipeline_wrapped
from dr_platform.staging.sweep import (
    DEFAULT_SWEEP_BATCH_SIZE,
    sweep_abandoned_stages,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.staging.registry import PipelineRegistry

logger = logging.getLogger(__name__)

DEFAULT_DISPATCHER_CRON = "*/5 * * * * *"
DISPATCHER_WORKFLOW_NAME = "dr_platform_staging_dispatcher"
SWEEP_WORKFLOW_NAME = "dr_platform_staging_sweep"

ScheduledWorkflow = Callable[[datetime, datetime], None]


class UnwrappedPipelineError(RuntimeError):
    """A registry admitted from contains a pipeline that was not wrapped.

    Admission enqueues the registered stage callables directly, so a declared
    (unwrapped) definition never runs the completion transaction and its stage
    sits ADMITTED forever.  Register ``wrap_pipeline_workflows(...)``'s return
    value instead of the raw declaration.
    """

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


@dataclass(frozen=True, slots=True)
class DispatcherRegistration:
    """Owned DBOS client and the registered scheduled workflows.

    ``sweep_workflow`` is set only when a ``sweep_cron`` was supplied; both
    workflows share the one owned client, so ``close`` tears down both.
    """

    client: DBOSClient
    workflow: ScheduledWorkflow
    sweep_workflow: ScheduledWorkflow | None = None

    def close(self) -> None:
        self.client.destroy()


def register_scheduled_dispatcher(  # noqa: PLR0913 -- explicit wiring facts
    *,
    config: PlatformDbosConfig,
    engine: Engine,
    registry: PipelineRegistry,
    cron: str = DEFAULT_DISPATCHER_CRON,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
    sweep_cron: str | None = None,
    sweep_batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
) -> DispatcherRegistration:
    """Register the process's single scheduled admission workflow.

    When ``sweep_cron`` is set, a second scheduled workflow projects DBOS
    abandonment onto ADMITTED stages with the registration's own client;
    scheduled workflow-ID dedup makes it a single sweeper by construction.
    """
    validate_positive_integer(batch_size, label="admission batch size")
    if sweep_cron is not None:
        validate_positive_integer(sweep_batch_size, label="sweep batch size")
    validate_database_colocation(
        database_url=engine.url.render_as_string(hide_password=False),
        system_database_url=config.system_database_url,
    )
    # Reject unwrapped pipelines here, at the execution wiring boundary: a raw
    # declaration would be admitted without ever running the completion
    # transaction.  Submission-only registries never reach this check.
    _require_wrapped_registry(registry)
    client = DBOSClient(system_database_url=config.system_database_url)
    sweep_workflow: ScheduledWorkflow | None = None
    try:

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
                # A persisted position disagreeing with the registry is
                # registry/data drift -- a deployment bug, not an application
                # boundary failure -- so it is surfaced at ERROR.
                logger.error(
                    "admission found registry/data drift for stages: %s",
                    ", ".join(
                        f"{stage.pipeline_key!r} version "
                        f"{stage.pipeline_version} stage "
                        f"{stage.stage_key.value!r}: {stage.message}"
                        for stage in summary.mismatched_stages
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

    except Exception:
        client.destroy()
        raise
    return DispatcherRegistration(
        client=client,
        workflow=cast("ScheduledWorkflow", dispatch),
        sweep_workflow=sweep_workflow,
    )
