from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, cast

from dbos import DBOS, DBOSClient

from dr_platform._core.validation import validate_positive_integer
from dr_platform.admission.runner import (
    DEFAULT_ADMISSION_BATCH_SIZE,
    run_admission_pass,
)
from dr_platform.completion.barrier import (
    DEFAULT_RUN_BARRIER_BATCH_SIZE,
    run_barrier_pass,
)
from dr_platform.execution.handoff import is_pipeline_wrapped
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

DEFAULT_DISPATCHER_CRON = "*/5 * * * * *"
DEFAULT_RUN_BARRIER_CRON = "*/5 * * * * *"
DISPATCHER_WORKFLOW_NAME = "dr_platform_staging_dispatcher"
RUN_BARRIER_WORKFLOW_NAME = "dr_platform_run_barrier"
SWEEP_WORKFLOW_NAME = "dr_platform_staging_sweep"

ScheduledWorkflow = Callable[[datetime, datetime], None]


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


@dataclass(frozen=True, slots=True)
class DispatcherRegistration:
    """Owns the client shared by admission, barrier, and optional sweep."""

    client: DBOSClient
    workflow: ScheduledWorkflow
    barrier_workflow: ScheduledWorkflow
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
    barrier_cron: str = DEFAULT_RUN_BARRIER_CRON,
    barrier_batch_size: int = DEFAULT_RUN_BARRIER_BATCH_SIZE,
    sweep_cron: str | None = None,
    sweep_batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
) -> DispatcherRegistration:
    """Register admission and optional single-sweeper workflows."""
    validate_positive_integer(batch_size, label="admission batch size")
    validate_positive_integer(
        barrier_batch_size, label="run barrier batch size"
    )
    if sweep_cron is not None:
        validate_positive_integer(sweep_batch_size, label="sweep batch size")
    validate_database_colocation(
        database_url=engine.url.render_as_string(hide_password=False),
        system_database_url=config.system_database_url,
    )
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

    except Exception:
        client.destroy()
        raise
    return DispatcherRegistration(
        client=client,
        workflow=cast("ScheduledWorkflow", dispatch),
        barrier_workflow=cast("ScheduledWorkflow", reconcile_run_barriers),
        sweep_workflow=sweep_workflow,
    )
