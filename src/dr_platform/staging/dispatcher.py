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

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.staging.registry import PipelineRegistry

logger = logging.getLogger(__name__)

DEFAULT_DISPATCHER_CRON = "*/5 * * * * *"
DISPATCHER_WORKFLOW_NAME = "dr_platform_staging_dispatcher"

ScheduledWorkflow = Callable[[datetime, datetime], None]


@dataclass(frozen=True, slots=True)
class DispatcherRegistration:
    """Owned DBOS client and the one registered scheduled workflow."""

    client: DBOSClient
    workflow: ScheduledWorkflow

    def close(self) -> None:
        self.client.destroy()


def register_scheduled_dispatcher(
    *,
    config: PlatformDbosConfig,
    engine: Engine,
    registry: PipelineRegistry,
    cron: str = DEFAULT_DISPATCHER_CRON,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
) -> DispatcherRegistration:
    """Register the process's single scheduled admission workflow."""
    validate_positive_integer(batch_size, label="admission batch size")
    validate_database_colocation(
        database_url=engine.url.render_as_string(hide_password=False),
        system_database_url=config.system_database_url,
    )
    client = DBOSClient(system_database_url=config.system_database_url)
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

    except Exception:
        client.destroy()
        raise
    return DispatcherRegistration(
        client=client,
        workflow=cast("ScheduledWorkflow", dispatch),
    )
