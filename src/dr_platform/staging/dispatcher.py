"""DBOS scheduled wiring for the single staging admission dispatcher.

The registration hook constructs and owns one ``DBOSClient`` from the
validated colocated system database URL.  Its returned registration handle
keeps that client alive and provides explicit cleanup.  The scheduled
workflow is deliberately only an adapter around ``run_admission_pass``.
Every declared stage must have an empty-selector capacity control before its
READY work is encountered; otherwise admission raises
``MissingStageControlError`` and rolls back the pass.
"""

from __future__ import annotations

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
            run_admission_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=batch_size,
            )

    except Exception:
        client.destroy()
        raise
    return DispatcherRegistration(
        client=client,
        workflow=cast("ScheduledWorkflow", dispatch),
    )
