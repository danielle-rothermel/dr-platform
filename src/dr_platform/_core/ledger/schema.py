"""SQLAlchemy schema for the staged-work ledger."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from dr_platform._core.ledger.states import StageExecutionState

if TYPE_CHECKING:
    from enum import StrEnum

PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
MAX_PREFIX_BYTES = 21
DEFAULT_PREFIX = "platform"


def _enum_check(column_name: str, enum_type: type[StrEnum]) -> str:
    values = ", ".join(f"'{value.value}'" for value in enum_type)
    return f"{column_name} IN ({values})"


class StagingSchema:
    """The five staged-work tables."""

    def __init__(self, prefix: str = DEFAULT_PREFIX) -> None:
        if PREFIX_PATTERN.fullmatch(prefix) is None:
            raise ValueError(
                "prefix must be a lowercase SQL identifier using letters, "
                "numbers, or _"
            )
        if len(prefix.encode()) > MAX_PREFIX_BYTES:
            raise ValueError(
                "prefix is too long for generated PostgreSQL identifiers: "
                f"maximum is {MAX_PREFIX_BYTES} ASCII bytes"
            )

        self.prefix = prefix
        self.metadata = MetaData()

        def name(suffix: str) -> str:
            return f"{prefix}_{suffix}"

        self.pipeline_runs = Table(
            name("pipeline_runs"),
            self.metadata,
            Column("run_key", Text, primary_key=True),
            Column("campaign_key", Text, nullable=False),
            Column("pipeline_key", Text, nullable=False),
            Column("pipeline_version", Integer, nullable=False),
            Column("execution_config_reference", Text, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("submission_completed_at", DateTime(timezone=True)),
            CheckConstraint(
                "pipeline_version > 0",
                name=name("ck_pipeline_runs_version"),
            ),
            CheckConstraint(
                "submission_completed_at IS NULL "
                "OR submission_completed_at >= created_at",
                name=name("ck_pipeline_runs_submission_time"),
            ),
        )

        self.work_items = Table(
            name("work_items"),
            self.metadata,
            Column(
                "work_item_id",
                BigInteger,
                Identity(),
                primary_key=True,
            ),
            Column("campaign_key", Text, nullable=False),
            Column("work_key", Text, nullable=False),
            Column(
                "origin_run_key",
                Text,
                ForeignKey(
                    f"{name('pipeline_runs')}.run_key",
                    name=name("fk_work_items_origin_run"),
                    ondelete="RESTRICT",
                ),
                nullable=False,
            ),
            Column("input_reference", Text, nullable=False),
            Column("labels", JSONB, nullable=False),
            Column("rank", BigInteger, nullable=False),
            UniqueConstraint(
                "campaign_key",
                "work_key",
                name=name("uq_work_items_campaign_work"),
            ),
            UniqueConstraint(
                "work_item_id",
                "rank",
                name=name("uq_work_items_id_rank"),
            ),
            CheckConstraint(
                "jsonb_typeof(labels) = 'object'",
                name=name("ck_work_items_labels_object"),
            ),
            CheckConstraint(
                "rank > 0",
                name=name("ck_work_items_rank"),
            ),
        )
        Index(
            name("ix_work_items_labels"),
            self.work_items.c.labels,
            postgresql_using="gin",
        )

        self.stage_executions = Table(
            name("stage_executions"),
            self.metadata,
            Column(
                "stage_execution_id",
                BigInteger,
                Identity(),
                primary_key=True,
            ),
            Column("work_item_id", BigInteger, nullable=False),
            Column("stage_key", Text, nullable=False),
            Column("stage_index", Integer, nullable=False),
            Column("state", Text, nullable=False),
            Column("current_attempt", Integer, nullable=False),
            Column("rank", BigInteger, nullable=False),
            Column("output_reference", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            ForeignKeyConstraint(
                ["work_item_id", "rank"],
                [
                    f"{name('work_items')}.work_item_id",
                    f"{name('work_items')}.rank",
                ],
                name=name("fk_stage_executions_work_item"),
                ondelete="RESTRICT",
            ),
            UniqueConstraint(
                "work_item_id",
                "stage_key",
                name=name("uq_stage_executions_work_stage"),
            ),
            CheckConstraint(
                "stage_index >= 0",
                name=name("ck_stage_executions_index"),
            ),
            CheckConstraint(
                _enum_check("state", StageExecutionState),
                name=name("ck_stage_executions_state"),
            ),
            CheckConstraint(
                "current_attempt >= 0",
                name=name("ck_stage_executions_current_attempt"),
            ),
            CheckConstraint(
                "rank > 0",
                name=name("ck_stage_executions_rank"),
            ),
            CheckConstraint(
                "updated_at >= created_at",
                name=name("ck_stage_executions_updated_time"),
            ),
        )
        Index(
            name("ix_stage_executions_ready_admission"),
            self.stage_executions.c.stage_key,
            self.stage_executions.c.rank,
            postgresql_where=text("state = 'ready'"),
        )

        self.stage_attempts = Table(
            name("stage_attempts"),
            self.metadata,
            Column(
                "stage_attempt_id",
                BigInteger,
                Identity(),
                primary_key=True,
            ),
            Column(
                "stage_execution_id",
                BigInteger,
                ForeignKey(
                    f"{name('stage_executions')}.stage_execution_id",
                    name=name("fk_stage_attempts_execution"),
                    ondelete="RESTRICT",
                ),
                nullable=False,
            ),
            Column("attempt_number", Integer, nullable=False),
            Column("workflow_id", Text, nullable=False),
            Column("terminal_summary", JSONB),
            Column("terminal_reference", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("admitted_at", DateTime(timezone=True)),
            Column("terminal_at", DateTime(timezone=True)),
            UniqueConstraint(
                "stage_execution_id",
                "attempt_number",
                name=name("uq_stage_attempts_execution_number"),
            ),
            UniqueConstraint(
                "workflow_id",
                name=name("uq_stage_attempts_workflow"),
            ),
            CheckConstraint(
                "attempt_number > 0",
                name=name("ck_stage_attempts_number"),
            ),
            CheckConstraint(
                "terminal_summary IS NULL "
                "OR jsonb_typeof(terminal_summary) = 'object'",
                name=name("ck_stage_attempts_summary_object"),
            ),
            CheckConstraint(
                "admitted_at IS NULL OR admitted_at >= created_at",
                name=name("ck_stage_attempts_admitted_time"),
            ),
            CheckConstraint(
                "terminal_at IS NULL OR terminal_at >= created_at",
                name=name("ck_stage_attempts_terminal_time"),
            ),
            CheckConstraint(
                "admitted_at IS NULL OR terminal_at IS NULL "
                "OR terminal_at >= admitted_at",
                name=name("ck_stage_attempts_time_order"),
            ),
        )

        self.stage_controls = Table(
            name("stage_controls"),
            self.metadata,
            Column(
                "stage_control_id",
                BigInteger,
                Identity(),
                primary_key=True,
            ),
            Column("pipeline_key", Text, nullable=False),
            Column("pipeline_version", Integer, nullable=False),
            Column("stage_key", Text, nullable=False),
            Column("selector", JSONB, nullable=False),
            Column("capacity", Integer, nullable=False),
            Column("paused", Boolean, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            UniqueConstraint(
                "pipeline_key",
                "pipeline_version",
                "stage_key",
                "selector",
                name=name("uq_stage_controls_stage_selector"),
            ),
            CheckConstraint(
                "pipeline_version > 0",
                name=name("ck_stage_controls_version"),
            ),
            CheckConstraint(
                "jsonb_typeof(selector) = 'object'",
                name=name("ck_stage_controls_selector_object"),
            ),
            CheckConstraint(
                "capacity >= 0",
                name=name("ck_stage_controls_capacity"),
            ),
        )
