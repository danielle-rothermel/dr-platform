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

from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)

if TYPE_CHECKING:
    from enum import StrEnum

PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
MAX_PREFIX_BYTES = 21
DEFAULT_PREFIX = "platform"


def _enum_check(column_name: str, enum_type: type[StrEnum]) -> str:
    values = ", ".join(f"'{value.value}'" for value in enum_type)
    return f"{column_name} IN ({values})"


class StagingSchema:
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
            Column("expected_member_count", Integer, nullable=False),
            Column("manifest_reference", Text),
            Column("membership_digest", Text),
            Column("run_completion_key", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("registration_closed_at", DateTime(timezone=True)),
            Column("registered_member_count", Integer),
            Column("created_work_count", Integer),
            Column("reused_work_count", Integer),
            Column("released_at", DateTime(timezone=True)),
            Column("release_terminal_state_counts", JSONB),
            CheckConstraint(
                "pipeline_version > 0",
                name=name("ck_pipeline_runs_version"),
            ),
            CheckConstraint(
                "expected_member_count >= 0",
                name=name("ck_pipeline_runs_member_count"),
            ),
            CheckConstraint(
                "(manifest_reference IS NULL) = (membership_digest IS NULL)",
                name=name("ck_pipeline_runs_manifest_binding"),
            ),
            CheckConstraint(
                "run_completion_key IS NULL OR manifest_reference IS NOT NULL",
                name=name("ck_pipeline_runs_completion_manifest"),
            ),
            CheckConstraint(
                "registration_closed_at IS NULL OR "
                "registration_closed_at >= created_at",
                name=name("ck_pipeline_runs_registration_time"),
            ),
            CheckConstraint(
                "(registration_closed_at IS NULL) = "
                "(registered_member_count IS NULL)",
                name=name("ck_pipeline_runs_registration_count"),
            ),
            CheckConstraint(
                "(registration_closed_at IS NULL) = "
                "(created_work_count IS NULL) AND "
                "(registration_closed_at IS NULL) = "
                "(reused_work_count IS NULL)",
                name=name("ck_pipeline_runs_receipt_presence"),
            ),
            CheckConstraint(
                "registration_closed_at IS NULL OR "
                "(registered_member_count = expected_member_count AND "
                "created_work_count >= 0 AND reused_work_count >= 0 AND "
                "created_work_count + reused_work_count = "
                "registered_member_count)",
                name=name("ck_pipeline_runs_receipt_counts"),
            ),
            CheckConstraint(
                "released_at IS NULL OR "
                "(registration_closed_at IS NOT NULL AND "
                "released_at >= registration_closed_at)",
                name=name("ck_pipeline_runs_release_time"),
            ),
            CheckConstraint(
                "(released_at IS NULL) = "
                "(release_terminal_state_counts IS NULL)",
                name=name("ck_pipeline_runs_release_counts"),
            ),
        )
        Index(
            name("ix_pipeline_runs_completion_candidates"),
            self.pipeline_runs.c.run_key,
            postgresql_where=text(
                "registration_closed_at IS NOT NULL AND "
                "run_completion_key IS NOT NULL AND released_at IS NULL"
            ),
        )
        Index(
            name("ix_pipeline_runs_campaign_cursor"),
            self.pipeline_runs.c.campaign_key,
            self.pipeline_runs.c.created_at,
            self.pipeline_runs.c.run_key,
        )

        self.run_barrier_cursor = Table(
            name("run_barrier_cursor"),
            self.metadata,
            Column("singleton", Boolean, primary_key=True),
            Column("last_run_key", Text),
            CheckConstraint(
                "singleton",
                name=name("ck_run_barrier_cursor_singleton"),
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

        self.run_memberships = Table(
            name("run_memberships"),
            self.metadata,
            Column(
                "run_key",
                Text,
                ForeignKey(
                    f"{name('pipeline_runs')}.run_key",
                    name=name("fk_run_memberships_run"),
                    ondelete="RESTRICT",
                ),
                primary_key=True,
            ),
            Column("member_ordinal", Integer, primary_key=True),
            Column(
                "work_item_id",
                BigInteger,
                ForeignKey(
                    f"{name('work_items')}.work_item_id",
                    name=name("fk_run_memberships_work_item"),
                    ondelete="RESTRICT",
                ),
                nullable=False,
            ),
            UniqueConstraint(
                "run_key",
                "work_item_id",
                name=name("uq_run_memberships_run_work"),
            ),
            CheckConstraint(
                "member_ordinal >= 0",
                name=name("ck_run_memberships_ordinal"),
            ),
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
        Index(
            name("ix_stage_executions_nonterminal_work"),
            self.stage_executions.c.work_item_id,
            postgresql_where=text("state IN ('ready', 'admitted')"),
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

        self.run_completion_executions = Table(
            name("run_completion_executions"),
            self.metadata,
            Column(
                "run_completion_execution_id",
                BigInteger,
                Identity(),
                primary_key=True,
            ),
            Column(
                "run_key",
                Text,
                ForeignKey(
                    f"{name('pipeline_runs')}.run_key",
                    name=name("fk_run_completion_executions_run"),
                    ondelete="RESTRICT",
                ),
                nullable=False,
                unique=True,
            ),
            Column("workflow_id", Text, nullable=False, unique=True),
            Column("state", Text, nullable=False),
            Column("enqueued_at", DateTime(timezone=True), nullable=False),
            Column("output_reference", Text),
            Column("error_summary", JSONB),
            Column("terminal_at", DateTime(timezone=True)),
            CheckConstraint(
                _enum_check("state", RunCompletionExecutionState),
                name=name("ck_run_completion_executions_state"),
            ),
            CheckConstraint(
                "terminal_at IS NULL OR terminal_at >= enqueued_at",
                name=name("ck_run_completion_executions_terminal_time"),
            ),
            CheckConstraint(
                "(state = 'enqueued' AND terminal_at IS NULL AND "
                "output_reference IS NULL AND error_summary IS NULL) OR "
                "(state = 'succeeded' AND terminal_at IS NOT NULL AND "
                "output_reference IS NOT NULL AND error_summary IS NULL) OR "
                "(state = 'failed' AND terminal_at IS NOT NULL AND "
                "output_reference IS NULL AND error_summary IS NOT NULL)",
                name=name("ck_run_completion_executions_outcome"),
            ),
        )
