from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision = "0001_staging_baseline"
down_revision = None
branch_labels = None
depends_on = None

DEFAULT_PREFIX = "platform"
MAX_PREFIX_BYTES = 21
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
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
    return prefix


def _name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def upgrade() -> None:
    prefix = _prefix()
    pipeline_runs = _name(prefix, "pipeline_runs")
    work_items = _name(prefix, "work_items")
    stage_executions = _name(prefix, "stage_executions")
    stage_attempts = _name(prefix, "stage_attempts")
    stage_controls = _name(prefix, "stage_controls")

    op.create_table(
        pipeline_runs,
        sa.Column("run_key", sa.Text(), nullable=False),
        sa.Column("campaign_key", sa.Text(), nullable=False),
        sa.Column("pipeline_key", sa.Text(), nullable=False),
        sa.Column("pipeline_version", sa.Integer(), nullable=False),
        sa.Column("execution_config_reference", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submission_completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "pipeline_version > 0",
            name=_name(prefix, "ck_pipeline_runs_version"),
        ),
        sa.CheckConstraint(
            "submission_completed_at IS NULL "
            "OR submission_completed_at >= created_at",
            name=_name(prefix, "ck_pipeline_runs_submission_time"),
        ),
        sa.PrimaryKeyConstraint("run_key"),
    )
    provenance_guard = _name(prefix, "guard_pipeline_run_provenance")
    _execute(
        f"""
        CREATE FUNCTION {provenance_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(
            NEW.run_key,
            NEW.campaign_key,
            NEW.pipeline_key,
            NEW.pipeline_version,
            NEW.execution_config_reference,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.run_key,
            OLD.campaign_key,
            OLD.pipeline_key,
            OLD.pipeline_version,
            OLD.execution_config_reference,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'pipeline run provenance is immutable'
              USING ERRCODE = 'check_violation';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {provenance_guard}
        BEFORE UPDATE ON {pipeline_runs}
        FOR EACH ROW EXECUTE FUNCTION {provenance_guard}()
        """
    )

    op.create_table(
        work_items,
        sa.Column(
            "work_item_id",
            sa.BigInteger(),
            sa.Identity(),
            nullable=False,
        ),
        sa.Column("campaign_key", sa.Text(), nullable=False),
        sa.Column("work_key", sa.Text(), nullable=False),
        sa.Column("origin_run_key", sa.Text(), nullable=False),
        sa.Column("input_reference", sa.Text(), nullable=False),
        sa.Column("labels", postgresql.JSONB(), nullable=False),
        sa.Column("rank", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "jsonb_typeof(labels) = 'object'",
            name=_name(prefix, "ck_work_items_labels_object"),
        ),
        sa.CheckConstraint(
            "rank > 0",
            name=_name(prefix, "ck_work_items_rank"),
        ),
        sa.ForeignKeyConstraint(
            ["origin_run_key"],
            [f"{pipeline_runs}.run_key"],
            name=_name(prefix, "fk_work_items_origin_run"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("work_item_id"),
        sa.UniqueConstraint(
            "campaign_key",
            "work_key",
            name=_name(prefix, "uq_work_items_campaign_work"),
        ),
        sa.UniqueConstraint(
            "work_item_id",
            "rank",
            name=_name(prefix, "uq_work_items_id_rank"),
        ),
    )
    op.create_index(
        _name(prefix, "ix_work_items_labels"),
        work_items,
        ["labels"],
        unique=False,
        postgresql_using="gin",
    )

    op.create_table(
        stage_executions,
        sa.Column(
            "stage_execution_id",
            sa.BigInteger(),
            sa.Identity(),
            nullable=False,
        ),
        sa.Column("work_item_id", sa.BigInteger(), nullable=False),
        sa.Column("stage_key", sa.Text(), nullable=False),
        sa.Column("stage_index", sa.Integer(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("current_attempt", sa.Integer(), nullable=False),
        sa.Column("rank", sa.BigInteger(), nullable=False),
        sa.Column("output_reference", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "stage_index >= 0",
            name=_name(prefix, "ck_stage_executions_index"),
        ),
        sa.CheckConstraint(
            "state IN "
            "('ready', 'admitted', 'succeeded', 'failed', 'cancelled')",
            name=_name(prefix, "ck_stage_executions_state"),
        ),
        sa.CheckConstraint(
            "current_attempt >= 0",
            name=_name(prefix, "ck_stage_executions_current_attempt"),
        ),
        sa.CheckConstraint(
            "rank > 0",
            name=_name(prefix, "ck_stage_executions_rank"),
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name=_name(prefix, "ck_stage_executions_updated_time"),
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id", "rank"],
            [f"{work_items}.work_item_id", f"{work_items}.rank"],
            name=_name(prefix, "fk_stage_executions_work_item"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("stage_execution_id"),
        sa.UniqueConstraint(
            "work_item_id",
            "stage_key",
            name=_name(prefix, "uq_stage_executions_work_stage"),
        ),
    )
    op.create_index(
        _name(prefix, "ix_stage_executions_ready_admission"),
        stage_executions,
        ["stage_key", "rank"],
        unique=False,
        postgresql_where=sa.text("state = 'ready'"),
    )

    op.create_table(
        stage_attempts,
        sa.Column(
            "stage_attempt_id",
            sa.BigInteger(),
            sa.Identity(),
            nullable=False,
        ),
        sa.Column("stage_execution_id", sa.BigInteger(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("workflow_id", sa.Text(), nullable=False),
        sa.Column("terminal_summary", postgresql.JSONB()),
        sa.Column("terminal_reference", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("admitted_at", sa.DateTime(timezone=True)),
        sa.Column("terminal_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=_name(prefix, "ck_stage_attempts_number"),
        ),
        sa.CheckConstraint(
            "terminal_summary IS NULL "
            "OR jsonb_typeof(terminal_summary) = 'object'",
            name=_name(prefix, "ck_stage_attempts_summary_object"),
        ),
        sa.CheckConstraint(
            "admitted_at IS NULL OR admitted_at >= created_at",
            name=_name(prefix, "ck_stage_attempts_admitted_time"),
        ),
        sa.CheckConstraint(
            "terminal_at IS NULL OR terminal_at >= created_at",
            name=_name(prefix, "ck_stage_attempts_terminal_time"),
        ),
        sa.CheckConstraint(
            "admitted_at IS NULL OR terminal_at IS NULL "
            "OR terminal_at >= admitted_at",
            name=_name(prefix, "ck_stage_attempts_time_order"),
        ),
        sa.ForeignKeyConstraint(
            ["stage_execution_id"],
            [f"{stage_executions}.stage_execution_id"],
            name=_name(prefix, "fk_stage_attempts_execution"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("stage_attempt_id"),
        sa.UniqueConstraint(
            "stage_execution_id",
            "attempt_number",
            name=_name(prefix, "uq_stage_attempts_execution_number"),
        ),
        sa.UniqueConstraint(
            "workflow_id",
            name=_name(prefix, "uq_stage_attempts_workflow"),
        ),
    )

    op.create_table(
        stage_controls,
        sa.Column(
            "stage_control_id",
            sa.BigInteger(),
            sa.Identity(),
            nullable=False,
        ),
        sa.Column("pipeline_key", sa.Text(), nullable=False),
        sa.Column("pipeline_version", sa.Integer(), nullable=False),
        sa.Column("stage_key", sa.Text(), nullable=False),
        sa.Column("selector", postgresql.JSONB(), nullable=False),
        sa.Column("capacity", sa.Integer(), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "pipeline_version > 0",
            name=_name(prefix, "ck_stage_controls_version"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(selector) = 'object'",
            name=_name(prefix, "ck_stage_controls_selector_object"),
        ),
        sa.CheckConstraint(
            "capacity >= 0",
            name=_name(prefix, "ck_stage_controls_capacity"),
        ),
        sa.PrimaryKeyConstraint("stage_control_id"),
        sa.UniqueConstraint(
            "pipeline_key",
            "pipeline_version",
            "stage_key",
            "selector",
            name=_name(prefix, "uq_stage_controls_stage_selector"),
        ),
    )


def downgrade() -> None:
    raise NotImplementedError("platform baseline migration is irreversible")
