# ruff: noqa: S608 -- every interpolated identifier uses the strict prefix
# validator above; SQL parameters cannot represent DDL identifiers.
from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op

from dr_platform._core.ledger.schema import StagingSchema

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
    schema = StagingSchema(prefix)
    schema.metadata.create_all(op.get_bind(), checkfirst=False)

    pipeline_runs = _name(prefix, "pipeline_runs")
    run_memberships = _name(prefix, "run_memberships")
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
            NEW.expected_member_count,
            NEW.manifest_reference,
            NEW.membership_digest,
            NEW.run_completion_key,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.run_key,
            OLD.campaign_key,
            OLD.pipeline_key,
            OLD.pipeline_version,
            OLD.execution_config_reference,
            OLD.expected_member_count,
            OLD.manifest_reference,
            OLD.membership_digest,
            OLD.run_completion_key,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'pipeline run provenance is immutable'
              USING ERRCODE = 'check_violation';
          END IF;
          IF OLD.registration_closed_at IS NOT NULL AND ROW(
            NEW.registration_closed_at,
            NEW.registered_member_count,
            NEW.created_work_count,
            NEW.reused_work_count
          ) IS DISTINCT FROM ROW(
            OLD.registration_closed_at,
            OLD.registered_member_count,
            OLD.created_work_count,
            OLD.reused_work_count
          ) THEN
            RAISE EXCEPTION 'pipeline run registration closure is immutable'
              USING ERRCODE = 'check_violation';
          END IF;
          IF OLD.released_at IS NOT NULL AND ROW(
            NEW.released_at,
            NEW.release_terminal_state_counts
          ) IS DISTINCT FROM ROW(
            OLD.released_at,
            OLD.release_terminal_state_counts
          ) THEN
            RAISE EXCEPTION 'pipeline run release is immutable'
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

    membership_guard = _name(prefix, "guard_closed_run_membership")
    _execute(
        f"""
        CREATE FUNCTION {membership_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
          checked_run_key text;
        BEGIN
          IF TG_OP = 'DELETE' THEN
            checked_run_key := OLD.run_key;
          ELSE
            checked_run_key := NEW.run_key;
          END IF;
          IF EXISTS (
            SELECT 1 FROM {pipeline_runs}
            WHERE run_key = checked_run_key
              AND registration_closed_at IS NOT NULL
          ) THEN
            RAISE EXCEPTION 'closed run membership is immutable'
              USING ERRCODE = 'check_violation';
          END IF;
          IF TG_OP = 'UPDATE' AND OLD.run_key IS DISTINCT FROM NEW.run_key
             AND EXISTS (
               SELECT 1 FROM {pipeline_runs}
               WHERE run_key = OLD.run_key
                 AND registration_closed_at IS NOT NULL
             ) THEN
            RAISE EXCEPTION 'closed run membership is immutable'
              USING ERRCODE = 'check_violation';
          END IF;
          RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {membership_guard}
        BEFORE INSERT OR UPDATE OR DELETE ON {run_memberships}
        FOR EACH ROW EXECUTE FUNCTION {membership_guard}()
        """
    )


def downgrade() -> None:
    raise NotImplementedError("platform baseline migration is irreversible")
