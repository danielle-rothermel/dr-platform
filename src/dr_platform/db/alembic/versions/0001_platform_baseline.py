"""Fresh final platform kernel baseline.

Revision ID: 0001_platform_baseline
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform.db.schema import DEFAULT_PREFIX, PlatformSchema
from dr_platform.status import TERMINAL_EXECUTION_STATES

revision = "0001_platform_baseline"
down_revision = None
branch_labels = None
depends_on = None


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
    return prefix


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def _install_change_tracking(schema: PlatformSchema) -> None:
    prefix = schema.prefix
    sequence = f"{prefix}_change_seq"
    function = f"{prefix}_assign_change_seq"
    trigger = f"{prefix}_assign_change_seq"
    _execute(f"CREATE SEQUENCE {sequence}")
    _execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          NEW.change_seq := nextval(
            format('%I.%I', TG_TABLE_SCHEMA, '{sequence}')::regclass
          );
          RETURN NEW;
        END;
        $$
        """
    )
    for table in schema.metadata.sorted_tables:
        _execute(
            f"""
            CREATE TRIGGER {trigger}
            BEFORE INSERT OR UPDATE ON {table.name}
            FOR EACH ROW EXECUTE FUNCTION {function}()
            """
        )


def _install_lifecycle_guards(schema: PlatformSchema) -> None:
    prefix = schema.prefix
    delete_function = f"{prefix}_reject_kernel_delete"
    delete_trigger = f"{prefix}_reject_kernel_delete"
    _execute(
        f"""
        CREATE FUNCTION {delete_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'kernel lifecycle rows cannot be deleted';
        END;
        $$
        """
    )
    for table in schema.metadata.sorted_tables:
        _execute(
            f"""
            CREATE TRIGGER {delete_trigger}
            BEFORE DELETE ON {table.name}
            FOR EACH ROW EXECUTE FUNCTION {delete_function}()
            """
        )

    terminal_function = f"{prefix}_reject_terminal_attempt_mutation"
    terminal_trigger = f"{prefix}_00_reject_terminal_attempt_mutation"
    terminal_states = ", ".join(
        f"'{state.value}'" for state in sorted(TERMINAL_EXECUTION_STATES)
    )
    _execute(
        f"""
        CREATE FUNCTION {terminal_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.execution_state IN ({terminal_states}) THEN
            IF NEW IS DISTINCT FROM OLD THEN
              RAISE EXCEPTION 'terminal item attempts are immutable';
            END IF;
            RETURN NULL;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {terminal_trigger}
        BEFORE UPDATE ON {schema.item_attempts.name}
        FOR EACH ROW EXECUTE FUNCTION {terminal_function}()
        """
    )

    operation_function = f"{prefix}_guard_operation_update"
    operation_trigger = f"{prefix}_00_guard_operation_update"
    _execute(
        f"""
        CREATE FUNCTION {operation_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF ROW(
            NEW.operation_key,
            NEW.group_key,
            NEW.workflow_role,
            NEW.requested_count,
            NEW.manifest_version,
            NEW.manifest_digest,
            NEW.manifest_page_size,
            NEW.manifest_page_count,
            NEW.operation_execution_recipe_digest,
            NEW.target_key,
            NEW.target_version,
            NEW.target_contract_digest,
            NEW.retry_policy,
            NEW.spec,
            NEW.metadata,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.operation_key,
            OLD.group_key,
            OLD.workflow_role,
            OLD.requested_count,
            OLD.manifest_version,
            OLD.manifest_digest,
            OLD.manifest_page_size,
            OLD.manifest_page_count,
            OLD.operation_execution_recipe_digest,
            OLD.target_key,
            OLD.target_version,
            OLD.target_contract_digest,
            OLD.retry_policy,
            OLD.spec,
            OLD.metadata,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'Operation identity fields are immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {operation_trigger}
        BEFORE UPDATE ON {schema.operations.name}
        FOR EACH ROW EXECUTE FUNCTION {operation_function}()
        """
    )

    item_function = f"{prefix}_guard_item_update"
    item_trigger = f"{prefix}_00_guard_item_update"
    _execute(
        f"""
        CREATE FUNCTION {item_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.operation_key,
            NEW.item_index,
            NEW.item_key,
            NEW.shuffle_rank,
            NEW.service_class,
            NEW.service_priority,
            NEW.spec,
            NEW.insert_status,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.operation_key,
            OLD.item_index,
            OLD.item_key,
            OLD.shuffle_rank,
            OLD.service_class,
            OLD.service_priority,
            OLD.spec,
            OLD.insert_status,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'Item identity fields are immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {item_trigger}
        BEFORE UPDATE ON {schema.items.name}
        FOR EACH ROW EXECUTE FUNCTION {item_function}()
        """
    )

    claim_function = f"{prefix}_guard_enqueue_claim_update"
    claim_trigger = f"{prefix}_00_guard_enqueue_claim_update"
    _execute(
        f"""
        CREATE FUNCTION {claim_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF OLD.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION 'resolved enqueue Claims are immutable';
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.attempt,
            NEW.claim_id,
            NEW.workflow_id,
            NEW.enqueue_try,
            NEW.claimed_at,
            NEW.lease_expires_at,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.attempt,
            OLD.claim_id,
            OLD.workflow_id,
            OLD.enqueue_try,
            OLD.claimed_at,
            OLD.lease_expires_at,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'enqueue Claim identity is immutable';
          END IF;
          IF OLD.enqueue_call_started_at IS NOT NULL
             AND NEW.enqueue_call_started_at IS DISTINCT FROM
                 OLD.enqueue_call_started_at THEN
            RAISE EXCEPTION 'enqueue Claim call-start fact is immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {claim_trigger}
        BEFORE UPDATE ON {schema.enqueue_claims.name}
        FOR EACH ROW EXECUTE FUNCTION {claim_function}()
        """
    )

    compensation_function = f"{prefix}_guard_compensation_update"
    compensation_trigger = f"{prefix}_00_guard_compensation_update"
    _execute(
        f"""
        CREATE FUNCTION {compensation_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF OLD.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION 'resolved enqueue compensations are immutable';
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.attempt,
            NEW.claim_id,
            NEW.workflow_id,
            NEW.reason,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.attempt,
            OLD.claim_id,
            OLD.workflow_id,
            OLD.reason,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'enqueue compensation identity is immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {compensation_trigger}
        BEFORE UPDATE ON {schema.enqueue_compensations.name}
        FOR EACH ROW EXECUTE FUNCTION {compensation_function}()
        """
    )

    request_function = f"{prefix}_guard_next_attempt_request_update"
    request_trigger = f"{prefix}_00_guard_next_attempt_request_update"
    _execute(
        f"""
        CREATE FUNCTION {request_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          RAISE EXCEPTION 'next-Attempt request ledger is immutable';
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {request_trigger}
        BEFORE UPDATE ON {schema.next_attempt_requests.name}
        FOR EACH ROW EXECUTE FUNCTION {request_function}()
        """
    )


def upgrade() -> None:
    schema = PlatformSchema(prefix=_prefix())
    schema.metadata.create_all(bind=op.get_bind(), checkfirst=False)
    _install_change_tracking(schema)
    _install_lifecycle_guards(schema)


def downgrade() -> None:
    schema = PlatformSchema(prefix=_prefix())
    prefix = schema.prefix
    schema.metadata.drop_all(bind=op.get_bind(), checkfirst=False)
    _execute(f"DROP FUNCTION {prefix}_guard_next_attempt_request_update()")
    _execute(f"DROP FUNCTION {prefix}_guard_compensation_update()")
    _execute(f"DROP FUNCTION {prefix}_guard_enqueue_claim_update()")
    _execute(f"DROP FUNCTION {prefix}_guard_item_update()")
    _execute(f"DROP FUNCTION {prefix}_guard_operation_update()")
    _execute(f"DROP FUNCTION {prefix}_reject_terminal_attempt_mutation()")
    _execute(f"DROP FUNCTION {prefix}_reject_kernel_delete()")
    _execute(f"DROP FUNCTION {prefix}_assign_change_seq()")
    _execute(f"DROP SEQUENCE {prefix}_change_seq")
