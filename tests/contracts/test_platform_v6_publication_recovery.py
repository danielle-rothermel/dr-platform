"""Public v6 publication-operation recovery contract."""

from __future__ import annotations

import inspect

from sqlalchemy import create_engine

import dr_platform
from dr_platform import (
    CleanupEligibility,
    CleanupResult,
    DestinationResult,
    ExportOptions,
    PostgresPublicationFence,
    PreparedStage,
    PublicationCapabilities,
    PublicationObservation,
    PublicationOperationIdentity,
    PublicationReceipt,
)


def test_operation_recovery_models_are_public_and_typed() -> None:
    expected = {
        "CleanupEligibility",
        "CleanupResult",
        "PreparedStage",
        "PublicationCapabilities",
        "PublicationObservation",
        "PublicationOperationIdentity",
        "PublicationReceipt",
    }
    assert expected <= set(dr_platform.__all__)
    assert PublicationOperationIdentity.model_fields.keys() == {
        "operation_id",
        "attempt_id",
    }
    assert PreparedStage.model_fields.keys() >= {
        "identity",
        "members",
        "plan_digest",
        "plan_signature",
    }
    assert PublicationReceipt.model_fields.keys() >= {
        "operation_id",
        "destination_id",
        "bundle_key",
        "bundle_id",
        "stage_plan_digest",
    }
    assert CleanupEligibility.model_fields.keys() == {
        "disposition",
        "observation",
    }
    assert CleanupResult.model_fields.keys() == {
        "disposition",
        "operation_id",
        "cleanup_request_id",
        "observation",
    }
    assert PublicationObservation.model_fields["state"].is_required()
    assert PublicationCapabilities.model_fields.keys() == {
        "operation_cleanup",
        "reason",
    }


def test_export_contract_separates_operation_from_attempt_and_receipts() -> (
    None
):
    options = ExportOptions(
        destination_path="publication.duckdb", run_id="attempt"
    )
    assert options.operation_id == "attempt"
    explicit = ExportOptions(
        destination_path="publication.duckdb",
        run_id="attempt",
        operation_id="operation",
    )
    assert explicit.operation_id == "operation"
    assert "receipt" in DestinationResult.model_fields


def test_motherduck_operation_path_avoids_unproven_postgres_constructs() -> (
    None
):
    publication_source = inspect.getsource(PostgresPublicationFence)
    promote_source = inspect.getsource(PostgresPublicationFence.promote)
    ensure_schema_source = inspect.getsource(
        PostgresPublicationFence.ensure_schema
    )
    for forbidden in ("SAVEPOINT", "FOR UPDATE", "advisory", "TRIGGER"):
        assert forbidden not in promote_source
        assert forbidden not in ensure_schema_source
    assert "whetstone" not in publication_source.lower()
    assert "SERIALIZABLE" not in promote_source
    # MotherDuck's parser rejects ADD COLUMN with constraints, so additive
    # legacy migrations stay constraint-free; fresh CREATE statements carry
    # the full NOT NULL shape instead.
    assert "mutation_epoch BIGINT DEFAULT 0" in ensure_schema_source
    assert "pin_kind TEXT DEFAULT 'EXTERNAL'" in ensure_schema_source
    assert "ADD COLUMN IF NOT EXISTS mutation_epoch BIGINT NOT NULL" not in (
        ensure_schema_source
    )
    # MotherDuck rejects SERIALIZABLE; destructive cleanup relies on the
    # same-transaction gate compare-and-set instead of an isolation level.
    for cleanup in (
        PostgresPublicationFence._cleanup_operation_once,
        PostgresPublicationFence._cleanup_bundles_once,
    ):
        cleanup_source = inspect.getsource(cleanup)
        assert 'if self.kind == "neon":' in cleanup_source
        assert "SERIALIZABLE" in cleanup_source


def test_fault_boundaries_are_named_in_platform_protocol() -> None:
    source = inspect.getsource(PostgresPublicationFence)
    for boundary in (
        "after_plan_commit",
        "before_stage_gate",
        "after_each_stage_member",
        "after_stage_commit",
        "after_promotion_commit",
        "before_cleanup_gate",
        "after_cleanup_commit",
        "before_retention_gate",
    ):
        assert boundary in source


def test_operation_cleanup_capability_defaults_fail_closed() -> None:
    engine = create_engine("postgresql+psycopg:///contract_never_connected")
    default_fence = PostgresPublicationFence(
        engine, destination_id="contract"
    )
    assert default_fence.capabilities.operation_cleanup is False
    assert default_fence.capabilities.reason is not None
    enabled_neon = PostgresPublicationFence(
        engine,
        destination_id="contract",
        kind="neon",
        operation_cleanup_enabled=True,
    )
    assert enabled_neon.capabilities.operation_cleanup is True
    motherduck = PostgresPublicationFence(
        engine,
        destination_id="contract",
        kind="motherduck",
        operation_cleanup_enabled=True,
    )
    assert motherduck.capabilities.operation_cleanup is False
    assert motherduck.capabilities.reason is not None


def test_recovery_boundaries_verify_the_signed_plan_with_key_identity() -> (
    None
):
    ensure_schema_source = inspect.getsource(
        PostgresPublicationFence.ensure_schema
    )
    assert "plan_key_id" in ensure_schema_source
    for boundary in (
        PostgresPublicationFence.prepare_stage,
        PostgresPublicationFence._plan_inventory_matches,
        PostgresPublicationFence._cleanup_operation_once,
        PostgresPublicationFence._repair_cleaned_residual,
    ):
        assert "_plan_signature_valid" in inspect.getsource(boundary)


def test_effect_transactions_open_with_a_gate_compare_and_set() -> None:
    promote_source = inspect.getsource(PostgresPublicationFence.promote)
    retention_source = inspect.getsource(
        PostgresPublicationFence._cleanup_bundles_once
    )
    for source in (promote_source, retention_source):
        assert "RETURNING mutation_epoch" in source
