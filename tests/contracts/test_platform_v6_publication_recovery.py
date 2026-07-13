"""Public v6 publication-operation recovery contract."""

from __future__ import annotations

import inspect

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


def test_fault_boundaries_are_named_in_platform_protocol() -> None:
    source = inspect.getsource(PostgresPublicationFence)
    for boundary in (
        "after_plan_commit",
        "after_each_stage_member",
        "after_stage_commit",
        "after_promotion_commit",
        "after_cleanup_commit",
    ):
        assert boundary in source
