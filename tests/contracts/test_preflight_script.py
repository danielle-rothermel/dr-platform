from __future__ import annotations

from decimal import Decimal
from inspect import getsource

import pytest
import typer
from typer.testing import CliRunner

import scripts.platform_v6_preflight as preflight
from scripts.platform_v6_preflight import (
    CaptureSkewResult,
    EndpointRelationship,
    FencingProbeResult,
    ProviderKind,
    app,
    parity_view_model,
    render_fencing,
    run_fencing_probe,
    run_motherduck_fencing_probe,
    verify_capture_skew_result,
)


@pytest.mark.parametrize(
    "failed_field",
    [
        "renewal_cas",
        "current_promotion_succeeded",
        "stale_promotion_rejected",
        "atomic_bundle_pointer",
        "independent_writer_connections",
    ],
)
def test_fencing_probe_exits_nonzero_if_any_contract_fails(
    failed_field: str,
) -> None:
    passing = FencingProbeResult(
        provider=ProviderKind.POSTGRES,
        project_hash="contract-hash",
        renewal_cas=True,
        current_promotion_succeeded=True,
        stale_promotion_rejected=True,
        atomic_bundle_pointer=True,
        independent_writer_connections=True,
    )
    result = passing.model_copy(update={failed_field: False})

    with pytest.raises(typer.Exit) as raised:
        render_fencing(result)

    assert raised.value.exit_code == 2


def test_fencing_probe_returns_normally_when_all_contracts_pass() -> None:
    result = FencingProbeResult(
        provider=ProviderKind.POSTGRES,
        project_hash="contract-hash",
        renewal_cas=True,
        current_promotion_succeeded=True,
        stale_promotion_rejected=True,
        atomic_bundle_pointer=True,
        independent_writer_connections=True,
    )

    assert render_fencing(result) is None


def capture_result(**updates: object) -> CaptureSkewResult:
    result = CaptureSkewResult(
        application_project_hash="application-hash",
        system_project_hash="application-hash",
        sample_count=100,
        p99_skew_ms=1.0,
        median_query_quantum_ms=0.1,
        max_capture_skew_ms=100,
        cap_exceeded=False,
        system_url_fell_back_to_application=True,
        raw_skew_ms=(1.0,),
    )
    return result.model_copy(update=updates)


@pytest.mark.parametrize(
    ("updates", "expected_field"),
    [
        ({"application_project_hash": "drifted"}, "identity_matches"),
        (
            {"system_url_fell_back_to_application": False},
            "relationship_matches",
        ),
        ({"max_capture_skew_ms": 200}, "configured_bound_matches"),
        ({"p99_skew_ms": 100.001}, "measured_within_bound"),
    ],
)
def test_capture_skew_verification_fails_each_pinned_contract_drift(
    updates: dict[str, object], expected_field: str
) -> None:
    verification = verify_capture_skew_result(
        capture_result(**updates),
        expected_application_project_hash="application-hash",
        expected_system_project_hash="application-hash",
        expected_relationship=EndpointRelationship.SAME_ENDPOINT_FALLBACK,
        configured_max_capture_skew_ms=100,
    )

    assert not getattr(verification, expected_field)


def test_capture_skew_verification_rejects_configured_bound_drift() -> None:
    verification = verify_capture_skew_result(
        capture_result(),
        expected_application_project_hash="application-hash",
        expected_system_project_hash="application-hash",
        expected_relationship=EndpointRelationship.SAME_ENDPOINT_FALLBACK,
        configured_max_capture_skew_ms=101,
    )

    assert not verification.configured_bound_matches


def test_capture_skew_verification_accepts_exact_pinned_contract() -> None:
    verification = verify_capture_skew_result(
        capture_result(),
        expected_application_project_hash="application-hash",
        expected_system_project_hash="application-hash",
        expected_relationship=EndpointRelationship.SAME_ENDPOINT_FALLBACK,
        configured_max_capture_skew_ms=100,
    )

    assert all(verification.model_dump().values())


def test_verify_capture_skew_cli_exits_nonzero_on_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubEngine:
        def dispose(self) -> None:
            pass

    monkeypatch.setattr(preflight, "create_engine", lambda _url: StubEngine())
    monkeypatch.setattr(
        preflight,
        "measure_capture_skew",
        lambda *_args, **_kwargs: capture_result(),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "verify-capture-skew",
            "--expected-application-project-hash",
            "wrong-hash",
            "--expected-system-project-hash",
            "application-hash",
        ],
        env={"DATABASE_URL": "postgresql://local/test"},
    )

    assert result.exit_code == 2
    assert "identity_matches=FAIL" in result.stdout


def test_fencing_probes_use_guarded_pointer_promotion_sql() -> None:
    for probe in (run_fencing_probe, run_motherduck_fencing_probe):
        source = getsource(probe)
        assert ".pointer AS pointer" in source
        assert "pointer.bundle_id" in source
        assert "lease.owner" in source
        assert "lease.fencing_token" in source
        assert "lease.expires_at > CURRENT_TIMESTAMP" in source
        assert "RETURNING pointer.bundle_id" in source


def test_parity_view_model_normalizes_live_driver_row_shape() -> None:
    assert parity_view_model(
        ("phase0", Decimal("12.34"), 42)
    ).model_dump() == {
        "label": "phase0",
        "amount": Decimal("12.34"),
        "count": 42,
    }
