from __future__ import annotations

from decimal import Decimal

import pytest
import typer

from scripts.platform_v6_preflight import (
    FencingProbeResult,
    ProviderKind,
    parity_view_model,
    render_fencing,
)


@pytest.mark.parametrize(
    "failed_field",
    [
        "renewal_cas",
        "stale_writer_rejected",
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
        stale_writer_rejected=True,
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
        stale_writer_rejected=True,
        atomic_bundle_pointer=True,
        independent_writer_connections=True,
    )

    assert render_fencing(result) is None


def test_parity_view_model_normalizes_live_driver_row_shape() -> None:
    assert parity_view_model(
        ("phase0", Decimal("12.34"), 42)
    ).model_dump() == {
        "label": "phase0",
        "amount": Decimal("12.34"),
        "count": 42,
    }
