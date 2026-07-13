import json
import subprocess
from pathlib import Path

import pytest

from dr_platform.publication import (
    _OPENSSL,
    OpenSslEd25519Signer,
    _verify_spki_ed25519,
    canonical_integrity_json,
)


def test_signed_integrity_jcs_vectors_match_node() -> None:
    vectors = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "signed-integrity-jcs-vectors.json"
        ).read_text()
    )
    for vector in vectors:
        assert canonical_integrity_json(vector["value"]) == vector["canonical"]


@pytest.mark.parametrize("value", [1.25, 9_007_199_254_740_992])
def test_signed_integrity_jcs_rejects_non_portable_numbers(
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        canonical_integrity_json(value)


def test_openssl_ed25519_pem_to_der_spki_round_trip(tmp_path: Path) -> None:
    private = tmp_path / "integrity-private.pem"
    public = tmp_path / "integrity-public.der"
    subprocess.run(
        [_OPENSSL, "genpkey", "-algorithm", "ED25519", "-out", str(private)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            _OPENSSL,
            "pkey",
            "-in",
            str(private),
            "-pubout",
            "-outform",
            "DER",
            "-out",
            str(public),
        ],
        check=True,
        capture_output=True,
    )
    message = b"signed-integrity-round-trip"
    signature = OpenSslEd25519Signer("rotation-1", private).sign(message)
    assert _verify_spki_ed25519(public.read_bytes(), message, signature)
