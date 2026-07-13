from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, text

from dr_platform.publication import OpenSslEd25519Signer

TEST_DATABASE_URL = os.environ.get(
    "DR_PLATFORM_TEST_DATABASE_URL",
    "postgresql+psycopg:///dr_platform_test",
)


@lru_cache(maxsize=1)
def signed_integrity_test_material() -> tuple[
    OpenSslEd25519Signer, dict[str, str]
]:
    """Ephemeral OpenSSL key material used by signed-publication tests."""

    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl unavailable; install it and add it to PATH")

    private = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)  # noqa: SIM115
    private.close()
    public = tempfile.NamedTemporaryFile(suffix=".der", delete=False)  # noqa: SIM115
    public.close()
    subprocess.run(
        [
            openssl,
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            private.name,
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            openssl,
            "pkey",
            "-in",
            private.name,
            "-pubout",
            "-outform",
            "DER",
            "-out",
            public.name,
        ],
        check=True,
        capture_output=True,
    )
    return OpenSslEd25519Signer("test-ed25519", Path(private.name)), {
        "test-ed25519": base64.b64encode(
            Path(public.name).read_bytes()
        ).decode()
    }


@pytest.fixture(scope="session")
def pg_url() -> str:
    try:
        engine = create_engine(TEST_DATABASE_URL)
        with engine.connect():
            pass
        engine.dispose()
    except Exception:  # noqa: BLE001 -- any connect failure means skip
        pytest.skip(
            "postgres unavailable (set DR_PLATFORM_TEST_DATABASE_URL "
            "or create dr_platform_test)"
        )
    return TEST_DATABASE_URL


@pytest.fixture
def clean_pg(pg_url: str) -> str:
    """A scratch database with pgcrypto restored after each schema reset."""
    engine = create_engine(pg_url)
    with engine.begin() as connection:
        # pgcrypto installs its functions in public. Dropping public without
        # removing the extension leaves a catalog entry whose functions no
        # longer exist, so recreate the extension after the schema reset.
        connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("CREATE EXTENSION pgcrypto"))
    engine.dispose()
    return pg_url


@pytest.fixture
def pg_engine(clean_pg: str) -> Iterator[Engine]:
    engine = create_engine(clean_pg)
    yield engine
    engine.dispose()
