from __future__ import annotations

import fcntl
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ADVISORY_LOCK_KEY = 7_260_011
SUBPROCESS_TIMEOUT_SECONDS = 10
UNICODE_WHITESPACE = (
    "\u0009\u000a\u000b\u000c\u000d\u0020\u0085\u00a0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000"
)
POPULATED_SQL = (
    "COALESCE(btrim(CAST(:value AS text), CAST(:whitespace AS text)) <> '', "
    "FALSE)"
)
BASE_POPULATED_CASES = (
    (None, False),
    ("", False),
    (UNICODE_WHITESPACE, False),
    (UNICODE_WHITESPACE[::-1], False),
    ("candidate", True),
    (" \u00a0candidate\u3000", True),
    ("\u200b", True),
    ("\u180e", True),
    ("\ufeff", True),
    ("\u0008", True),
)
POPULATED_CASES = BASE_POPULATED_CASES + tuple(
    (character, False) for character in UNICODE_WHITESPACE
)
LOCK_PROBE = """
import fcntl
import pathlib
import sys

import duckdb

database_path = pathlib.Path(sys.argv[1])
lock_path = pathlib.Path(sys.argv[2])
with lock_path.open("a+") as lock_file:
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(23)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("SELECT 1").fetchone()
"""


@pytest.fixture
def contract_pg_engine(pg_url: str) -> Iterator[Engine]:
    """Connect without resetting public or mutating application tables."""
    engine = create_engine(pg_url)
    yield engine
    engine.dispose()


def test_postgres_reports_database_ctype_and_collation(
    contract_pg_engine: Engine,
) -> None:
    with contract_pg_engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT datcollate, datctype, datlocprovider, datlocale "
                    "FROM pg_database WHERE datname = current_database()"
                )
            )
            .mappings()
            .one()
        )

    assert row["datcollate"]
    assert row["datctype"]
    assert row["datlocprovider"] in {"b", "c", "i"}


def test_populated_predicate_is_ctype_independent(
    contract_pg_engine: Engine,
) -> None:
    assert len(UNICODE_WHITESPACE) == 25
    assert len(set(UNICODE_WHITESPACE)) == 25
    with contract_pg_engine.connect() as connection:
        observed = [
            connection.execute(
                text(f"SELECT {POPULATED_SQL}"),
                {"value": value, "whitespace": UNICODE_WHITESPACE},
            ).scalar_one()
            for value, _expected in POPULATED_CASES
        ]

    expected = [expected for _value, expected in POPULATED_CASES]
    python_observed = [
        value is not None and value.strip(UNICODE_WHITESPACE) != ""
        for value, _expected in POPULATED_CASES
    ]
    assert observed == expected
    assert python_observed == expected


def test_transaction_advisory_lock_contends_then_releases(
    contract_pg_engine: Engine,
) -> None:
    first = contract_pg_engine.connect()
    second = contract_pg_engine.connect()
    first_transaction = first.begin()
    second_transaction = second.begin()
    try:
        assert first.execute(
            text("SELECT pg_try_advisory_xact_lock(:key)"),
            {"key": ADVISORY_LOCK_KEY},
        ).scalar_one()
        assert not second.execute(
            text("SELECT pg_try_advisory_xact_lock(:key)"),
            {"key": ADVISORY_LOCK_KEY},
        ).scalar_one()

        first_transaction.commit()
        assert second.execute(
            text("SELECT pg_try_advisory_xact_lock(:key)"),
            {"key": ADVISORY_LOCK_KEY},
        ).scalar_one()
    finally:
        if first_transaction.is_active:
            first_transaction.rollback()
        if second_transaction.is_active:
            second_transaction.rollback()
        first.close()
        second.close()


def test_duckdb_writer_uses_process_released_sibling_lock(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "analysis.duckdb"
    lock_path = tmp_path / "analysis.duckdb.lock"
    lock_path.touch()

    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        with duckdb.connect(str(database_path)) as connection:
            connection.execute("CREATE TABLE contract_probe(value INTEGER)")

        blocked = subprocess.run(
            [sys.executable, "-c", LOCK_PROBE, database_path, lock_path],
            check=False,
            timeout=SUBPROCESS_TIMEOUT_SECONDS,
        )
        assert blocked.returncode == 23

    acquired = subprocess.run(
        [sys.executable, "-c", LOCK_PROBE, database_path, lock_path],
        check=False,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert acquired.returncode == 0
