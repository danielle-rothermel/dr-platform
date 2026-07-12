"""Focused PostgreSQL coverage for platform operation-cut comparisons."""

from __future__ import annotations

from sqlalchemy import Engine, update

from dr_platform.cut import (
    OperationCutMismatchDisposition,
    PlatformOperationCut,
    compare_operation_cuts,
)
from dr_platform.status import ServiceClass
from tests.test_claims import _register


def test_compare_operation_cuts_locks_and_reports_advanced_cut(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(pg_engine, service_classes=(ServiceClass.STANDARD,))
    with pg_engine.begin() as connection:
        current = connection.scalar(
            schema.operations.select().with_only_columns(
                schema.operations.c.platform_cut_version
            )
        )
        assert current is not None
        expected = (
            PlatformOperationCut(
                operation_key="operation", platform_cut_version=current
            ),
        )
        assert compare_operation_cuts(
            connection, expected=expected, schema=schema
        ).matches
        connection.execute(
            update(schema.operations).values(
                platform_cut_version=schema.operations.c.platform_cut_version
                + 1
            )
        )
        comparison = compare_operation_cuts(
            connection, expected=expected, schema=schema
        )
    assert not comparison.matches
    assert comparison.mismatches[0].disposition is (
        OperationCutMismatchDisposition.PLATFORM_CUT_ADVANCED
    )
