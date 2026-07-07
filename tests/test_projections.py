from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel
from sqlalchemy import Engine, select

from dr_platform import (
    PlatformSchema,
    ProjectionSpec,
    load_projection_frame,
    load_projection_rows,
    rebuild_projection,
    upgrade_platform_schema,
)


class ScoreRow(BaseModel):
    item_id: str
    group_key: str
    passed: bool
    score: float | None = None
    details: dict[str, Any] | None = None


def _rows_v1(_connection: Any) -> list[ScoreRow]:
    return [
        ScoreRow(
            item_id="a",
            group_key="exp-1",
            passed=True,
            score=1.0,
            details={"n": 1},
        ),
        ScoreRow(item_id="b", group_key="exp-1", passed=False, score=0.0),
        ScoreRow(item_id="c", group_key="exp-2", passed=True),
    ]


SCORES_V1: ProjectionSpec[ScoreRow] = ProjectionSpec(
    name="scores",
    version="v1",
    row_model=ScoreRow,
    build=_rows_v1,
)


@pytest.fixture
def schema(pg_engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(str(pg_engine.url))
    return PlatformSchema()


def test_rebuild_and_load_round_trip(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    result = rebuild_projection(pg_engine, SCORES_V1, schema=schema)
    assert result.row_count == 3

    rows = load_projection_rows(pg_engine, SCORES_V1, schema=schema)
    assert sorted(row.item_id for row in rows) == ["a", "b", "c"]
    by_id = {row.item_id: row for row in rows}
    assert by_id["a"].details == {"n": 1}
    assert by_id["c"].score is None

    filtered = load_projection_rows(
        pg_engine,
        SCORES_V1,
        schema=schema,
        group_key="exp-1",
    )
    assert sorted(row.item_id for row in filtered) == ["a", "b"]


def test_rebuild_is_idempotent_and_updates_registry(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    rebuild_projection(pg_engine, SCORES_V1, schema=schema)
    rebuild_projection(pg_engine, SCORES_V1, schema=schema)

    rows = load_projection_rows(pg_engine, SCORES_V1, schema=schema)
    assert len(rows) == 3  # deleted-and-rebuilt, not appended

    with pg_engine.connect() as connection:
        registry = (
            connection.execute(select(schema.projections)).mappings().all()
        )
    assert len(registry) == 1
    assert registry[0]["projection_name"] == "scores"
    assert registry[0]["projection_version"] == "v1"
    assert registry[0]["row_count"] == 3


def test_versions_coexist_in_one_table(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    rebuild_projection(pg_engine, SCORES_V1, schema=schema)

    def rows_v2(_connection: Any) -> list[ScoreRow]:
        return [ScoreRow(item_id="z", group_key="exp-1", passed=True)]

    scores_v2: ProjectionSpec[ScoreRow] = ProjectionSpec(
        name="scores",
        version="v2",
        row_model=ScoreRow,
        build=rows_v2,
    )
    rebuild_projection(pg_engine, scores_v2, schema=schema)

    v1_rows = load_projection_rows(pg_engine, SCORES_V1, schema=schema)
    v2_rows = load_projection_rows(pg_engine, scores_v2, schema=schema)
    assert len(v1_rows) == 3
    assert [row.item_id for row in v2_rows] == ["z"]

    with pg_engine.connect() as connection:
        registry = (
            connection.execute(select(schema.projections)).mappings().all()
        )
    assert len(registry) == 2


def test_group_filter_requires_group_key_field(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    class BareRow(BaseModel):
        item_id: str

    bare: ProjectionSpec[BareRow] = ProjectionSpec(
        name="bare",
        version="v1",
        row_model=BareRow,
        build=lambda _connection: [BareRow(item_id="a")],
    )
    rebuild_projection(pg_engine, bare, schema=schema)
    with pytest.raises(ValueError, match="has no group_key column"):
        load_projection_rows(
            pg_engine,
            bare,
            schema=schema,
            group_key="exp",
        )


def test_frame_view_matches_rows(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    rebuild_projection(pg_engine, SCORES_V1, schema=schema)
    frame = load_projection_frame(
        pg_engine,
        SCORES_V1,
        schema=schema,
        group_key="exp-1",
    )
    assert sorted(frame["item_id"]) == ["a", "b"]
    assert bool(frame.loc[frame["item_id"] == "a", "passed"].iloc[0])
