"""Rebuildable, versioned analysis projections.

Flat typed tables derived from append-only rows: keyed by
``projection_version``, rebuilt from scratch instead of migrated. The
*query* (``build``) and any plotting stay app-side; the pattern —
typed rows, versioning, registry bookkeeping — is the platform's.

Column mapping is deliberately small: str/int/float/bool/datetime map
to native columns, everything else lands in JSONB. A field named
``group_key`` doubles as the per-sweep filter column.
"""

from __future__ import annotations

import types
import typing

# Runtime imports: pydantic resolves ProjectionSpec's annotations.
from collections.abc import Callable, Iterable  # noqa: TC003
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, StrictStr
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    MetaData,
    Table,
    Text,
    delete,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.engine import Connection  # noqa: TC002

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from dr_platform.db.schema import PlatformSchema

GROUP_KEY_FIELD = "group_key"
PROJECTION_VERSION_COLUMN = "projection_version"
INSERT_CHUNK_SIZE = 1000

_SCALAR_COLUMN_TYPES: tuple[tuple[type, Any], ...] = (
    (bool, Boolean),
    (int, BigInteger),
    (float, Float),
    (str, Text),
    (datetime, DateTime(timezone=True)),
)


class ProjectionSpec[RowT: BaseModel](BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: StrictStr
    version: StrictStr
    row_model: type[RowT]
    build: Callable[[Connection], Iterable[RowT]]


class ProjectionBuildResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: StrictStr
    version: StrictStr
    row_count: int
    built_at: datetime


def projection_table_name(
    spec: ProjectionSpec[Any],
    *,
    schema: PlatformSchema,
) -> str:
    return schema.naming.table_name(f"projection_{spec.name}")


def projection_table(
    spec: ProjectionSpec[Any],
    *,
    schema: PlatformSchema,
) -> Table:
    columns = [Column(PROJECTION_VERSION_COLUMN, Text, nullable=False)]
    for field_name, field in spec.row_model.model_fields.items():
        column_type, nullable = _column_type_for(field.annotation)
        columns.append(Column(field_name, column_type, nullable=nullable))
    return Table(
        projection_table_name(spec, schema=schema),
        MetaData(),
        *columns,
    )


def _column_type_for(annotation: Any) -> tuple[Any, bool]:
    nullable = False
    resolved = annotation
    origin = typing.get_origin(resolved)
    if origin in (typing.Union, types.UnionType):
        arguments = [
            argument
            for argument in typing.get_args(resolved)
            if argument is not type(None)
        ]
        nullable = len(arguments) < len(typing.get_args(resolved))
        if len(arguments) == 1:
            resolved = arguments[0]
        else:
            return JSONB, True
    for scalar_type, column_type in _SCALAR_COLUMN_TYPES:
        if isinstance(resolved, type) and issubclass(resolved, scalar_type):
            return column_type, nullable
    return JSONB, True


def rebuild_projection(
    engine: Engine,
    spec: ProjectionSpec[Any],
    *,
    schema: PlatformSchema,
) -> ProjectionBuildResult:
    """Delete this (name, version)'s rows and rebuild from ``build``."""
    table = projection_table(spec, schema=schema)
    table.metadata.create_all(engine, tables=[table], checkfirst=True)
    built_at = datetime.now(UTC)
    row_count = 0
    with engine.begin() as connection:
        connection.execute(
            delete(table).where(
                table.c[PROJECTION_VERSION_COLUMN] == spec.version
            )
        )
        pending: list[dict[str, Any]] = []
        for row in spec.build(connection):
            values = row.model_dump(mode="json")
            values[PROJECTION_VERSION_COLUMN] = spec.version
            pending.append(values)
            row_count += 1
            if len(pending) >= INSERT_CHUNK_SIZE:
                connection.execute(table.insert().values(pending))
                pending = []
        if pending:
            connection.execute(table.insert().values(pending))
        connection.execute(
            insert(schema.projections)
            .values(
                projection_name=spec.name,
                projection_version=spec.version,
                built_at=built_at,
                row_count=row_count,
            )
            .on_conflict_do_update(
                index_elements=["projection_name", "projection_version"],
                set_={"built_at": built_at, "row_count": row_count},
            )
        )
    return ProjectionBuildResult(
        name=spec.name,
        version=spec.version,
        row_count=row_count,
        built_at=built_at,
    )


def load_projection_rows[RowT: BaseModel](
    engine: Engine,
    spec: ProjectionSpec[RowT],
    *,
    schema: PlatformSchema,
    group_key: str | None = None,
) -> list[RowT]:
    table = projection_table(spec, schema=schema)
    statement = select(table).where(
        table.c[PROJECTION_VERSION_COLUMN] == spec.version
    )
    if group_key is not None:
        if GROUP_KEY_FIELD not in table.c:
            raise ValueError(
                f"projection {spec.name!r} has no {GROUP_KEY_FIELD} column"
            )
        statement = statement.where(table.c[GROUP_KEY_FIELD] == group_key)
    with engine.connect() as connection:
        rows = connection.execute(statement).mappings().all()
    return [
        spec.row_model.model_validate(
            {
                key: value
                for key, value in dict(row).items()
                if key != PROJECTION_VERSION_COLUMN
            }
        )
        for row in rows
    ]


def load_projection_frame(
    engine: Engine,
    spec: ProjectionSpec[Any],
    *,
    schema: PlatformSchema,
    group_key: str | None = None,
) -> Any:
    """DataFrame view; requires the ``pandas`` extra."""
    try:
        import pandas as pd  # noqa: PLC0415 -- optional extra
    except ImportError as error:  # pragma: no cover
        raise ImportError(
            "load_projection_frame requires pandas "
            "(install dr-platform[frames])"
        ) from error
    rows = load_projection_rows(
        engine,
        spec,
        schema=schema,
        group_key=group_key,
    )
    return pd.DataFrame([row.model_dump() for row in rows])
