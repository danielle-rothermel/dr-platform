from __future__ import annotations

from datetime import timedelta

from sqlalchemy import Engine

from dr_platform._core.identities import PipelineKey
from dr_platform.admission.controls import (
    list_stage_controls,
    read_controls,
    set_selector_capacity,
    set_stage_capacity,
    upsert_stage_control,
)
from dr_platform.pipeline.definitions import PipelineIdentity
from tests.conftest import NOW, _migrate


def test_empty_selector_is_default_and_upserts_as_one_control(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        original = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector=None,
            capacity=1,
            paused=False,
            updated_at=NOW,
        )
        replaced = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector={},
            capacity=3,
            paused=True,
            updated_at=NOW + timedelta(seconds=1),
        )
        selected = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector={"cohort": "blue"},
            capacity=1,
            paused=False,
            updated_at=NOW,
        )
        red_controls = list_stage_controls(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            labels={"cohort": "red"},
        )
        blue_controls = list_stage_controls(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            labels={"cohort": "blue"},
        )

    assert replaced.stage_control_id == original.stage_control_id
    assert (
        replaced.selector,
        replaced.capacity,
        replaced.paused,
    ) == ({}, 3, True)
    assert red_controls == (replaced,)
    assert blue_controls == (replaced, selected)


def test_read_controls_returns_stage_capacity(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    set_stage_capacity(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    controls = read_controls(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        engine=pg_engine,
    )

    assert controls[0].capacity == 4


def test_read_controls_filters_selectors_by_work_item_labels(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    pipeline = PipelineIdentity(PipelineKey("evaluation"), 1)
    set_stage_capacity(
        pipeline=pipeline,
        stage_key="execute",
        capacity=8,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample"},
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample", "cohort": "blue"},
        capacity=2,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "other"},
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    controls = read_controls(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample", "cohort": "blue", "split": "validation"},
        engine=pg_engine,
    )

    assert [
        (dict(control.selector), control.capacity) for control in controls
    ] == [
        ({}, 8),
        ({"kind": "sample"}, 4),
        ({"kind": "sample", "cohort": "blue"}, 2),
    ]
