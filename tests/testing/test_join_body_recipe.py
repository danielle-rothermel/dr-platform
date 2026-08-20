from __future__ import annotations

from sqlalchemy import Engine

from dr_platform._core.identities import StageKey
from dr_platform.admission.runner import AdmissionPayload
from dr_platform.execution.stage_completion import StageCompletion
from dr_platform.inspection.work_items import list_episode_predecessor_outputs
from dr_platform.testing import (
    admission_payload_for_stage,
    seed_deferral_episode,
)
from tests.conftest import _migrate


def _join_eval_fanin(
    payload: AdmissionPayload,
    *,
    engine: Engine,
    origin_stage_index: int,
) -> StageCompletion:
    rows = list_episode_predecessor_outputs(
        payload.work_item_id,
        payload.stage_index,
        origin_stage_index=origin_stage_index,
        stage_key=StageKey("eval_row"),
        engine=engine,
    )
    merged = "|".join(row.output_reference for row in rows)
    return StageCompletion(
        output_reference=f"join:{payload.input_reference}:{merged}",
    )


def test_join_body_without_dbos_bootstrap(pg_engine: Engine) -> None:
    """Join stages are testable with Engine + AdmissionPayload, no DBOS."""
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, origin_index, fanin_index = seed_deferral_episode(
            connection,
            schema=schema,
        )

    payload = admission_payload_for_stage(
        pg_engine,
        work_item_id=work_item_id,
        stage_index=fanin_index,
        schema=schema,
    )

    completion = _join_eval_fanin(
        payload,
        engine=pg_engine,
        origin_stage_index=origin_index,
    )

    assert completion.output_reference == (
        "join:fanin:in:1:row:out:1|row:out:2"
    )
