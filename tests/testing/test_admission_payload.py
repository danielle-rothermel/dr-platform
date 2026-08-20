from __future__ import annotations

from sqlalchemy import Engine, select

from dr_platform._core.identities import CampaignKey, RunKey, StageKey, WorkKey
from dr_platform.admission.runner import AdmissionPayload, run_admission_pass
from dr_platform.testing import (
    admission_payload_for_stage,
    seed_work_item,
    succeed_stage,
)
from tests.admission.test_runner import _control, _registry, _submit
from tests.conftest import NOW, _as_dbos_client, _migrate, _RecordingClient


def test_admission_payload_for_stage_matches_admission_runner(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({"cohort": "blue"},))
    _control(pg_engine, selector=None, capacity=1)
    client = _RecordingClient()

    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    runner_payload = AdmissionPayload.model_validate(
        client.enqueued_args[0][0]
    )
    schema = _migrate(pg_engine)
    with pg_engine.connect() as connection:
        work_item_id = connection.execute(
            select(schema.work_items.c.work_item_id).where(
                schema.work_items.c.work_key == "work-0"
            )
        ).scalar_one()

    assembled = admission_payload_for_stage(
        pg_engine,
        work_item_id=work_item_id,
        stage_index=0,
    )
    assert assembled == runner_payload
    assert assembled == AdmissionPayload(
        campaign_key=CampaignKey("campaign-1"),
        work_key=WorkKey("work-0"),
        work_item_id=work_item_id,
        origin_run_key=RunKey("run-1"),
        input_reference="input:0",
        labels={"cohort": "blue"},
        pipeline_key="evaluation",
        pipeline_version=1,
        stage_key=StageKey("execute"),
        stage_index=0,
        attempt_number=1,
    )


def test_admission_payload_for_stage_uses_stage_input_reference(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = seed_work_item(
            connection,
            campaign_key="campaign-stage-input",
            work_key="work-stage-input",
            run_key="run-stage-input",
            input_reference="submission-input",
            schema=schema,
        )
        succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=0,
            input_reference="stage-input",
            output_reference="stage-output",
        )

    payload = admission_payload_for_stage(
        pg_engine,
        work_item_id=work_item_id,
        stage_index=0,
    )
    assert payload.input_reference == "stage-input"
