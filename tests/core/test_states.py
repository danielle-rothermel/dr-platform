from dr_platform._core.ledger.states import StageExecutionState


def test_stage_execution_state_is_exactly_the_minimal_logical_set() -> None:
    assert {state.name for state in StageExecutionState} == {
        "READY",
        "ADMITTED",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    }
    assert "RUNNING" not in StageExecutionState.__members__
