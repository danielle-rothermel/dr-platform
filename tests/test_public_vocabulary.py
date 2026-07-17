"""Cheap root-facade vocabulary checks with no runtime fixtures."""

import dr_platform


def test_root_facade_excludes_superseded_vocabulary() -> None:
    superseded_names = {
        "AttemptRecord",
        "EnqueueClaimRecord",
        "JsonlFieldNames",
        "OperationRecord",
        "ServiceClass",
        "cancel_operation",
        "reconcile",
        "request_next_attempt",
        "submit_jsonl",
    }

    assert superseded_names.isdisjoint(dr_platform.__all__)


def test_root_facade_exports_stage_attempt_records() -> None:
    assert "StageAttemptRecord" in dr_platform.__all__
    assert dr_platform.StageAttemptRecord.__name__ == "StageAttemptRecord"
