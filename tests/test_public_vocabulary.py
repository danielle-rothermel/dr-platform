"""Cheap root-facade vocabulary checks with no runtime fixtures."""

import dr_platform

_SUPERSEDED_NAMES = frozenset(
    {
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
)


def test_root_facade_excludes_superseded_vocabulary() -> None:
    assert _SUPERSEDED_NAMES.isdisjoint(dr_platform.__all__)


def test_root_facade_unbinds_superseded_vocabulary() -> None:
    # __all__ only governs star imports; a stray root import could rebind a
    # legacy symbol without tripping the disjointness check above.
    for name in _SUPERSEDED_NAMES:
        assert not hasattr(dr_platform, name)


def test_root_facade_exports_stage_attempt_records() -> None:
    assert "StageAttemptRecord" in dr_platform.__all__
    assert dr_platform.StageAttemptRecord.__name__ == "StageAttemptRecord"
