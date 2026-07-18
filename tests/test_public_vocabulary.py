"""Cheap root-facade vocabulary checks with no runtime fixtures."""

import dr_platform


def test_root_facade_exports_stage_attempt_records() -> None:
    assert "StageAttemptRecord" in dr_platform.__all__
    assert dr_platform.StageAttemptRecord.__name__ == "StageAttemptRecord"
