from dr_store import OBJECT_REFERENCE_PREFIX

from dr_platform._core.ledger.evidence import STAGE_FAILURE_EVIDENCE_SCHEMA

SAMPLE_EVIDENCE_REFERENCE = (
    f"{OBJECT_REFERENCE_PREFIX}:{STAGE_FAILURE_EVIDENCE_SCHEMA}:"
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)


def test_stage_failure_evidence_schema_is_pinned() -> None:
    assert STAGE_FAILURE_EVIDENCE_SCHEMA == "stage_failure_evidence"


def test_object_reference_prefix_is_pinned() -> None:
    assert OBJECT_REFERENCE_PREFIX == "dr-store-object:v1"
