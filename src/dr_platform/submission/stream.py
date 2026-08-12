from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import UNIQUE, StrEnum, verify
from functools import partial
from typing import TYPE_CHECKING

from dr_serialize import canonical_json_bytes
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert

from dr_platform._core.clock import utc_now
from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    RunKey,
    WorkKey,
    normalize_key,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import (
    validate_labels,
    validate_non_empty_string,
    validate_nonnegative_integer,
    validate_positive_integer,
)
from dr_platform.pipeline.definitions import (
    PipelineIdentity,
    validate_pipeline_identity,
)
from dr_platform.submission.runs import (
    PipelineRunRecord,
    close_registration,
    get_pipeline_run,
    insert_pipeline_run,
)
from dr_platform.submission.work_items import stable_random_rank

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from datetime import datetime

    from sqlalchemy import Connection, Engine

    from dr_platform.pipeline.registry import PipelineRegistry

DEFAULT_CHUNK_SIZE = 10_000
MEMBERSHIP_DIGEST_SCHEMA = "dr-platform/run-membership/v1"


@verify(UNIQUE)
class MembershipDigestField(StrEnum):
    """Persisted digest keys; spell them out at encoding sites."""

    ENTRIES = "entries"
    EXPECTED_MEMBER_COUNT = "expected_member_count"
    SCHEMA = "schema"
    INPUT_REFERENCE = "input_reference"
    ORDINAL = "ordinal"
    WORK_KEY = "work_key"


def _digest_key_bytes(field: MembershipDigestField) -> bytes:
    return b'"' + field.encode() + b'":'


_ENTRIES_OPEN = b"{" + _digest_key_bytes(MembershipDigestField.ENTRIES) + b"["
_ENTRY_OPEN = b"{" + _digest_key_bytes(MembershipDigestField.INPUT_REFERENCE)
_ENTRY_ORDINAL = b"," + _digest_key_bytes(MembershipDigestField.ORDINAL)
_ENTRY_WORK_KEY = b"," + _digest_key_bytes(MembershipDigestField.WORK_KEY)
_ENTRIES_CLOSE = b"]," + _digest_key_bytes(
    MembershipDigestField.EXPECTED_MEMBER_COUNT
)
_DIGEST_SCHEMA_KEY = b"," + _digest_key_bytes(MembershipDigestField.SCHEMA)


@dataclass(frozen=True, slots=True, init=False)
class WorkInput:
    work_key: WorkKey
    input_reference: str
    labels: Mapping[str, str]

    def __init__(
        self,
        *,
        work_key: WorkKey | str,
        input_reference: str,
        labels: Mapping[str, str],
    ) -> None:
        normalized_work_key = normalize_key(work_key, WorkKey)
        normalized_input_reference = validate_non_empty_string(
            input_reference,
            label="input reference",
        )
        normalized_labels = validate_labels(labels, label="work input labels")
        object.__setattr__(self, "work_key", normalized_work_key)
        object.__setattr__(self, "input_reference", normalized_input_reference)
        object.__setattr__(
            self,
            "labels",
            immutable_mapping(normalized_labels),
        )


@dataclass(frozen=True, slots=True)
class RunMemberInput:
    ordinal: int
    work: WorkInput

    def __post_init__(self) -> None:
        validate_nonnegative_integer(self.ordinal, label="member ordinal")
        if not isinstance(self.work, WorkInput):
            raise TypeError("run member work must be a WorkInput")


@dataclass(frozen=True, slots=True)
class RunRegistrationDeclaration:
    expected_member_count: int
    manifest_reference: str | None = None
    membership_digest: str | None = None

    def __post_init__(self) -> None:
        validate_nonnegative_integer(
            self.expected_member_count, label="expected member count"
        )
        if (self.manifest_reference is None) != (
            self.membership_digest is None
        ):
            raise ValueError(
                "manifest reference and membership digest must be supplied "
                "together"
            )
        if self.manifest_reference is not None:
            validate_non_empty_string(
                self.manifest_reference, label="manifest reference"
            )
            validate_non_empty_string(
                self.membership_digest, label="membership digest"
            )


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    run_key: RunKey
    membership_digest: str | None
    registered_member_count: int
    created_work_count: int
    reused_work_count: int
    registration_closed_at: datetime


class RegistrationClosureError(RuntimeError):
    pass


class RunMembershipConflictError(RuntimeError):
    pass


class _MembershipDigester:
    def __init__(self, *, expected_member_count: int) -> None:
        self._hash = hashlib.sha256()
        self._first = True
        self._hash.update(_ENTRIES_OPEN)
        self._expected_member_count = expected_member_count

    def add(
        self, *, ordinal: int, work_key: str, input_reference: str
    ) -> None:
        if not self._first:
            self._hash.update(b",")
        self._first = False
        self._hash.update(_ENTRY_OPEN)
        self._hash.update(canonical_json_bytes(input_reference))
        self._hash.update(_ENTRY_ORDINAL)
        self._hash.update(canonical_json_bytes(ordinal))
        self._hash.update(_ENTRY_WORK_KEY)
        self._hash.update(canonical_json_bytes(work_key))
        self._hash.update(b"}")

    def finish(self) -> str:
        self._hash.update(_ENTRIES_CLOSE)
        self._hash.update(canonical_json_bytes(self._expected_member_count))
        self._hash.update(_DIGEST_SCHEMA_KEY)
        self._hash.update(canonical_json_bytes(MEMBERSHIP_DIGEST_SCHEMA))
        self._hash.update(b"}")
        return self._hash.hexdigest()


def compute_run_membership_digest(
    members: Iterable[RunMemberInput], *, expected_member_count: int
) -> str:
    validate_nonnegative_integer(
        expected_member_count, label="expected member count"
    )
    digester = _MembershipDigester(expected_member_count=expected_member_count)
    member_count = 0
    for member in members:
        if not isinstance(member, RunMemberInput):
            raise TypeError("members must yield RunMemberInput values")
        if member.ordinal != member_count:
            raise ValueError(
                "members must be ordered by contiguous ordinals from zero"
            )
        digester.add(
            ordinal=member.ordinal,
            work_key=member.work.work_key.value,
            input_reference=member.work.input_reference,
        )
        member_count += 1
    if member_count != expected_member_count:
        raise ValueError("member count does not match expected member count")
    return digester.finish()


def submit(  # noqa: PLR0913 -- explicit submission boundary
    *,
    campaign_key: CampaignKey | str,
    run_key: RunKey | str,
    pipeline: PipelineIdentity,
    execution_config_reference: str,
    declaration: RunRegistrationDeclaration,
    members: Iterable[RunMemberInput],
    registry: PipelineRegistry,
    engine: Engine,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    clock: Callable[[], datetime] = utc_now,
    schema: StagingSchema | None = None,
) -> SubmissionReceipt:
    """Register one complete ordered membership in bounded transactions."""
    # Phase 1 — validate and normalize; this ordering is load-bearing, since
    # tests pin that a rejected submission never touches the member input.
    validate_positive_integer(chunk_size, label="chunk size")
    validate_pipeline_identity(pipeline)
    if not isinstance(declaration, RunRegistrationDeclaration):
        raise TypeError("declaration must be a RunRegistrationDeclaration")
    selected_schema = schema or StagingSchema()
    normalized_campaign_key = normalize_key(campaign_key, CampaignKey)
    normalized_run_key = normalize_key(run_key, RunKey)
    pipeline_definition = registry.get(
        key=pipeline.key,
        version=pipeline.version,
    )
    completion = pipeline_definition.run_completion
    if completion is not None and declaration.manifest_reference is None:
        raise ValueError(
            "a completion-enabled pipeline requires a manifest reference "
            "and membership digest"
        )

    # Phase 2 — open or replay the run registration in its own transaction.
    with engine.begin() as connection:
        run = insert_pipeline_run(
            connection,
            run_key=normalized_run_key,
            campaign_key=normalized_campaign_key,
            pipeline_key=pipeline_definition.key.value,
            pipeline_version=pipeline_definition.version,
            execution_config_reference=execution_config_reference,
            expected_member_count=declaration.expected_member_count,
            manifest_reference=declaration.manifest_reference,
            membership_digest=declaration.membership_digest,
            run_completion_key=(
                None if completion is None else completion.key.value
            ),
            created_at=clock(),
            schema=selected_schema,
        )
        if run.registration_closed_at is not None:
            return _receipt(run)

    # Phase 3 — stream members into bounded per-chunk transactions.
    flush = partial(
        _commit_chunk,
        engine=engine,
        schema=selected_schema,
        campaign_key=normalized_campaign_key,
        run_key=normalized_run_key,
        first_stage_key=pipeline_definition.stages[0].key.value,
        clock=clock,
    )
    chunk: list[RunMemberInput] = []
    for member in members:
        if not isinstance(member, RunMemberInput):
            raise TypeError("members must yield RunMemberInput values")
        chunk.append(member)
        if len(chunk) < chunk_size:
            continue
        flush(chunk=chunk)
        chunk.clear()
    if chunk:
        flush(chunk=chunk)

    # Phase 4 — verify the recorded membership and close registration.
    with engine.begin() as connection:
        run = get_pipeline_run(
            connection,
            run_key=normalized_run_key,
            for_update=True,
            schema=selected_schema,
        )
        if run is None:
            raise LookupError(f"pipeline run does not exist: {run_key}")
        if run.registration_closed_at is not None:
            return _receipt(run)
        digest, member_count, created_count = _validate_membership_for_closure(
            connection,
            run=run,
            schema=selected_schema,
        )
        if declaration.membership_digest is not None:
            if digest != declaration.membership_digest:
                raise RegistrationClosureError(
                    "persisted membership digest does not match declaration"
                )
            stored_digest: str | None = digest
        else:
            stored_digest = None
        run = close_registration(
            connection,
            run_key=normalized_run_key,
            membership_digest=stored_digest,
            member_count=member_count,
            created_work_count=created_count,
            reused_work_count=member_count - created_count,
            closed_at=clock(),
            schema=selected_schema,
        )
    return _receipt(run)


def _commit_chunk(  # noqa: PLR0913 -- explicit chunk dependencies
    *,
    engine: Engine,
    schema: StagingSchema,
    campaign_key: CampaignKey,
    run_key: RunKey,
    first_stage_key: str,
    chunk: list[RunMemberInput],
    clock: Callable[[], datetime],
) -> None:
    work_keys = [member.work.work_key.value for member in chunk]
    if len(work_keys) != len(set(work_keys)):
        raise RunMembershipConflictError(
            "a registration chunk contains duplicate work identities"
        )
    ordinals = [member.ordinal for member in chunk]
    if len(ordinals) != len(set(ordinals)):
        raise RunMembershipConflictError(
            "a registration chunk contains duplicate member ordinals"
        )
    ranks_by_work_key = {
        member.work.work_key.value: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                campaign_key, member.work.work_key
            )
        )
        for member in chunk
    }

    with engine.begin() as connection:
        run = get_pipeline_run(
            connection,
            run_key=run_key,
            for_update=True,
            schema=schema,
        )
        if run is None:
            raise LookupError(f"pipeline run does not exist: {run_key}")
        if run.registration_closed_at is not None:
            raise RunMembershipConflictError(
                "closed run membership cannot be changed"
            )

        work_items = schema.work_items
        inserted_rows = connection.execute(
            insert(work_items)
            .values(
                [
                    {
                        "campaign_key": campaign_key.value,
                        "work_key": member.work.work_key.value,
                        "origin_run_key": run_key.value,
                        "input_reference": member.work.input_reference,
                        "labels": dict(member.work.labels),
                        "rank": ranks_by_work_key[member.work.work_key.value],
                    }
                    for member in chunk
                ]
            )
            .on_conflict_do_nothing(
                index_elements=["campaign_key", "work_key"]
            )
            .returning(work_items.c.work_item_id)
        ).scalars()
        inserted_ids = frozenset(inserted_rows)

        origin_runs = schema.pipeline_runs.alias("origin_runs")
        rows = connection.execute(
            select(
                work_items,
                origin_runs.c.pipeline_key.label("origin_pipeline_key"),
                origin_runs.c.pipeline_version.label(
                    "origin_pipeline_version"
                ),
                origin_runs.c.execution_config_reference.label(
                    "origin_execution_config_reference"
                ),
            )
            .select_from(
                work_items.join(
                    origin_runs,
                    work_items.c.origin_run_key == origin_runs.c.run_key,
                )
            )
            .where(
                work_items.c.campaign_key == campaign_key.value,
                work_items.c.work_key.in_(work_keys),
            )
        ).mappings()
        by_key = {row["work_key"]: row for row in rows}
        if len(by_key) != len(chunk):
            raise RuntimeError(
                "bulk work read-back did not resolve every item"
            )
        for member in chunk:
            row = by_key[member.work.work_key.value]
            expected_rank = ranks_by_work_key[member.work.work_key.value]
            if (
                row["input_reference"] != member.work.input_reference
                or dict(row["labels"]) != dict(member.work.labels)
                or row["rank"] != expected_rank
            ):
                raise RunMembershipConflictError(
                    "campaign/work identity is bound to different immutable "
                    "facts"
                )
            if (
                row["origin_pipeline_key"] != run.pipeline_key
                or row["origin_pipeline_version"] != run.pipeline_version
                or row["origin_execution_config_reference"]
                != run.execution_config_reference
            ):
                raise RunMembershipConflictError(
                    "reused work has incompatible execution provenance"
                )

        memberships = schema.run_memberships
        expected_memberships = [
            {
                "run_key": run_key.value,
                "member_ordinal": member.ordinal,
                "work_item_id": by_key[member.work.work_key.value][
                    "work_item_id"
                ],
            }
            for member in chunk
        ]
        connection.execute(
            insert(memberships)
            .values(expected_memberships)
            .on_conflict_do_nothing()
        )
        work_item_ids = [item["work_item_id"] for item in expected_memberships]
        membership_rows = tuple(
            connection.execute(
                select(memberships).where(
                    memberships.c.run_key == run_key.value,
                    or_(
                        memberships.c.member_ordinal.in_(ordinals),
                        memberships.c.work_item_id.in_(work_item_ids),
                    ),
                )
            ).mappings()
        )
        actual_pairs = {
            (row["member_ordinal"], row["work_item_id"])
            for row in membership_rows
        }
        expected_pairs = {
            (item["member_ordinal"], item["work_item_id"])
            for item in expected_memberships
        }
        if actual_pairs != expected_pairs:
            raise RunMembershipConflictError(
                "member ordinal or work identity conflicts with persisted "
                "membership"
            )

        if inserted_ids:
            created_at = clock()
            connection.execute(
                insert(schema.stage_executions).values(
                    [
                        {
                            "work_item_id": row["work_item_id"],
                            "stage_key": first_stage_key,
                            "stage_index": 0,
                            "state": StageExecutionState.READY.value,
                            "current_attempt": 0,
                            "rank": row["rank"],
                            "output_reference": None,
                            "created_at": created_at,
                            "updated_at": created_at,
                        }
                        for row in by_key.values()
                        if row["work_item_id"] in inserted_ids
                    ]
                )
            )


def _validate_membership_for_closure(
    connection: Connection,
    *,
    run: PipelineRunRecord,
    schema: StagingSchema,
) -> tuple[str, int, int]:
    memberships = schema.run_memberships
    work_items = schema.work_items
    statement = (
        select(
            memberships.c.member_ordinal,
            work_items.c.work_key,
            work_items.c.input_reference,
            work_items.c.origin_run_key,
        )
        .select_from(
            memberships.join(
                work_items,
                memberships.c.work_item_id == work_items.c.work_item_id,
            )
        )
        .where(memberships.c.run_key == run.run_key.value)
        .order_by(memberships.c.member_ordinal)
    )
    digester = _MembershipDigester(
        expected_member_count=run.expected_member_count
    )
    member_count = 0
    created_count = 0
    for row in connection.execute(
        statement.execution_options(yield_per=DEFAULT_CHUNK_SIZE)
    ).mappings():
        if row["member_ordinal"] != member_count:
            raise RegistrationClosureError(
                "persisted member ordinals must be contiguous from zero"
            )
        digester.add(
            ordinal=row["member_ordinal"],
            work_key=row["work_key"],
            input_reference=row["input_reference"],
        )
        member_count += 1
        if row["origin_run_key"] == run.run_key.value:
            created_count += 1
    if member_count != run.expected_member_count:
        raise RegistrationClosureError(
            "persisted member count does not match declaration"
        )
    return digester.finish(), member_count, created_count


def _receipt(run: PipelineRunRecord) -> SubmissionReceipt:
    if (
        run.registration_closed_at is None
        or run.registered_member_count is None
        or run.created_work_count is None
        or run.reused_work_count is None
    ):
        raise RuntimeError("open run has no closure receipt")
    return SubmissionReceipt(
        run_key=run.run_key,
        membership_digest=run.membership_digest,
        registered_member_count=run.registered_member_count,
        created_work_count=run.created_work_count,
        reused_work_count=run.reused_work_count,
        registration_closed_at=run.registration_closed_at,
    )
