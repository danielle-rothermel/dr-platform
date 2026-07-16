"""Execution-target and recipe registration contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Protocol, runtime_checkable

from dr_serialize import sha256_json_digest
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)

from dr_platform.status import WorkflowTopology

if TYPE_CHECKING:
    from dr_platform.items import SubmittableItem

EXECUTION_RECIPE_FORMAT_VERSION = 1
NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]


class ExecutionTargetRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    target_key: NonEmptyStr
    target_version: PositiveInt
    target_contract_digest: NonEmptyStr


class ExecutionRecipeEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format_version: PositiveInt = EXECUTION_RECIPE_FORMAT_VERSION
    target_ref: ExecutionTargetRef
    managed_workflow_name: NonEmptyStr
    managed_workflow_version: PositiveInt
    topology: WorkflowTopology = WorkflowTopology.TOP_LEVEL_ONLY
    argument_recipe_version: PositiveInt
    payload: dict[StrictStr, Any]

    @model_validator(mode="after")
    def validate_recipe(self) -> ExecutionRecipeEnvelope:
        if self.format_version != EXECUTION_RECIPE_FORMAT_VERSION:
            raise ValueError(
                "unsupported execution recipe format version: "
                f"{self.format_version}"
            )
        if self.topology is not WorkflowTopology.TOP_LEVEL_ONLY:
            raise ValueError("only top-level workflows are supported")
        return self

    def digest(self) -> str:
        return sha256_json_digest(self.model_dump(mode="json"))


@runtime_checkable
class SubmissionSource(Protocol):
    """Paged source read exactly once by one submission call."""

    @property
    def item_count(self) -> int: ...

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[SubmittableItem, ...]: ...
