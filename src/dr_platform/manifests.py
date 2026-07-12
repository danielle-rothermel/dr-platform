"""Frozen manifest and execution-recipe registration contracts."""

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

MANIFEST_FORMAT_VERSION = 3
EXECUTION_RECIPE_FORMAT_VERSION = 1
NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
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


class ManifestPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_index: NonNegativeInt
    start_index: NonNegativeInt
    end_index: PositiveInt
    page_digest: NonEmptyStr

    @model_validator(mode="after")
    def validate_range(self) -> ManifestPage:
        if self.end_index <= self.start_index:
            raise ValueError("manifest pages must be non-empty")
        return self


class OperationManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format_version: PositiveInt = MANIFEST_FORMAT_VERSION
    operation_key: NonEmptyStr
    workflow_role: NonEmptyStr
    group_key: NonEmptyStr
    target_ref: ExecutionTargetRef
    operation_execution_recipe_digest: NonEmptyStr
    item_count: NonNegativeInt
    page_size: PositiveInt
    items_digest: NonEmptyStr
    pages: tuple[ManifestPage, ...]
    manifest_digest: NonEmptyStr

    @model_validator(mode="after")
    def validate_manifest(self) -> OperationManifest:
        if self.format_version != MANIFEST_FORMAT_VERSION:
            raise ValueError(
                f"manifest format version must be {MANIFEST_FORMAT_VERSION}"
            )
        self._validate_pages()
        if self.manifest_digest != self.expected_manifest_digest():
            raise ValueError("manifest_digest does not match manifest content")
        return self

    def _validate_pages(self) -> None:
        expected_page_count = (
            self.item_count + self.page_size - 1
        ) // self.page_size
        if len(self.pages) != expected_page_count:
            raise ValueError("manifest pages do not cover item_count")
        cursor = 0
        for expected_page_index, page in enumerate(self.pages):
            if page.page_index != expected_page_index:
                raise ValueError("manifest page indexes must be contiguous")
            if page.start_index != cursor:
                raise ValueError("manifest page ranges must be contiguous")
            expected_end = min(cursor + self.page_size, self.item_count)
            if page.end_index != expected_end:
                raise ValueError("manifest page has an invalid range")
            cursor = page.end_index
        if cursor != self.item_count:
            raise ValueError("manifest pages do not end at item_count")

    def expected_manifest_digest(self) -> str:
        payload = self.model_dump(mode="json", exclude={"manifest_digest"})
        return sha256_json_digest(payload)


@runtime_checkable
class ManifestSource(Protocol):
    """Re-readable source for the exact ordered Items in a Manifest."""

    @property
    def item_count(self) -> int: ...

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[SubmittableItem, ...]: ...


@runtime_checkable
class ManifestSourceCutValidator(Protocol):
    """Optional strong validation for a complete external source cut."""

    def validate_source_cut(self) -> None: ...
