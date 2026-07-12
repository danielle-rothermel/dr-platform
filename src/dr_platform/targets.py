"""Runtime execution-target registration and resolution contracts."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from enum import StrEnum
from typing import Annotated, Any, Protocol, runtime_checkable

from dr_serialize import sha256_json_digest
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)

from dr_platform.items import SubmittableItem
from dr_platform.manifests import (
    EXECUTION_RECIPE_FORMAT_VERSION,
    ExecutionRecipeEnvelope,
    ExecutionTargetRef,
)
from dr_platform.records import FailureSnapshot, ItemRecord
from dr_platform.status import WorkflowTopology
from dr_platform.submission import (  # noqa: TC001 -- pydantic resolves it
    RegistrationHook,
)

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
TargetIdentity = tuple[str, int]
WorkflowCallable = Callable[..., object]
ExecutionIdentityCallable = Callable[[ItemRecord, int], "ExecutionIdentity"]
ArgumentsCallable = Callable[[ItemRecord, int], tuple[Any, ...]]
RecipeCallable = Callable[[SubmittableItem], ExecutionRecipeEnvelope]
ErrorClassifier = Callable[[BaseException], FailureSnapshot]


class ExecutionIdentity(BaseModel):
    """Content-scoped identity used for one managed workflow execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    execution_key: NonEmptyStr
    workflow_id: NonEmptyStr


class TargetContractDeclaration(BaseModel):
    """Serializable declaration whose digest defines a target contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    queue_name: NonEmptyStr
    workflow_role: NonEmptyStr
    managed_workflow_name: NonEmptyStr
    managed_workflow_version: PositiveInt
    topology: WorkflowTopology = WorkflowTopology.TOP_LEVEL_ONLY
    argument_recipe_version: PositiveInt
    recipe_envelope_version: PositiveInt = EXECUTION_RECIPE_FORMAT_VERSION
    classifier_version: PositiveInt
    registration_hook_name: NonEmptyStr | None = None
    registration_hook_version: PositiveInt | None = None

    @model_validator(mode="after")
    def validate_declaration(self) -> TargetContractDeclaration:
        if self.topology is not WorkflowTopology.TOP_LEVEL_ONLY:
            raise ValueError("only top-level workflows are supported")
        if self.recipe_envelope_version != EXECUTION_RECIPE_FORMAT_VERSION:
            raise ValueError(
                "unsupported execution recipe format version: "
                f"{self.recipe_envelope_version}"
            )
        hook_fields = (
            self.registration_hook_name,
            self.registration_hook_version,
        )
        if any(value is None for value in hook_fields) and any(
            value is not None for value in hook_fields
        ):
            raise ValueError(
                "registration hook name and version must be set together"
            )
        return self

    def digest(self) -> str:
        return sha256_json_digest(self.model_dump(mode="json"))

    def target_ref(
        self,
        *,
        target_key: str,
        target_version: int,
    ) -> ExecutionTargetRef:
        return ExecutionTargetRef(
            target_key=target_key,
            target_version=target_version,
            target_contract_digest=self.digest(),
        )


class ExecutionTarget(BaseModel):
    """Runtime-only behavior registered under an immutable target reference."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    ref: ExecutionTargetRef
    queue_name: NonEmptyStr
    workflow_role: NonEmptyStr
    managed_workflow_name: NonEmptyStr
    managed_workflow_version: PositiveInt
    topology: WorkflowTopology = WorkflowTopology.TOP_LEVEL_ONLY
    argument_recipe_version: PositiveInt
    recipe_envelope_version: PositiveInt = EXECUTION_RECIPE_FORMAT_VERSION
    classifier_version: PositiveInt
    registration_hook_name: NonEmptyStr | None = None
    registration_hook_version: PositiveInt | None = None
    workflow: WorkflowCallable = Field(exclude=True)
    execution_for: ExecutionIdentityCallable = Field(exclude=True)
    args_for: ArgumentsCallable = Field(exclude=True)
    recipe_for: RecipeCallable = Field(exclude=True)
    classify_error: ErrorClassifier = Field(exclude=True)
    registration_hook: RegistrationHook | None = Field(
        default=None,
        exclude=True,
    )

    @model_validator(mode="after")
    def validate_target(self) -> ExecutionTarget:
        declaration = self.contract_declaration()
        if (self.registration_hook is None) != (
            declaration.registration_hook_name is None
        ):
            raise ValueError(
                "registration hook callable and declaration must be set "
                "together"
            )
        return self

    def contract_declaration(self) -> TargetContractDeclaration:
        return TargetContractDeclaration(
            queue_name=self.queue_name,
            workflow_role=self.workflow_role,
            managed_workflow_name=self.managed_workflow_name,
            managed_workflow_version=self.managed_workflow_version,
            topology=self.topology,
            argument_recipe_version=self.argument_recipe_version,
            recipe_envelope_version=self.recipe_envelope_version,
            classifier_version=self.classifier_version,
            registration_hook_name=self.registration_hook_name,
            registration_hook_version=self.registration_hook_version,
        )


class TargetResolutionErrorCode(StrEnum):
    TARGET_UNAVAILABLE = "target_unavailable"
    TARGET_CONFLICT = "target_conflict"


class TargetResolutionFailure(BaseModel):
    """Typed, serializable detail for a fail-closed target lookup."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: TargetResolutionErrorCode
    target_ref: ExecutionTargetRef
    message: NonEmptyStr


class TargetResolutionError(RuntimeError):
    def __init__(self, failure: TargetResolutionFailure) -> None:
        super().__init__(failure.message)
        self.failure = failure

    @property
    def code(self) -> TargetResolutionErrorCode:
        return self.failure.code

    @property
    def target_ref(self) -> ExecutionTargetRef:
        return self.failure.target_ref


class TargetUnavailableError(TargetResolutionError):
    pass


class TargetConflictError(TargetResolutionError):
    pass


@runtime_checkable
class TargetResolver(Protocol):
    def resolve(self, target_ref: ExecutionTargetRef) -> ExecutionTarget: ...


class TargetRegistry:
    """Concrete startup registry for the complete process target set."""

    def __init__(self) -> None:
        self._targets: dict[TargetIdentity, ExecutionTarget] = {}
        self._declarations: dict[
            TargetIdentity, TargetContractDeclaration
        ] = {}
        self._workflow_refs: dict[str, ExecutionTargetRef] = {}

    def register(self, target: ExecutionTarget) -> ExecutionTarget:
        targets = dict(self._targets)
        declarations = dict(self._declarations)
        workflow_refs = dict(self._workflow_refs)
        registered = self._register_into(
            target,
            targets=targets,
            declarations=declarations,
            workflow_refs=workflow_refs,
        )
        self._targets = targets
        self._declarations = declarations
        self._workflow_refs = workflow_refs
        return registered

    def register_all(
        self,
        targets: Iterable[ExecutionTarget],
    ) -> tuple[ExecutionTarget, ...]:
        next_targets = dict(self._targets)
        next_declarations = dict(self._declarations)
        next_workflow_refs = dict(self._workflow_refs)
        registered = tuple(
            self._register_into(
                target,
                targets=next_targets,
                declarations=next_declarations,
                workflow_refs=next_workflow_refs,
            )
            for target in targets
        )
        self._targets = next_targets
        self._declarations = next_declarations
        self._workflow_refs = next_workflow_refs
        return registered

    def resolve(self, target_ref: ExecutionTargetRef) -> ExecutionTarget:
        identity = self._identity(target_ref)
        target = self._targets.get(identity)
        if target is None:
            raise TargetUnavailableError(
                self._failure(
                    TargetResolutionErrorCode.TARGET_UNAVAILABLE,
                    target_ref,
                    "execution target is not registered",
                )
            )
        if target.ref.target_contract_digest != (
            target_ref.target_contract_digest
        ):
            raise TargetConflictError(
                self._failure(
                    TargetResolutionErrorCode.TARGET_CONFLICT,
                    target_ref,
                    "execution target contract digest does not match the "
                    "registered target",
                )
            )
        return target

    def _register_into(
        self,
        target: ExecutionTarget,
        *,
        targets: dict[TargetIdentity, ExecutionTarget],
        declarations: dict[TargetIdentity, TargetContractDeclaration],
        workflow_refs: dict[str, ExecutionTargetRef],
    ) -> ExecutionTarget:
        declaration = target.contract_declaration()
        if target.ref.target_contract_digest != declaration.digest():
            raise TargetConflictError(
                self._failure(
                    TargetResolutionErrorCode.TARGET_CONFLICT,
                    target.ref,
                    "execution target contract digest does not match its "
                    "declaration",
                )
            )

        identity = self._identity(target.ref)
        existing = targets.get(identity)
        if existing is not None:
            if declarations[identity] != declaration:
                raise TargetConflictError(
                    self._failure(
                        TargetResolutionErrorCode.TARGET_CONFLICT,
                        target.ref,
                        "target key and version are already registered with "
                        "a different declaration",
                    )
                )
            if existing.workflow is not target.workflow:
                raise TargetConflictError(
                    self._failure(
                        TargetResolutionErrorCode.TARGET_CONFLICT,
                        target.ref,
                        "managed workflow name is already registered with a "
                        "different callable",
                    )
                )
            return existing

        existing_workflow_ref = workflow_refs.get(target.managed_workflow_name)
        if existing_workflow_ref is not None and existing_workflow_ref != (
            target.ref
        ):
            raise TargetConflictError(
                self._failure(
                    TargetResolutionErrorCode.TARGET_CONFLICT,
                    target.ref,
                    "managed workflow name is already registered under a "
                    "different target reference",
                )
            )

        targets[identity] = target
        declarations[identity] = declaration
        workflow_refs[target.managed_workflow_name] = target.ref
        return target

    @staticmethod
    def _identity(target_ref: ExecutionTargetRef) -> TargetIdentity:
        return target_ref.target_key, target_ref.target_version

    @staticmethod
    def _failure(
        code: TargetResolutionErrorCode,
        target_ref: ExecutionTargetRef,
        message: str,
    ) -> TargetResolutionFailure:
        return TargetResolutionFailure(
            code=code,
            target_ref=target_ref,
            message=message,
        )
