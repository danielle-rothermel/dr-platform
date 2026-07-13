"""Durable execution kernel contracts built on DBOS."""

from dr_platform.db import PlatformSchema, upgrade_platform_schema
from dr_platform.dbos_config import (
    PlatformDbosConfig,
    build_dbos_config,
    build_platform_dbos_config,
)
from dr_platform.items import SubmittableItem
from dr_platform.jsonl import submit_jsonl
from dr_platform.manifests import (
    ExecutionRecipeEnvelope,
    ExecutionTargetRef,
    ManifestPage,
    ManifestSource,
    OperationManifest,
)
from dr_platform.records import (
    AttemptRecord,
    EligibilityReference,
    EnqueueClaimRecord,
    EnqueueCompensationRecord,
    FailureSnapshot,
    ItemRecord,
    OperationRecord,
    RetryPolicy,
    ThrottleState,
)
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    FailureClass,
    ItemInsertStatus,
    NextAttemptDisposition,
    NextAttemptReason,
    OperationStatus,
    RetryDisposition,
    ServiceClass,
    WorkflowTopology,
)
from dr_platform.submission import (
    RegistrationHook,
    RegistrationResult,
    SubmitOptions,
    SubmitResult,
    abandon_registration,
    submit,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetRegistry,
    TargetResolver,
)

__all__ = [
    "AttemptEnqueueState",
    "AttemptExecutionState",
    "AttemptRecord",
    "EligibilityReference",
    "EnqueueClaimRecord",
    "EnqueueCompensationRecord",
    "ExecutionIdentity",
    "ExecutionRecipeEnvelope",
    "ExecutionTarget",
    "ExecutionTargetRef",
    "FailureClass",
    "FailureSnapshot",
    "ItemInsertStatus",
    "ItemRecord",
    "ManifestPage",
    "ManifestSource",
    "NextAttemptDisposition",
    "NextAttemptReason",
    "OperationManifest",
    "OperationRecord",
    "OperationStatus",
    "PlatformDbosConfig",
    "PlatformSchema",
    "RegistrationHook",
    "RegistrationResult",
    "RetryDisposition",
    "RetryPolicy",
    "ServiceClass",
    "SubmitOptions",
    "SubmitResult",
    "SubmittableItem",
    "TargetRegistry",
    "TargetResolver",
    "ThrottleState",
    "WorkflowTopology",
    "abandon_registration",
    "build_dbos_config",
    "build_platform_dbos_config",
    "submit",
    "submit_jsonl",
    "upgrade_platform_schema",
]
