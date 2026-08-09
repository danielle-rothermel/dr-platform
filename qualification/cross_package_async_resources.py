from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from functools import partial
from importlib import import_module
from importlib.metadata import distribution as metadata_distribution
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote, urlparse
from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import Engine, create_engine, make_url, select, text

from dr_platform import (
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    PlatformDbosConfig,
    RunCompletionDefinition,
    RunCompletionExecutionState,
    RunCompletionKey,
    RunCompletionPayload,
    RunMemberInput,
    RunRegistrationDeclaration,
    StageDefinition,
    StageExecutionState,
    StageKey,
    StagingSchema,
    WorkInput,
    WorkKey,
    bulk_work_statuses,
    compute_run_membership_digest,
    get_work_item_stages,
    initialize_dbos_runtime,
    inspect_run_completion,
    register_scheduled_dispatcher,
    set_stage_capacity,
    submit,
    upgrade_platform_schema,
    wrap_pipeline_workflows,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy.engine import URL

WATCHDOG_SECONDS = 30.0
MEMBER_COUNT = 2
CAMPAIGN_KEY = "cross-package-qualification"
RUN_KEY = "cross-package-run"
MANIFEST_SCHEMA = "dr_platform.qualification_manifest.v1"
STAGE_SCHEMA = "dr_platform.qualification_stage_result.v1"
AGGREGATE_SCHEMA = "dr_platform.qualification_aggregate.v1"
EXECUTION_CONFIG_REFERENCE = "config:cross-package-v1"
REPOSITORIES = Path("/Users/daniellerothermel/drotherm/repos")
PINS = {
    "dr-store": (
        "dr_store",
        REPOSITORIES / "dr-store",
        "0.2.0",
        "9787e72190c7fe1b2d3579c0179cae7d00a396d5",
        "v0.2.0",
    ),
    "dr-providers": (
        "dr_providers",
        REPOSITORIES / "dr-providers",
        "0.3.0",
        "f4931d71c3a2cec4c03caae03b02ccb8188000c6",
        "v0.3.0",
    ),
    "dr-exec": (
        "dr_exec",
        REPOSITORIES / "dr-exec",
        "0.1.7",
        "c06b45796b741dd2cac3c87955b8f3f239a7991e",
        "v0.1.7",
    ),
}


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _git(repository: Path, *args: str) -> str:
    return subprocess.check_output(  # noqa: S603 -- fixed executable
        ("/usr/bin/git", "-C", str(repository), *args),
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


def _repo_state(repository: Path) -> dict[str, object]:
    status = _git(repository, "status", "--short", "--untracked-files=all")
    return {
        "commit": _git(repository, "rev-parse", "HEAD"),
        "tags": tuple(
            _git(repository, "tag", "--points-at", "HEAD").splitlines()
        ),
        "status": tuple(status.splitlines()),
        "clean": not status,
    }


def _preflight_packages() -> dict[str, Mapping[str, object]]:
    packages: dict[str, Mapping[str, object]] = {}
    for distribution, pin in PINS.items():
        module_name, repository, want_version, commit, tag = pin
        installed = metadata_distribution(distribution)
        actual_version = installed.version
        direct_url_text = installed.read_text("direct_url.json")
        if direct_url_text is None:
            raise RuntimeError(f"{distribution} has no direct_url.json")
        direct_url = json.loads(direct_url_text).get("url")
        if not isinstance(direct_url, str):
            raise TypeError(f"{distribution} has no direct source URL")
        parsed_url = urlparse(direct_url)
        _require(
            parsed_url.scheme == "file"
            and parsed_url.netloc in {"", "localhost"},
            f"{distribution} does not have a local file source",
        )
        installed_source = Path(unquote(parsed_url.path)).resolve()
        state = _repo_state(repository)
        module = import_module(module_name)
        _require(
            actual_version == want_version, f"{distribution} version mismatch"
        )
        _require(state["commit"] == commit, f"{distribution} commit mismatch")
        _require(
            installed_source == repository.resolve(),
            f"{distribution} installed source is not the pinned repository",
        )
        _require(
            tag in cast("tuple[str, ...]", state["tags"]),
            f"{distribution} tag mismatch",
        )
        _require(state["clean"], f"{distribution} source tree is dirty")
        module_file = module.__file__
        if module_file is None:
            raise RuntimeError(f"{distribution} module has no source file")
        resolved_module_file = Path(module_file).resolve()
        expected_module_file = (
            repository / f"src/{module_name}/__init__.py"
        ).resolve()
        _require(
            resolved_module_file == expected_module_file,
            f"imported {distribution} is not from its pinned repository",
        )
        packages[distribution] = {
            "version": actual_version,
            "commit": state["commit"],
            "tag": tag,
            "git_status": state["status"],
            "repository": str(repository),
            "installed_source": str(installed_source),
            "module_file": str(resolved_module_file),
        }
    return packages


def _platform_source(repository: Path) -> str:
    module_file = import_module("dr_platform").__file__
    if module_file is None:
        raise RuntimeError("dr-platform module has no source file")
    actual = Path(module_file).resolve()
    expected = (repository / "src/dr_platform/__init__.py").resolve()
    _require(
        actual == expected,
        "imported dr-platform source is not this repository",
    )
    return str(actual)


class _Dependencies:
    def __init__(self) -> None:
        self.store = import_module("dr_store")
        self.providers = import_module("dr_providers")
        self.exec = import_module("dr_exec")


class _Bridge:
    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=6, thread_name_prefix="cross-package-app"
        )
        self._lock = threading.Lock()
        self.calls: dict[str, int] = {}
        self.shutdown_complete = False

    def submit[T](self, kind: str, function: Callable[[], T]) -> Future[T]:
        return cast(
            "Future[T]", self._executor.submit(self._invoke, kind, function)
        )

    def _invoke[T](self, kind: str, function: Callable[[], T]) -> T:
        with self._lock:
            self.calls[kind] = self.calls.get(kind, 0) + 1
        return function()

    async def call[T](self, kind: str, function: Callable[[], T]) -> T:
        return await asyncio.wait_for(
            asyncio.wrap_future(self.submit(kind, function)), WATCHDOG_SECONDS
        )

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)
        self.shutdown_complete = True


class _Resources:
    def __init__(
        self, deps: _Dependencies, sqlite_path: Path, bridge: _Bridge
    ):
        self.deps = deps
        self.sqlite_path = sqlite_path
        self.bridge = bridge
        self.provider = deps.providers.ScriptedProvider(
            [
                deps.providers.ScriptedOutcome(
                    text="scripted qualification response"
                )
            ]
        )
        self.provider_lock = threading.Lock()
        self.registered_member_count: int | None = None
        self.executor: Any | None = None
        self.backend: Any | None = None
        self.store: Any | None = None
        self.open_lock = asyncio.Lock()
        self.stage_gate = asyncio.Event()
        self.entered: set[str] = set()
        self.active = 0
        self.peak_active = 0
        self.loop_ids: list[int] = []
        self.open_loop: int | None = None
        self.close_loop: int | None = None
        self.stage_artifacts: dict[str, Mapping[str, object]] = {}

    def capture_loop(self) -> int:
        loop_id = id(asyncio.get_running_loop())
        self.loop_ids.append(loop_id)
        if self.open_loop is not None:
            _require(loop_id == self.open_loop, "workflow event loop changed")
        return loop_id

    async def object_store(self) -> Any:
        loop_id = self.capture_loop()
        async with self.open_lock:
            if self.backend is None:
                self.backend = await self.deps.store.SqliteBackend.open(
                    self.sqlite_path
                )
                self.store = self.deps.store.ObjectStore(self.backend)
                self.open_loop = loop_id
        return self.store

    async def enter_stage(self, input_reference: str) -> None:
        self.capture_loop()
        self.entered.add(input_reference)
        self.active += 1
        self.peak_active = max(self.active, self.peak_active)
        if len(self.entered) == MEMBER_COUNT:
            self.stage_gate.set()
        await asyncio.wait_for(self.stage_gate.wait(), WATCHDOG_SECONDS)

    async def close(self) -> None:
        loop_id = self.capture_loop()
        backend = self.backend
        if backend is None:
            raise RuntimeError("store was never opened")
        await backend.aclose()
        self.close_loop = loop_id


def _encode(reference: Any) -> str:
    return f"{reference.schema}:{reference.content_hash}"


def _decode(deps: _Dependencies, value: str) -> Any:
    schema, separator, content_hash = value.rpartition(":")
    _require(separator and schema and content_hash, "invalid object reference")
    return deps.store.ObjectReference(schema=schema, content_hash=content_hash)


def _manifest(
    members: tuple[RunMemberInput, ...], digest: str
) -> Mapping[str, object]:
    return {
        "schema": MANIFEST_SCHEMA,
        "campaign_key": CAMPAIGN_KEY,
        "run_key": RUN_KEY,
        "membership_digest": digest,
        "members": [
            {
                "ordinal": member.ordinal,
                "work_key": str(member.work.work_key),
                "input_reference": member.work.input_reference,
            }
            for member in members
        ],
    }


def _budgets(deps: _Dependencies) -> Any:
    output_bytes = 1_024
    return deps.exec.Budgets(
        wall_time=deps.exec.FiniteDurationLimit(max_ns=5_000_000_000),
        input_bytes=deps.exec.FiniteByteLimit(max_bytes=1),
        payload_output=deps.exec.FiniteOutput(
            max_bytes=output_bytes,
            overflow_policy=deps.exec.OutputOverflowPolicy.FAIL,
            retention=deps.exec.PayloadRetentionBudget(
                stdout=deps.exec.StreamRetentionBudget(
                    head_bytes=output_bytes, tail_bytes=0
                ),
                stderr=deps.exec.StreamRetentionBudget(
                    head_bytes=0, tail_bytes=0
                ),
            ),
        ),
    )


def _provider(resources: _Resources, input_reference: str) -> str:
    providers = resources.deps.providers
    request = providers.ProviderCallRequest(
        config=providers.openai_chat_config(model="scripted-qualification"),
        transcript=providers.Transcript(
            messages=(
                providers.PromptMessage(
                    role=providers.MessageRole.USER,
                    content=f"qualify {input_reference}",
                ),
            )
        ),
    )
    with resources.provider_lock:
        outcome = resources.provider.invoke(request).outcome
    _require(
        isinstance(outcome, providers.ProviderTransportResponse),
        "scripted provider returned a non-response",
    )
    _require(
        outcome.text == "scripted qualification response",
        "scripted response changed",
    )
    return outcome.text


def _process(resources: _Resources, input_reference: str) -> str:
    dependency = resources.deps.exec
    expected = f"executed:{input_reference}"
    executor = resources.executor
    if executor is None:
        raise RuntimeError("process executor is absent")
    completed = executor.run(
        dependency.ExecutionJob(
            job_id=dependency.JobId(uuid4()),
            target=dependency.TrustedCommandTarget(
                argv=(
                    sys.executable,
                    "-I",
                    "-c",
                    "import sys; sys.stdout.write(sys.argv[1])",
                    expected,
                )
            ),
            env=dependency.EnvGrant.none(),
            budgets=_budgets(resources.deps),
        )
    )
    result = completed.result
    _require(
        isinstance(result.outcome, dependency.ExitedOutcome)
        and result.outcome.exit_code == 0,
        "process did not return ExitedOutcome(0)",
    )
    stdout = result.payload_outputs.stdout
    _require(stdout.dropped_bytes == 0, "process stdout was truncated")
    _require(
        stdout.head + stdout.tail == expected.encode(),
        "process stdout changed",
    )
    _require(
        result.payload_outputs.stderr.produced_bytes == 0,
        "process wrote stderr",
    )
    return expected


def _stage_outcomes(engine: Engine) -> tuple[Mapping[str, object], ...]:
    statuses = bulk_work_statuses(
        CAMPAIGN_KEY, ("work-0", "work-1"), engine=engine
    ).statuses
    outcomes: list[Mapping[str, object]] = []
    for work_key in ("work-0", "work-1"):
        status = statuses[WorkKey(work_key)]
        work_item_id = status.work_item_id
        if not status.present or work_item_id is None:
            raise RuntimeError(f"{work_key} is absent")
        stages = get_work_item_stages(work_item_id, engine=engine)
        _require(len(stages) == 1, f"{work_key} stage history changed")
        execution = stages[0].execution
        outcomes.append(
            {
                "work_key": work_key,
                "state": execution.state.value,
                "output_reference": execution.output_reference,
            }
        )
    return tuple(outcomes)


def _pipeline(  # noqa: PLR0915 -- explicit qualification workflow
    resources: _Resources,
    engine: Engine,
    manifest: Mapping[str, object],
    manifest_reference: str,
    suffix: str,
) -> PipelineDefinition:
    expected_pipeline_key = f"cross-package-{suffix}"

    async def stage(input_reference: str) -> str:
        await resources.enter_stage(input_reference)
        try:
            store = await resources.object_store()
            stored_manifest, _ = await store.put(MANIFEST_SCHEMA, manifest)
            _require(
                _encode(stored_manifest) == manifest_reference,
                "manifest changed",
            )
            _require(
                await store.get(stored_manifest) == manifest,
                "manifest read failed",
            )
            await store.bind("qualification-manifest", stored_manifest)
            _require(
                await store.resolve("qualification-manifest")
                == stored_manifest,
                "manifest resolution failed",
            )
            provider_text = await resources.bridge.call(
                "provider", partial(_provider, resources, input_reference)
            )
            stdout = await resources.bridge.call(
                "exec", partial(_process, resources, input_reference)
            )
            artifact: Mapping[str, object] = {
                "schema": STAGE_SCHEMA,
                "input_reference": input_reference,
                "provider_text": provider_text,
                "exec_stdout": stdout,
                "exit_code": 0,
            }
            reference, _ = await store.put(STAGE_SCHEMA, artifact)
            key = f"stage-result-{input_reference}"
            await store.bind(key, reference)
            _require(
                await store.get(reference) == artifact,
                "stage artifact changed",
            )
            _require(
                await store.resolve(key) == reference, "stage resolve failed"
            )
            resources.stage_artifacts[input_reference] = artifact
            return _encode(reference)
        finally:
            resources.active -= 1

    async def completion(payload: RunCompletionPayload) -> str:
        store = await resources.object_store()
        _require(
            str(payload.campaign_key) == CAMPAIGN_KEY, "campaign key changed"
        )
        _require(str(payload.run_key) == RUN_KEY, "run key changed")
        _require(
            str(payload.pipeline_key) == expected_pipeline_key
            and payload.pipeline_version == 1,
            "pipeline identity changed",
        )
        _require(
            payload.execution_config_reference == EXECUTION_CONFIG_REFERENCE,
            "execution config reference changed",
        )
        _require(
            payload.member_count
            == resources.registered_member_count
            == MEMBER_COUNT,
            "completion member count changed",
        )
        _require(
            payload.manifest_reference == manifest_reference,
            "manifest ref changed",
        )
        _require(
            payload.membership_digest == manifest["membership_digest"],
            "membership digest changed",
        )
        loaded = await store.get(
            _decode(resources.deps, payload.manifest_reference)
        )
        _require(loaded == manifest, "completion manifest changed")
        release_counts = [
            {"state": item.state.value, "count": item.count}
            for item in payload.release_terminal_state_counts
        ]
        _require(
            release_counts
            == [
                {"state": "succeeded", "count": MEMBER_COUNT},
                {"state": "failed", "count": 0},
                {"state": "cancelled", "count": 0},
            ],
            "completion release facts changed",
        )
        outcomes = await resources.bridge.call(
            "platform_status", partial(_stage_outcomes, engine)
        )
        consumed = []
        for outcome in outcomes:
            _require(
                outcome["state"] == StageExecutionState.SUCCEEDED.value,
                f"{outcome['work_key']} did not succeed",
            )
            reference = outcome["output_reference"]
            if not isinstance(reference, str):
                raise TypeError("stage output ref is absent")
            artifact = await store.get(_decode(resources.deps, reference))
            index = str(outcome["work_key"]).removeprefix("work-")
            _require(
                artifact == resources.stage_artifacts[f"input:{index}"],
                "consumed stage artifact changed",
            )
            consumed.append({**outcome, "artifact": artifact})
        aggregate: Mapping[str, object] = {
            "schema": AGGREGATE_SCHEMA,
            "run_key": str(payload.run_key),
            "manifest_reference": payload.manifest_reference,
            "membership_digest": payload.membership_digest,
            "release_terminal_state_counts": release_counts,
            "consumed": consumed,
        }
        reference, _ = await store.put(AGGREGATE_SCHEMA, aggregate)
        await store.bind("qualification-aggregate", reference)
        _require(await store.get(reference) == aggregate, "aggregate changed")
        _require(
            await store.resolve("qualification-aggregate") == reference,
            "aggregate resolution failed",
        )
        return _encode(reference)

    declared = PipelineDefinition(
        key=PipelineKey(expected_pipeline_key),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"cross-package-stage-{suffix}",
                workflow=stage,
                args_for=lambda payload: (payload.input_reference,),
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name=f"cross-package-completion-{suffix}",
            workflow=completion,
            args_for=lambda payload: (payload,),
        ),
    )
    return wrap_pipeline_workflows(declared)


def _validate_database_url(value: str) -> URL:
    url = make_url(value)
    if url.get_backend_name() != "postgresql":
        raise ValueError("qualification requires PostgreSQL")
    if {"dbname", "service", "servicefile"}.intersection(
        key.casefold() for key in url.query
    ):
        raise ValueError("database identity query overrides are not allowed")
    if url.database is None or not url.database.endswith("_test"):
        raise ValueError("qualification database name must end in '_test'")
    return url


def _reset_database(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS dbos CASCADE"))
        connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("CREATE EXTENSION pgcrypto"))


def _result[T](future: Future[T]) -> T:
    return future.result(timeout=WATCHDOG_SECONDS)


def _collect_cleanup(
    errors: list[BaseException], function: Callable[[], object]
) -> None:
    try:
        function()
    except BaseException as error:  # noqa: BLE001 -- cleanup ledger
        errors.append(error)


def _stage_workflow_ids(engine: Engine) -> tuple[str, ...]:
    schema = StagingSchema()
    with engine.connect() as connection:
        return tuple(
            connection.execute(
                select(schema.stage_attempts.c.workflow_id).order_by(
                    schema.stage_attempts.c.stage_attempt_id
                )
            ).scalars()
        )


def _live_run(  # noqa: PLR0915 -- explicit end-to-end qualification
    engine: Engine, url: URL, deps: _Dependencies
) -> dict[str, object]:
    with TemporaryDirectory(prefix="dr-platform-cross-package-") as temp:
        root = Path(temp)
        sqlite_path = root / "objects.sqlite3"
        records = root / "execution-records"
        records.mkdir()
        bridge = _Bridge()
        resources = _Resources(deps, sqlite_path, bridge)
        registration = None
        initialized = False
        launched = False
        result_futures: list[Future[Any]] = []
        cleanup_errors: list[BaseException] = []
        try:
            resources.executor = deps.exec.ProcessExecutor(
                runtime=deps.exec.IsolatedHostPythonRuntime(
                    Path(sys.executable)
                ),
                run_store=deps.exec.DirectoryRunStore(root=records),
            )
            suffix = uuid4().hex[:8]
            members = tuple(
                RunMemberInput(
                    ordinal=index,
                    work=WorkInput(
                        work_key=f"work-{index}",
                        input_reference=f"input:{index}",
                        labels={},
                    ),
                )
                for index in range(MEMBER_COUNT)
            )
            digest = compute_run_membership_digest(
                members, expected_member_count=MEMBER_COUNT
            )
            manifest = _manifest(members, digest)
            manifest_reference = _encode(
                deps.store.ObjectReference.for_record(
                    MANIFEST_SCHEMA, manifest
                )
            )
            pipeline = _pipeline(
                resources, engine, manifest, manifest_reference, suffix
            )

            @DBOS.workflow(name=f"cross_package_close_{suffix}")
            async def close_store() -> None:
                await resources.close()

            @DBOS.workflow(name=f"cross_package_load_{suffix}")
            async def load_object(reference: str) -> object:
                store = await resources.object_store()
                return await store.get(_decode(deps, reference))

            registry = PipelineRegistry()
            registry.register(pipeline)
            Queue(pipeline.stages[0].queue_name, concurrency=MEMBER_COUNT)
            run_completion = pipeline.run_completion
            if run_completion is None:
                raise RuntimeError("completion disappeared")
            Queue(run_completion.queue_name, concurrency=1)
            now = datetime.now(UTC)
            set_stage_capacity(
                pipeline=pipeline.identity,
                stage_key=StageKey("execute"),
                capacity=MEMBER_COUNT,
                engine=engine,
                clock=lambda: now,
            )
            receipt = submit(
                campaign_key=CAMPAIGN_KEY,
                run_key=RUN_KEY,
                pipeline=pipeline.identity,
                execution_config_reference=EXECUTION_CONFIG_REFERENCE,
                declaration=RunRegistrationDeclaration(
                    MEMBER_COUNT, manifest_reference, digest
                ),
                members=members,
                registry=registry,
                engine=engine,
                clock=lambda: now,
            )
            _require(
                receipt.registered_member_count == MEMBER_COUNT
                and receipt.membership_digest == digest,
                "registration receipt changed",
            )
            resources.registered_member_count = receipt.registered_member_count
            rendered = url.render_as_string(hide_password=False)
            config = PlatformDbosConfig(
                database_url=rendered, system_database_url=rendered
            )
            initialize_dbos_runtime(config, app_name=f"drp-cross-{suffix}")
            initialized = True
            registration = register_scheduled_dispatcher(
                config=config,
                engine=engine,
                registry=registry,
                batch_size=MEMBER_COUNT,
                barrier_batch_size=1,
            )
            DBOS.launch()
            launched = True
            DBOS.set_latest_application_version(DBOS.application_version)
            registration.workflow(now, now)
            stage_ids = _stage_workflow_ids(engine)
            _require(len(stage_ids) == MEMBER_COUNT, "admission count changed")
            stage_futures = tuple(
                bridge.submit(
                    "dbos_result",
                    partial(
                        registration.client.retrieve_workflow(
                            workflow_id
                        ).get_result,
                        polling_interval_sec=0.01,
                    ),
                )
                for workflow_id in stage_ids
            )
            result_futures.extend(stage_futures)
            stage_results = tuple(_result(future) for future in stage_futures)
            outcomes = _stage_outcomes(engine)
            _require(
                all(
                    item["state"] == StageExecutionState.SUCCEEDED.value
                    for item in outcomes
                ),
                "stage terminal states changed",
            )
            _require(
                {item["output_reference"] for item in outcomes}
                == set(stage_results),
                "stage references changed",
            )
            registration.barrier_workflow(now, now)
            completion = inspect_run_completion(RUN_KEY, engine=engine)
            completion_future = bridge.submit(
                "dbos_result",
                partial(
                    registration.client.retrieve_workflow(
                        completion.workflow_id
                    ).get_result,
                    polling_interval_sec=0.01,
                ),
            )
            result_futures.append(completion_future)
            completion_result = _result(completion_future)
            recorded = inspect_run_completion(RUN_KEY, engine=engine)
            _require(
                isinstance(completion_result, str)
                and recorded.state is RunCompletionExecutionState.SUCCEEDED
                and recorded.output_reference == completion_result,
                "completion did not record its aggregate reference",
            )
            _require(
                recorded.error_summary is None
                and recorded.terminal_at is not None,
                "completion terminal facts changed",
            )
            release_counts = [
                {"state": "succeeded", "count": MEMBER_COUNT},
                {"state": "failed", "count": 0},
                {"state": "cancelled", "count": 0},
            ]
            expected_aggregate: Mapping[str, object] = {
                "schema": AGGREGATE_SCHEMA,
                "run_key": RUN_KEY,
                "manifest_reference": manifest_reference,
                "membership_digest": digest,
                "release_terminal_state_counts": release_counts,
                "consumed": [
                    {
                        **outcome,
                        "artifact": resources.stage_artifacts[
                            f"input:{str(outcome['work_key']).removeprefix('work-')}"
                        ],
                    }
                    for outcome in outcomes
                ],
            }
            load_handle = DBOS.start_workflow(
                cast("Callable[[str], object]", load_object), completion_result
            )
            load_future = bridge.submit(
                "dbos_result",
                partial(load_handle.get_result, polling_interval_sec=0.01),
            )
            result_futures.append(load_future)
            loaded_aggregate = _result(load_future)
            _require(
                loaded_aggregate == expected_aggregate,
                "stored aggregate does not match independent expectation",
            )
            _require(
                len(resources.provider.requests) == MEMBER_COUNT,
                "provider count",
            )
            _require(
                len(tuple(records.iterdir())) == MEMBER_COUNT,
                "exec record count",
            )
            evidence = {
                "stage_workflow_count": len(stage_ids),
                "stage_results": stage_results,
                "stage_terminal_outcomes": outcomes,
                "completion_succeeded": True,
                "completion_output_reference": completion_result,
                "manifest_reference": manifest_reference,
                "membership_digest": digest,
                "provider_calls": len(resources.provider.requests),
                "exec_calls": bridge.calls.get("exec", 0),
                "aggregate_exact": True,
                "aggregate": loaded_aggregate,
            }
        finally:
            primary_error = sys.exception()
            for future in result_futures:
                _collect_cleanup(cleanup_errors, partial(_result, future))
            if launched and resources.backend is not None:
                try:
                    close_handle = DBOS.start_workflow(
                        cast("Callable[[], None]", close_store)
                    )
                    _result(
                        bridge.submit(
                            "dbos_result",
                            partial(
                                close_handle.get_result,
                                polling_interval_sec=0.01,
                            ),
                        )
                    )
                except BaseException as error:  # noqa: BLE001 -- cleanup ledger
                    cleanup_errors.append(error)
            if registration is not None:
                _collect_cleanup(cleanup_errors, registration.close)
            if initialized:
                _collect_cleanup(
                    cleanup_errors,
                    partial(DBOS.destroy, destroy_registry=True),
                )
            _collect_cleanup(cleanup_errors, bridge.shutdown)
            if cleanup_errors and primary_error is not None:
                primary_error.add_note(
                    f"qualification cleanup errors: {cleanup_errors!r}"
                )
            if cleanup_errors and primary_error is None:
                raise BaseExceptionGroup(
                    "qualification cleanup failed", cleanup_errors
                )
        loops = tuple(sorted(set(resources.loop_ids)))
        resource = {
            "workflow_loop_ids": loops,
            "open_loop_id": resources.open_loop,
            "close_loop_id": resources.close_loop,
            "one_loop_reused": len(loops) == 1
            and resources.open_loop == resources.close_loop == loops[0],
            "entered_stages": tuple(sorted(resources.entered)),
            "peak_active_stages": resources.peak_active,
        }
        bridge_evidence = {
            "application_owned_executor": True,
            "provider_invoke_lock_owned_by_application": True,
            "calls": dict(sorted(bridge.calls.items())),
            "shutdown": bridge.shutdown_complete,
        }
        _require(resource["one_loop_reused"], "store loop affinity failed")
        _require(resource["peak_active_stages"] == MEMBER_COUNT, "no overlap")
        _require(bridge_evidence["shutdown"], "bridge shutdown failed")
        _require(
            sqlite_path.exists() and records.exists(), "temp paths vanished"
        )
        owned_root = str(root)
    _require(not Path(owned_root).exists(), "temp root was not cleaned")
    return {
        "assertions": evidence,
        "resource": resource,
        "bridge": bridge_evidence,
        "temporary_paths": {"owned_root": owned_root, "cleaned": True},
    }


def _run(database_url: str) -> dict[str, object]:
    repository = Path(__file__).resolve().parents[1]
    platform_module_file = _platform_source(repository)
    platform_start = _repo_state(repository)
    _require(platform_start["clean"], "dr-platform source tree is dirty")
    packages = _preflight_packages()
    deps = _Dependencies()
    url = _validate_database_url(database_url)
    engine = create_engine(url)
    try:
        _reset_database(engine)
        upgrade_platform_schema(url.render_as_string(hide_password=False))
        result = _live_run(engine, url, deps)
    finally:
        engine.dispose()
    platform_end = _repo_state(repository)
    _require(platform_end["clean"], "dr-platform source tree became dirty")
    return {
        "schema_version": 1,
        "qualified": True,
        "run_at": datetime.now(UTC).isoformat(),
        "platform_git": {
            "commit": platform_start["commit"],
            "module_file": platform_module_file,
            "status_at_start": platform_start["status"],
            "status_before_result": platform_end["status"],
            "clean": True,
        },
        "packages": packages,
        **result,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Qualify cross-package async workflow resources."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DR_PLATFORM_TEST_DATABASE_URL",
            "postgresql+psycopg:///dr_platform_test",
        ),
    )
    parser.add_argument(
        "--reset-test-database",
        action="store_true",
        help="Required acknowledgement that the named *_test database resets.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.reset_test_database:
        raise SystemExit("--reset-test-database is required")
    result = _run(args.database_url)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
