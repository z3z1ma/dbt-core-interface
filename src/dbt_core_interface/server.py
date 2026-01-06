# pyright: reportCallInDefaultInitializer=false, reportUnknownMemberType=false, reportAny=false
"""dbt-core-interface server API.

Note that the interface is coupled to https://github.com/datacoves/datacoves-power-user
and thus should not change in a backwards incompatible way. Create a versioned API or a
v2 server.py if we ever want/need to change the interface.
"""

import json
import logging
import os
import time
import typing as t
import uuid
from collections.abc import AsyncGenerator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from fastapi import (
    BackgroundTasks,
    Body,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from dbt_core_interface.container import DbtProjectContainer
from dbt_core_interface.project import (
    DbtConfiguration,
    DbtProject,
    ExecutionResult,
)
from dbt_core_interface.quality import (
    AlertChannel,
    ConsoleAlertChannel,
    CustomSqlCheck,
    DuplicateCheck,
    LogAlertChannel,
    NullPercentageCheck,
    QualityCheck,
    QualityCheckType,
    RowCountCheck,
    Severity,
    ValueRangeCheck,
    WebhookAlertChannel,
)
from dbt_core_interface.watcher import DbtProjectWatcher

__all__ = ["app", "main"]

logger = logging.getLogger(__name__)


class ServerErrorCode(int, Enum):
    """Enum for server error codes."""

    FailedToReachServer = -1
    CompileSqlFailure = 1
    ExecuteSqlFailure = 2
    ProjectParseFailure = 3
    ProjectNotRegistered = 4
    ProjectHeaderNotSupplied = 5
    MissingRequiredParams = 6
    StateInjectionFailure = 7


class ServerError(BaseModel):
    """Represents an error response from the server."""

    code: ServerErrorCode
    message: str
    data: dict[str, t.Any]


class ServerErrorContainer(BaseModel):
    """Container for server errors, used in HTTP responses."""

    error: ServerError


class ServerRunResult(BaseModel):
    """Represents the result of a SQL execution."""

    column_names: list[str]
    rows: list[list[t.Any]]
    raw_code: str
    executed_code: str


class ServerCompileResult(BaseModel):
    """Container for SQL compilation results."""

    result: str


class ServerRegisterResult(BaseModel):
    """Represents the result of a project registration."""

    added: str
    projects: list[str]


class ServerUnregisterResult(BaseModel):
    """Represents the result of a project unregistration."""

    removed: str
    projects: list[str]


class ServerResetResult(BaseModel):
    """Container for project reset results."""

    result: str


DISABLE_AUTOLOAD = "DCI_SERVER_DISABLE_AUTOLOAD"

STATE_FILE = "DCI_SERVER_STATE_FILE"
DEFAULT_STATE_FILE = Path.home() / ".dbt_core_interface_state.json"


def _get_state_file_path() -> Path:
    """Determine the path for the state file."""
    return Path(os.getenv(STATE_FILE, str(DEFAULT_STATE_FILE))).expanduser().resolve()


def _load_saved_state(runners: DbtProjectContainer) -> None:
    """On startup, re-register projects saved in the state file, unless disabled."""
    if os.getenv(DISABLE_AUTOLOAD, "").lower() in ("1", "true", "yes"):
        logging.info("Auto-register on startup disabled via %s", DISABLE_AUTOLOAD)
        return
    path = _get_state_file_path()
    if not path.exists():
        return
    try:
        entries = t.cast(list[dict[str, t.Any]], json.loads(path.read_text(encoding="utf-8")))
        if not isinstance(entries, list):  # pyright: ignore[reportUnnecessaryIsInstance]
            logging.warning("State file %s content invalid: expected list", path)
            return
        for entry in entries:
            project_name = t.cast(str, entry.get("project"))
            if not project_name:
                continue
            kwargs: dict[str, t.Any] = {
                "target": entry.get("target"),
                "profiles_dir": entry.get("profiles_dir"),
                "project_dir": entry.get("project_dir"),
                "threads": entry.get("threads", 1),
                "vars": entry.get("vars", {}),
            }
            try:
                _ = runners.create_project(**{k: v for k, v in kwargs.items() if v is not None})
                logging.info("Restored project %s from state", project_name)
            except Exception as e:
                logging.error("Failed to restore project %s: %s", project_name, e)
    except Exception as e:
        logging.error("Error loading state file %s: %s", path, e)


def _save_state(runners: DbtProjectContainer) -> None:
    """Persist all registered projects to disk."""
    state: list[dict[str, t.Any]] = []
    for path in runners.registered_projects():
        proj = runners.get_project(path)
        if not proj:
            continue
        cfg = proj.args
        state.append(
            {
                "project": proj.project_name,
                "project_dir": cfg.project_dir,
                "profiles_dir": cfg.profiles_dir,
                "target": cfg.target,
                "threads": cfg.threads,
                "vars": cfg.vars,
            }
        )
    path = _get_state_file_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        logging.error("Failed to save state file %s: %s", path, e)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, t.Any]:
    """Lifespan context manager for FastAPI app."""
    _load_saved_state(container := _get_container())
    app.state._p_references = {}
    for project in container:
        _ = DbtProjectWatcher(project, start=True)
        app.state._p_references[project] = True
    try:
        yield
    finally:
        _save_state(container)
        _ = DbtProjectWatcher.stop_all()
        app.state._p_references.clear()


app = FastAPI(
    title="dbt-core-interface API",
    version="1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def add_execution_time(
    request: Request, call_next: t.Callable[[Request], Awaitable[Response]]
) -> Response:
    """Middleware to measure execution time of requests."""
    start_time = time.time()
    response: Response = await call_next(request)
    exec_time = time.time() - start_time
    response.headers["X-dbt-Exec-Time"] = f"{exec_time:.3f}"
    return response


def _get_container() -> DbtProjectContainer:
    """Get the DbtProjectContainer instance."""
    return DbtProjectContainer()


def _get_runner(
    project_dir: str = Query(None, description="Project directory to use for the request."),
    runners: DbtProjectContainer = Depends(_get_container),
) -> DbtProject:
    """Get the DbtProject runner based on the X-dbt-Project header."""
    runner = None
    if project_dir:
        runner = runners.find_project_in_tree(project_dir)
    if not runner:
        runner = runners.get_default_project()
    if not runner:
        raise HTTPException(
            status_code=404,
            detail=ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=("Project is not registered. Make a POST request to /register first."),
                    data={"registered_projects": runners.registered_projects()},
                )
            ).model_dump(),
        )
    return runner


def _format_run_result(res: ExecutionResult) -> ServerRunResult:
    """Convert ExecutionResult to ServerRunResult."""
    table = res.table
    rows = [list(row) for row in table.rows]  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
    return ServerRunResult(
        column_names=list(table.column_names),  # pyright: ignore[reportUnknownArgumentType]
        rows=rows,  # pyright: ignore[reportUnknownArgumentType]
        raw_code=res.raw_code,
        executed_code=res.compiled_code,
    )


def _create_error_response(
    code: ServerErrorCode,
    message: str,
    data: dict[str, t.Any] | None = None,
) -> ServerErrorContainer:
    """Create a standardized error response.

    Args:
        code: The server error code.
        message: The error message.
        data: Optional additional error data.

    Returns:
        A ServerErrorContainer with the provided error details.

    """
    return ServerErrorContainer(
        error=ServerError(
            code=code,
            message=message,
            data=data or getattr(Exception(message), "__dict__", {}),
        )
    )


@app.post("/api/v1/run")
def run_sql(
    response: Response,
    raw_sql: str = Body(..., media_type="text/plain"),
    limit: int = Query(200, ge=1, le=1000, description="Limit the number of rows returned"),
    model_path: str | None = Query(None),
    runner: DbtProject = Depends(_get_runner),
) -> ServerRunResult | ServerErrorContainer:
    """Run raw SQL code against the registered dbt project."""
    _ = response
    if model_path:
        node = runner.get_node_by_path(model_path)
        if not node:
            response.status_code = 404
            return _create_error_response(
                ServerErrorCode.MissingRequiredParams,
                f"Model path not found in dbt manifest: {model_path}",
            )
        orig_raw_sql = node.raw_code
        try:
            node.raw_code = raw_sql
            comp_res = runner.compile_node(node)
        finally:
            node.raw_code = orig_raw_sql
    else:
        comp_res = runner.compile_sql(raw_sql)
    try:
        model_context = runner.generate_runtime_model_context(comp_res.node)
        query = runner.adapter.execute_macro(
            macro_name="get_show_sql",
            macro_resolver=runner.manifest,
            context_override=model_context,
            kwargs={
                "compiled_code": model_context["compiled_code"],
                "sql_header": model_context["config"].get("sql_header"),
                "limit": limit,
            },
        )
        exec_res = runner.execute_sql(t.cast(str, query), compile=False)  # pyright: ignore[reportInvalidCast]
    except Exception as e:
        response.status_code = 500
        return _create_error_response(
            ServerErrorCode.ExecuteSqlFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    return _format_run_result(exec_res)


@app.post("/api/v1/compile")
def compile_sql(
    response: Response,
    raw_sql: str = Body(..., media_type="text/plain"),
    model_path: str | None = Query(None),
    runner: DbtProject = Depends(_get_runner),
) -> ServerCompileResult | ServerErrorContainer:
    """Compile raw SQL code without executing it."""
    try:
        if model_path:
            node = runner.get_node_by_path(model_path)
            if not node:
                response.status_code = 404
                return _create_error_response(
                    ServerErrorCode.MissingRequiredParams,
                    f"Model path not found: {model_path}",
                )
            orig_raw_sql = node.raw_code
            try:
                node.raw_code = raw_sql
                comp_res = runner.compile_node(node)
            finally:
                node.raw_code = orig_raw_sql
            result_sql = comp_res.compiled_code
        else:
            comp_res = runner.compile_sql(raw_sql)
            result_sql = comp_res.compiled_code
    except Exception as e:
        response.status_code = 400
        return _create_error_response(
            ServerErrorCode.CompileSqlFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    return ServerCompileResult(result=result_sql)


@app.get("/api/v1/register")
def register_project(
    response: Response,
    request: Request,
    project_dir: str = Query(...),
    profiles_dir: str | None = Query(None),
    profile: str | None = Query(None),
    target: str | None = Query(None),
    vars: dict[str, t.Any] | None = Body(None),
    runners: DbtProjectContainer = Depends(_get_container),
) -> ServerRegisterResult | ServerErrorContainer:
    """Register a new dbt project with the server."""
    project_path = Path(project_dir).expanduser().resolve()
    maybe_dbt_project = runners.get_project(project_path)
    if maybe_dbt_project:
        args = {}
        if profile:
            args["profile"] = profile
        if target:
            args["target"] = target
        if profiles_dir:
            args["profiles_dir"] = profiles_dir
        if vars:
            args["vars"] = vars
        if args:
            maybe_dbt_project.args = args
        return ServerRegisterResult(
            added=project_path.name, projects=list(map(str, runners.registered_projects()))
        )
    try:
        dbt_project = runners.create_project(
            profile=profile,
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            vars=vars or {},
        )
        _save_state(runners)
        watcher = DbtProjectWatcher(dbt_project)
        watcher.start()
        request.app.state._p_references[dbt_project] = True
    except Exception as e:
        response.status_code = 400
        return _create_error_response(
            ServerErrorCode.ProjectParseFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    return ServerRegisterResult(
        added=project_path.name, projects=list(map(str, runners.registered_projects()))
    )


@app.delete("/api/v1/register")
def unregister_project(
    response: Response,
    request: Request,
    project_dir: str = Query(..., description="Project directory to unregister."),
    runners: DbtProjectContainer = Depends(_get_container),
) -> ServerUnregisterResult | ServerErrorContainer:
    """Remove a registered dbt project from the server."""
    if (
        not project_dir
        or (project_path := Path(project_dir).expanduser().resolve())
        not in runners.registered_projects()
    ):
        response.status_code = 404
        return _create_error_response(
            ServerErrorCode.ProjectNotRegistered,
            "Project not registered; register first.",
            {"registered_projects": list(map(str, runners.registered_projects()))},
        )
    dbt_project = runners.drop_project(project_path)
    _save_state(runners)
    if dbt_project:
        DbtProjectWatcher.stop_path(dbt_project.project_root)
        _ = request.app.state._p_references.pop(dbt_project, None)
    return ServerUnregisterResult(
        removed=project_path.name, projects=list(map(str, runners.registered_projects()))
    )


@app.get("/api/v1/parse")
def parse_project(
    response: Response,
    background_tasks: BackgroundTasks,
    target: str | None = Query(None),
    reset: bool = Query(False),
    write_manifest: bool = Query(False),
    runner: DbtProject = Depends(_get_runner),
) -> ServerResetResult | ServerErrorContainer:
    """Re-parse the dbt project configuration and manifest."""
    sync = False
    if target is not None and target != runner.runtime_config.target_name:
        params = asdict(runner.args)
        params["target"] = target
        runner.args = DbtConfiguration(**params)
        sync = True
    try:
        if sync or reset:
            runner.parse_project(write_manifest=True, reparse_configuration=reset)
        else:
            background_tasks.add_task(runner.parse_project, write_manifest=write_manifest)
    except Exception as e:
        response.status_code = 500
        return _create_error_response(
            ServerErrorCode.ProjectParseFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    return ServerResetResult(result="Project re-parsed successfully.")


@app.get("/health")  # legacy extension support
@app.get("/api/v1/status")
def status(runner: DbtProject = Depends(_get_runner)) -> dict[str, t.Any]:
    """Health check endpoint to verify server status."""
    return {
        "result": {
            "status": "ready",
            "project_name": runner.project_name,
            "target_name": runner.runtime_config.target_name,
            "logs": str(runner.log_path),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "id": str(uuid.uuid4()),
        "dbt-interface-server": __name__,
    }


@app.get("/api/v1/heartbeat")
def heartbeat() -> dict[str, t.Any]:
    """Heartbeat endpoint to check server availability."""
    return {"result": {"status": "ready"}}


class ServerLintResult(BaseModel):
    """Container for SQL linting results."""

    result: list[dict[str, t.Any]]


class ServerFormatResult(BaseModel):
    """Container for SQL formatting results."""

    result: bool
    sql: str | None


@app.post("/lint")  # legacy extension support
@app.get("/api/v1/lint")
@app.post("/api/v1/lint")
def lint_sql(
    request: Request,
    response: Response,
    sql_path: str | None = Query(None, description="Path to the SQL file to lint."),
    extra_config_path: str | None = Query(None, description="Path to extra SQLFluff config file."),
    raw_sql: str | None = Body(
        None, media_type="text/plain", description="Raw SQL string to lint."
    ),
    runner: DbtProject = Depends(_get_runner),
) -> ServerLintResult | ServerErrorContainer:
    """Lint SQL string or file via SQLFluff."""
    records = []
    try:
        if sql_path:
            records = runner.lint(
                sql=Path(sql_path),
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
        else:
            if not raw_sql:
                response.status_code = 400
                return _create_error_response(
                    ServerErrorCode.MissingRequiredParams,
                    "No SQL provided. Provide sql_path or SQL body.",
                )
            records = runner.lint(
                sql=raw_sql,
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
    except Exception as e:
        response.status_code = 500
        return _create_error_response(ServerErrorCode.CompileSqlFailure, str(e), {})
    if request.url.path.endswith("/lint"):
        return ServerLintResult(result=records[0]["violations"] if len(records) > 0 else [])
    return ServerLintResult(result=records)  # pyright: ignore[reportArgumentType]


@app.post("/format")  # legacy extension support
@app.get("/api/v1/format")
@app.post("/api/v1/format")
def format_sql(
    response: Response,
    sql_path: str | None = Query(None, description="Path to the SQL file to format."),
    extra_config_path: str | None = Query(None, description="Path to extra SQLFluff config file."),
    raw_sql: str | None = Body(
        None, media_type="text/plain", description="Raw SQL string to format."
    ),
    runner: DbtProject = Depends(_get_runner),
) -> ServerFormatResult | ServerErrorContainer:
    """Format SQL string or file via SQLFluff."""
    try:
        if sql_path:
            success, formatted = runner.format(
                sql=Path(sql_path),
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
        else:
            if not raw_sql:
                response.status_code = 400
                return _create_error_response(
                    ServerErrorCode.MissingRequiredParams,
                    "No SQL provided. Provide sql_path or SQL body.",
                )
            success, formatted = runner.format(
                sql=raw_sql,
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
    except Exception as e:
        response.status_code = 500
        return _create_error_response(ServerErrorCode.CompileSqlFailure, str(e), {})
    return ServerFormatResult(result=success, sql=formatted)


@app.post("/api/v1/command")
def run_dbt_command(
    response: Response,
    cmd: str = Query(..., description="The dbt command to run, e.g. 'run', 'test', 'build'"),
    args: list[str] | None = Body(None, description="List of positional args for the command"),
    kwargs: dict[str, t.Any] | None = Body(None, description="Keyword args for the command"),
    runner: DbtProject = Depends(_get_runner),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Run an arbitrary dbt CLI command on the project."""
    import agate  # pyright: ignore[reportMissingTypeStubs]
    from dbt.artifacts.schemas.base import VersionedSchema
    from dbt.contracts.graph.manifest import Manifest

    try:
        result = runner.command(cmd, *(args or []), **(kwargs or {}))
    except Exception as e:
        response.status_code = 500
        return _create_error_response(
            ServerErrorCode.ExecuteSqlFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    if isinstance(result.result, Manifest):
        result.result = result.result.writable_manifest()
    return jsonable_encoder(
        result,
        custom_encoder={
            agate.Table: lambda tbl: list(map(dict, tbl.rows)),
            VersionedSchema: lambda obj: obj.to_dict(),
        },
    )


@app.get("/api/v1/write-manifest")
def write_manifest(
    response: Response,
    target_path: str | None = Query(None, description="Optional custom path for manifest.json"),
    runner: DbtProject = Depends(_get_runner),
) -> ServerResetResult | ServerErrorContainer:
    """Write the current manifest out to disk."""
    try:
        runner.write_manifest(target_path)
    except Exception as e:
        response.status_code = 500
        return _create_error_response(
            ServerErrorCode.ProjectParseFailure,
            str(e),
            getattr(e, "__dict__", {}),
        )
    return ServerResetResult(result="Manifest written.")


@app.get("/api/v1/projects")
def list_projects(runners: DbtProjectContainer = Depends(_get_container)) -> dict[str, t.Any]:
    """List all registered dbt projects."""
    return {
        "projects": [
            {
                "name": runner.project_name,
                "path": str(runner.project_root),
                "target": runner.runtime_config.target_name,
                "profiles_dir": runner.args.profiles_dir,
                "threads": runner.args.threads,
                "vars": runner.args.vars,
            }
            for runner in runners
        ]
    }


@app.get("/api/v1/state")
def inject_state(
    runner: DbtProject = Depends(_get_runner),
    directory: str = Query(..., description="Directory containing the deferral state files."),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Enable dbt deferral by injecting the deferral state into the runner."""
    try:
        runner.inject_deferred_state(directory)
        runner.parse_project(write_manifest=True)
        return {"success": True}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.StateInjectionFailure,
                    message=str(e),
                    data=getattr(e, "__dict__", {}),
                )
            ).model_dump(),
        ) from e


@app.delete("/api/v1/state")
def clear_state(runner: DbtProject = Depends(_get_runner)) -> dict[str, t.Any]:
    """Clear the deferral state in the runner."""
    try:
        runner.clear_deferred_state()
        runner.parse_project(write_manifest=True)
        return {"success": True}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.StateInjectionFailure,
                    message=str(e),
                    data=getattr(e, "__dict__", {}),
                )
            ).model_dump(),
        ) from e


class QualityCheckConfig(BaseModel):
    """Configuration for creating a quality check."""

    name: str
    check_type: QualityCheckType
    model_name: str
    description: str = ""
    severity: Severity = Severity.WARNING
    enabled: bool = True
    config: dict[str, t.Any] = {}


class QualityCheckResultResponse(BaseModel):
    """Response model for quality check results."""

    results: list[dict[str, t.Any]]


class AlertChannelConfig(BaseModel):
    """Configuration for adding an alert channel."""

    channel_type: str
    config: dict[str, t.Any] = {}


@app.post("/api/v1/quality/checks")
def add_quality_check(
    response: Response,
    check_config: QualityCheckConfig,
    runner: DbtProject = Depends(_get_runner),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Add a quality check to a model."""
    try:
        monitor = runner.quality_monitor

        check: QualityCheck
        if check_config.check_type == QualityCheckType.ROW_COUNT:
            check = RowCountCheck(
                name=check_config.name,
                description=check_config.description,
                severity=check_config.severity,
                enabled=check_config.enabled,
                min_rows=check_config.config.get("min_rows"),
                max_rows=check_config.config.get("max_rows"),
            )
        elif check_config.check_type == QualityCheckType.NULL_PERCENTAGE:
            check = NullPercentageCheck(
                name=check_config.name,
                description=check_config.description,
                severity=check_config.severity,
                enabled=check_config.enabled,
                column_name=check_config.config.get("column_name", ""),
                max_null_percentage=check_config.config.get("max_null_percentage", 0.0),
            )
        elif check_config.check_type == QualityCheckType.DUPLICATE:
            check = DuplicateCheck(
                name=check_config.name,
                description=check_config.description,
                severity=check_config.severity,
                enabled=check_config.enabled,
                columns=check_config.config.get("columns", []),
                max_duplicate_percentage=check_config.config.get("max_duplicate_percentage", 0.0),
            )
        elif check_config.check_type == QualityCheckType.VALUE_RANGE:
            check = ValueRangeCheck(
                name=check_config.name,
                description=check_config.description,
                severity=check_config.severity,
                enabled=check_config.enabled,
                column_name=check_config.config.get("column_name", ""),
                min_value=check_config.config.get("min_value"),
                max_value=check_config.config.get("max_value"),
            )
        elif check_config.check_type == QualityCheckType.CUSTOM_SQL:
            check = CustomSqlCheck(
                name=check_config.name,
                description=check_config.description,
                severity=check_config.severity,
                enabled=check_config.enabled,
                sql_template=check_config.config.get("sql_template", ""),
                expect_true=check_config.config.get("expect_true", True),
            )
        else:
            response.status_code = 400
            return ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.MissingRequiredParams,
                    message=f"Unknown check type: {check_config.check_type}",
                    data={},
                )
            )

        monitor.add_check(check_config.model_name, check)
        return {"success": True, "check_name": check_config.name, "model": check_config.model_name}
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


@app.delete("/api/v1/quality/checks")
def remove_quality_check(
    response: Response,
    model_name: str = Query(..., description="Name of the model"),
    check_name: str = Query(..., description="Name of the check to remove"),
    runner: DbtProject = Depends(_get_runner),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Remove a quality check from a model."""
    try:
        monitor = runner.quality_monitor
        removed = monitor.remove_check(model_name, check_name)
        if not removed:
            response.status_code = 404
            return ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=f"Check '{check_name}' not found for model '{model_name}'",
                    data={},
                )
            )
        return {"success": True, "check_name": check_name, "model": model_name}
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


@app.get("/api/v1/quality/checks")
def list_quality_checks(
    model_name: str | None = Query(None, description="Filter checks by model name"),
    runner: DbtProject = Depends(_get_runner),
) -> dict[str, t.Any]:
    """List all quality checks, optionally filtered by model."""
    monitor = runner.quality_monitor
    checks = monitor.get_checks(model_name)
    return {"checks": checks}


@app.post("/api/v1/quality/run")
def run_quality_checks(
    response: Response,
    model_name: str | None = Query(None, description="Run checks for a specific model"),
    check_name: str | None = Query(None, description="Run a specific check by name"),
    only_enabled: bool = Query(True, description="Only run enabled checks"),
    runner: DbtProject = Depends(_get_runner),
) -> QualityCheckResultResponse | ServerErrorContainer:
    """Run quality checks for a model or all models."""
    try:
        monitor = runner.quality_monitor

        if check_name and not model_name:
            response.status_code = 400
            return ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.MissingRequiredParams,
                    message="model_name is required when check_name is specified",
                    data={},
                )
            )

        if check_name:
            result = monitor.run_check_by_name(model_name or "", check_name)
            results = [result.to_dict()] if result else []
        else:
            results = [r.to_dict() for r in monitor.run_checks(model_name, only_enabled)]

        return QualityCheckResultResponse(results=results)
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ExecuteSqlFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


@app.post("/api/v1/quality/alerts")
def add_alert_channel(
    response: Response,
    channel_config: AlertChannelConfig,
    runner: DbtProject = Depends(_get_runner),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Add an alert channel for quality failures."""
    try:
        monitor = runner.quality_monitor

        channel: AlertChannel
        if channel_config.channel_type == "webhook":
            channel = WebhookAlertChannel(
                url=channel_config.config.get("url", ""),
                timeout=channel_config.config.get("timeout", 5.0),
                headers=channel_config.config.get("headers"),
                verify_ssl=channel_config.config.get("verify_ssl", True),
            )
        elif channel_config.channel_type == "log":
            channel = LogAlertChannel()
        elif channel_config.channel_type == "console":
            channel = ConsoleAlertChannel()
        else:
            response.status_code = 400
            return ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.MissingRequiredParams,
                    message=f"Unknown channel type: {channel_config.channel_type}",
                    data={},
                )
            )

        monitor.add_alert_channel(channel)
        return {"success": True, "channel_type": channel_config.channel_type}
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


class TestSuggestionResult(BaseModel):
    """Container for test suggestion results."""

    model: str
    unique_id: str
    path: str
    suggestions: list[dict[str, t.Any]]


class ServerTestSuggestionResult(BaseModel):
    """Container for test suggestions response."""

    result: list[TestSuggestionResult]


class ServerTestYmlResult(BaseModel):
    """Container for generated test YAML."""

    result: str


@app.get("/api/v1/suggest-tests")
@app.post("/api/v1/suggest-tests")
def suggest_tests(
    response: Response,
    model_name: str | None = Query(None, description="Name of specific model to analyze"),
    model_path: str | None = Query(None, description="Path to model file"),
    learn: bool = Query(True, description="Whether to learn from existing project tests"),
    runner: DbtProject = Depends(_get_runner),
) -> ServerTestSuggestionResult | ServerErrorContainer:
    """Suggest tests for dbt models based on column patterns and project conventions."""
    try:
        suggestions = runner.suggest_tests(
            model_name=model_name,
            model_path=Path(model_path) if model_path else None,
            learn=learn,
        )
        return ServerTestSuggestionResult(
            result=[
                TestSuggestionResult(
                    model=s["model"],
                    unique_id=s["unique_id"],
                    path=s["path"],
                    suggestions=s["suggestions"],
                )
                for s in suggestions
            ]
        )
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


@app.get("/api/v1/generate-test-yml")
@app.post("/api/v1/generate-test-yml")
def generate_test_yml(
    response: Response,
    model_name: str | None = Query(None, description="Name of specific model"),
    model_path: str | None = Query(None, description="Path to model file"),
    learn: bool = Query(True, description="Whether to learn from existing project tests"),
    runner: DbtProject = Depends(_get_runner),
) -> ServerTestYmlResult | ServerErrorContainer:
    """Generate YAML schema file with suggested tests."""
    try:
        yml = runner.generate_test_yml(
            model_name=model_name,
            model_path=Path(model_path) if model_path else None,
            learn=learn,
        )
        return ServerTestYmlResult(result=yml)
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )


def main() -> None:
    """Entry point for running the FastAPI server via `python -m dbt_core_interface.server`."""
    import argparse
    import logging

    import uvicorn

    parser = argparse.ArgumentParser(description="Run the dbt-core-interface server.")
    _ = parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to. Defaults to 127.0.0.1",
    )
    _ = parser.add_argument(
        "--port",
        type=int,
        default=8581,
        help="Port to bind the server on. Defaults to 8581",
    )
    _ = parser.add_argument(
        "--log-level",
        default="info",
        help="Uvicorn log level (debug, info, warning, error, critical)",
    )
    args = parser.parse_args()

    if args.log_level.upper() == "DEBUG":
        log = logging.getLogger("dbt_core_interface")
        log.setLevel(logging.DEBUG)

    uvicorn.run(
        "dbt_core_interface.server:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
