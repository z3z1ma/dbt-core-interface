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
from collections.abc import Awaitable
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
    path = _get_state_file_path()
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
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        logging.error("Failed to save state file %s: %s", path, e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    _ = app
    _load_saved_state(container := get_container())
    for project in container:
        _ = DbtProjectWatcher(project, start=True)
    try:
        yield
    finally:
        _save_state(container)
        _ = DbtProjectWatcher.stop_all()


app = FastAPI(
    title="dbt-core-interface API",
    version="1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def add_exec_time(
    request: Request, call_next: t.Callable[[Request], Awaitable[Response]]
) -> Response:
    """Middleware to measure execution time of requests."""
    start = time.time()
    response: Response = await call_next(request)
    exec_time = time.time() - start
    response.headers["X-dbt-Exec-Time"] = f"{exec_time:.3f}"
    return response


def get_container() -> DbtProjectContainer:
    """Get the DbtProjectContainer instance."""
    return DbtProjectContainer()


def get_runner(
    project_dir: str = Query(None, description="Project directory to use for the request."),
    runners: DbtProjectContainer = Depends(get_container),
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


def format_run_result(res: ExecutionResult) -> ServerRunResult:
    """Convert ExecutionResult to ServerRunResult."""
    table = res.table
    rows = [list(row) for row in table.rows]  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
    return ServerRunResult(
        column_names=list(table.column_names),  # pyright: ignore[reportUnknownArgumentType]
        rows=rows,  # pyright: ignore[reportUnknownArgumentType]
        raw_code=res.raw_code,
        executed_code=res.compiled_code,
    )


@app.post("/api/v1/run")
def run_sql(
    response: Response,
    raw_sql: str = Body(..., media_type="text/plain"),
    limit: int = Query(200, ge=1, le=1000, description="Limit the number of rows returned"),
    path: str | None = Query(None),
    runner: DbtProject = Depends(get_runner),
) -> ServerRunResult | ServerErrorContainer:
    """Run raw SQL code against the registered dbt project."""
    _ = response
    if path:
        node = runner.get_node_by_path(path)
        if not node:
            response.status_code = 404
            return ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.MissingRequiredParams,
                    message=f"Model path not found in dbt manifest: {path}",
                    data={},
                )
            )
        orig_raw_sql = node.raw_code
        try:
            node.raw_code = raw_sql
            comp_res = runner.compile_node(node)
        finally:
            node.raw_code = orig_raw_sql
    else:
        comp_res = runner.compile_sql(raw_sql)
    raw_sql = comp_res.compiled_code
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
        exec_res = runner.execute_sql(t.cast(str, query), compile=path is None)  # pyright: ignore[reportInvalidCast]
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ExecuteSqlFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )
    return format_run_result(exec_res)


@app.post("/api/v1/compile")
def compile_sql(
    response: Response,
    raw_sql: str = Body(..., media_type="text/plain"),
    path: str | None = Query(None),
    runner: DbtProject = Depends(get_runner),
) -> ServerCompileResult | ServerErrorContainer:
    """Compile raw SQL code without executing it."""
    try:
        if path:
            node = runner.get_node_by_path(path)
            if not node:
                response.status_code = 404
                return ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.MissingRequiredParams,
                        message=f"Model path not found: {path}",
                        data={},
                    )
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
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.CompileSqlFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )
    return ServerCompileResult(result=result_sql)


@app.get("/api/v1/register")
def register_project(
    response: Response,
    project_dir: str = Query(...),
    profiles_dir: str | None = Query(None),
    target: str | None = Query(None),
    runners: DbtProjectContainer = Depends(get_container),
) -> ServerRegisterResult | ServerErrorContainer:
    """Register a new dbt project with the server."""
    project_path = Path(project_dir).expanduser().resolve()
    if project_path in runners.registered_projects():
        return ServerRegisterResult(
            added=project_path.name, projects=list(map(str, runners.registered_projects()))
        )
    try:
        dbt_project = runners.create_project(
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )
        _save_state(runners)
        watcher = DbtProjectWatcher(dbt_project)
        watcher.start()
    except Exception as e:
        response.status_code = 400
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )
    return ServerRegisterResult(
        added=project_path.name, projects=list(map(str, runners.registered_projects()))
    )


@app.delete("/api/v1/register")
def unregister_project(
    response: Response,
    project_dir: str = Query(..., description="Project directory to unregister."),
    runners: DbtProjectContainer = Depends(get_container),
) -> ServerUnregisterResult | ServerErrorContainer:
    """Remove a registered dbt project from the server."""
    if (
        not project_dir
        or (project_path := Path(project_dir).expanduser().resolve())
        not in runners.registered_projects()
    ):
        response.status_code = 404
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectNotRegistered,
                message="Project not registered; register first.",
                data={"registered_projects": list(map(str, runners.registered_projects()))},
            )
        )
    dbt_project = runners.drop_project(project_path)
    _save_state(runners)
    if dbt_project:
        DbtProjectWatcher.stop_path(dbt_project.project_root)
    return ServerUnregisterResult(
        removed=project_path.name, projects=list(map(str, runners.registered_projects()))
    )


@app.get("/api/v1/parse")
@app.get("/api/v1/reset")
def reset_project(
    response: Response,
    background_tasks: BackgroundTasks,
    target: str | None = Query(None),
    reset: bool = Query(False),
    write_manifest: bool = Query(False),
    runner: DbtProject = Depends(get_runner),
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
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )
    return ServerResetResult(result="Project re-parsed successfully.")


@app.get("/api/v1/status")
def health_check(runner: DbtProject = Depends(get_runner)) -> dict[str, t.Any]:
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


@app.post("/api/v1/lint")
def lint_sql(
    response: Response,
    sql_path: str | None = Query(None),
    extra_config_path: str | None = Query(None),
    raw_sql: str | None = Body(None, media_type="text/plain"),
    runner: DbtProject = Depends(get_runner),
) -> ServerLintResult | ServerErrorContainer:
    """Lint SQL string or file via SQLFluff."""
    record = None
    try:
        if sql_path:
            record = runner.lint(
                sql=Path(sql_path),
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
        else:
            if not raw_sql:
                response.status_code = 400
                return ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.MissingRequiredParams,
                        message="No SQL provided. Provide sql_path or SQL body.",
                        data={},
                    )
                )
            record = runner.lint(
                sql=raw_sql,
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.CompileSqlFailure,
                message=str(e),
                data={},
            )
        )
    return ServerLintResult(result=record["violations"] if record is not None else [])


@app.post("/api/v1/format")
def format_sql(
    response: Response,
    sql_path: str | None = Query(None),
    extra_config_path: str | None = Query(None),
    raw_sql: str | None = Body(None, media_type="text/plain"),
    runner: DbtProject = Depends(get_runner),
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
                return ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.MissingRequiredParams,
                        message="No SQL provided. Provide sql_path or SQL body.",
                        data={},
                    )
                )
            success, formatted = runner.format(
                sql=raw_sql,
                extra_config_path=Path(extra_config_path) if extra_config_path else None,
            )
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.CompileSqlFailure,
                message=str(e),
                data={},
            )
        )
    return ServerFormatResult(result=success, sql=formatted)


@app.post("/api/v1/command")
def run_dbt_command(
    response: Response,
    cmd: str = Query(..., description="The dbt command to run, e.g. 'run', 'test', 'build'"),
    args: list[str] | None = Body(None, description="List of positional args for the command"),
    kwargs: dict[str, t.Any] | None = Body(None, description="Keyword args for the command"),
    runner: DbtProject = Depends(get_runner),
) -> dict[str, t.Any] | ServerErrorContainer:
    """Run an arbitrary dbt CLI command on the project."""
    import agate  # pyright: ignore[reportMissingTypeStubs]
    from dbt.artifacts.schemas.base import VersionedSchema
    from dbt.contracts.graph.manifest import Manifest

    try:
        result = runner.command(cmd, *(args or []), **(kwargs or {}))
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ExecuteSqlFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
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


@app.post("/api/v1/write-manifest")
def write_manifest(
    response: Response,
    target_path: str | None = Query(None, description="Optional custom path for manifest.json"),
    runner: DbtProject = Depends(get_runner),
) -> ServerResetResult | ServerErrorContainer:
    """Write the current manifest out to disk."""
    try:
        runner.write_manifest(target_path)
    except Exception as e:
        response.status_code = 500
        return ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(e),
                data=getattr(e, "__dict__", {}),
            )
        )
    return ServerResetResult(result="Manifest written.")


@app.get("/api/v1/projects")
def list_projects(runners: DbtProjectContainer = Depends(get_container)) -> dict[str, t.Any]:
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


@app.post("/api/v1/state")
def inject_state(
    runner: DbtProject = Depends(get_runner), directory: str = Query(...)
) -> ServerErrorContainer | None:
    """Enable dbt deferral by injecting the deferred state into the runner."""
    try:
        runner.inject_deferred_state(directory)
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
def clear_state(runner: DbtProject = Depends(get_runner)) -> None:
    """Clear the deferred state in the runner."""
    try:
        runner.clear_deferred_state()
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
