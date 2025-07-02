# pyright: reportAny=false
"""dbt-core-interface client for interacting with a dbt-core-interface FastAPI server."""

import functools
import logging
import typing as t

import requests

from dbt_core_interface.server import (
    ServerCompileResult,
    ServerErrorContainer,
    ServerFormatResult,
    ServerLintResult,
    ServerRegisterResult,
    ServerResetResult,
    ServerRunResult,
    ServerUnregisterResult,
)
from dbt_core_interface.server import (
    ServerError as _ServerError,
)

logger = logging.getLogger(__name__)


@t.final
class ServerError(Exception):
    """Custom exception for handling server errors from the dbt-core-interface."""

    def __init__(self, error: _ServerError) -> None:
        """Initialize the exception with the error details."""
        self.code = error.code
        self.message = error.message
        self.data = error.data
        super().__init__(f"[{self.code}] {self.message}")


@t.final
class DbtInterfaceClient:
    """Client for interacting with a dbt-core-interface FastAPI server.

    Example:
        client = DbtInterfaceClient("http://localhost:8581")
        client.register_project("/path/to/project")
        result = client.run_sql("select 1")

    """

    def __init__(
        self,
        project_dir: str,
        profiles_dir: str | None = None,
        target: str | None = None,
        base_url: str = "http://localhost:8581",
        timeout: float | tuple[float, float] = 10.0,
    ) -> None:
        """Initialize the client with the base URL and optional project name."""
        self.project_dir = project_dir
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        response = self._register_project(profiles_dir=profiles_dir, target=target)
        logger.info("Registered project '%s' with server at %s", response.added, self.base_url)

    def __del__(self) -> None:
        """Unregister the project on client destruction."""
        try:
            response = self._unregister_project()
            logger.info(
                "Unregistered project '%s' with server at %s", response.removed, self.base_url
            )
        except Exception as e:
            logger.error("Failed to unregister project '%s': %s", self.project_dir, e)

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": "dbt-core-interface-client/1.0",
            "X-dbt-Project": self.project_dir,
        }

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, t.Any] | None = None,
        data: t.Any = None,
        json_payload: t.Any = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = {**self._headers(), **(headers or {})}
        logger.debug(
            "Requesting %s %s with params=%s, data=%s, json=%s, headers=%s",
            method,
            path,
            params,
            data,
            json_payload,
            headers,
        )
        resp = self.session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json_payload,
            headers=headers,
            timeout=self.timeout,
        )
        if resp.status_code >= 400:
            try:
                err = ServerErrorContainer.model_validate(resp.json())
                raise ServerError(err.error)
            except ValueError as e:
                logger.error("Failed to parse error response: %s", e)
                resp.raise_for_status()
        return resp

    def _register_project(
        self,
        profiles_dir: str | None = None,
        target: str | None = None,
    ) -> ServerRegisterResult:
        """Register a new dbt project."""
        params: dict[str, t.Any] = {"project_dir": self.project_dir}
        if profiles_dir is not None:
            params["profiles_dir"] = profiles_dir
        if target is not None:
            params["target"] = target
        resp = self._request("POST", "/register", params=params)
        return ServerRegisterResult.model_validate(resp.json())

    def _unregister_project(self) -> ServerUnregisterResult:
        """Unregister the current project."""
        resp = self._request("POST", "/unregister")
        return ServerUnregisterResult.model_validate(resp.json())

    def run_sql(
        self,
        raw_sql: str,
        limit: int = 200,
        path: str | None = None,
    ) -> ServerRunResult:
        """Execute raw SQL against the registered dbt project."""
        params: dict[str, t.Any] = {"limit": limit}
        if path is not None:
            params["path"] = path
        resp = self._request(
            method="POST",
            path="/run",
            data=raw_sql,
            headers={"Content-Type": "text/plain"},
            params=params,
        )
        return ServerRunResult.model_validate(resp.json())

    def compile_sql(
        self,
        raw_sql: str,
        path: str | None = None,
    ) -> ServerCompileResult:
        """Compile raw SQL without executing it."""
        params: dict[str, t.Any] = {}
        if path is not None:
            params["path"] = path
        resp = self._request(
            method="POST",
            path="/compile",
            data=raw_sql,
            headers={"Content-Type": "text/plain"},
            params=params,
        )
        return ServerCompileResult.model_validate(resp.json())

    def reset_project(
        self,
        target: str | None = None,
        reset: bool = False,
        write_manifest: bool = False,
    ) -> ServerResetResult:
        """Re-parse the dbt project."""
        params: dict[str, t.Any] = {}
        if target is not None:
            params["target"] = target
        if reset:
            params["reset"] = reset
        if write_manifest:
            params["write_manifest"] = write_manifest
        resp = self._request("GET", "/reset", params=params)
        return ServerResetResult.model_validate(resp.json())

    def health_check(self) -> dict[str, t.Any]:
        """Check server health and project status."""
        resp = self._request("GET", "/health")
        return resp.json()

    def heartbeat(self) -> dict[str, t.Any]:
        """Check server availability."""
        resp = self._request("GET", "/heartbeat")
        return resp.json()

    def lint_sql(
        self,
        sql_path: str | None = None,
        raw_sql: str | None = None,
        extra_config_path: str | None = None,
    ) -> ServerLintResult:
        """Lint SQL string or file via SQLFluff."""
        params: dict[str, t.Any] = {}
        if sql_path is not None:
            params["sql_path"] = sql_path
        if extra_config_path is not None:
            params["extra_config_path"] = extra_config_path
        data: t.Any = None
        headers: dict[str, str] | None = None
        if raw_sql is not None and sql_path is None:
            data = raw_sql
            headers = {"Content-Type": "text/plain"}
        resp = self._request("POST", "/lint", params=params, data=data, headers=headers)
        return ServerLintResult.model_validate(resp.json())

    def format_sql(
        self,
        sql_path: str | None = None,
        raw_sql: str | None = None,
        extra_config_path: str | None = None,
    ) -> ServerFormatResult:
        """Format SQL string or file via SQLFluff."""
        params: dict[str, t.Any] = {}
        if sql_path is not None:
            params["sql_path"] = sql_path
        if extra_config_path is not None:
            params["extra_config_path"] = extra_config_path
        data: t.Any = None
        headers: dict[str, str] | None = None
        if raw_sql is not None and sql_path is None:
            data = raw_sql
            headers = {"Content-Type": "text/plain"}
        resp = self._request("POST", "/format", params=params, data=data, headers=headers)
        return ServerFormatResult.model_validate(resp.json())

    def command(
        self,
        cmd: str,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> t.Any:
        """Run an arbitrary dbt command on the server."""
        payload: dict[str, t.Any] = {"args": args, "kwargs": kwargs}
        resp = self._request(
            method="POST",
            path="/command",
            json_payload=payload,
            params={"cmd": cmd},
        )
        return resp.json()

    build = functools.partialmethod(command, "build")
    clean = functools.partialmethod(command, "clean")
    clone = functools.partialmethod(command, "clone")
    compile = functools.partialmethod(command, "compile")
    debug = functools.partialmethod(command, "debug")
    deps = functools.partialmethod(command, "deps")
    docs_generate = functools.partialmethod(command, "docs generate")
    list = functools.partialmethod(command, "list")
    parse = functools.partialmethod(command, "parse")
    run = functools.partialmethod(command, "run")
    run_operation = functools.partialmethod(command, "run-operation")
    seed = functools.partialmethod(command, "seed")
    show = functools.partialmethod(command, "show")
    snapshot = functools.partialmethod(command, "snapshot")
    source_freshness = functools.partialmethod(command, "source freshness")
    test = functools.partialmethod(command, "test")
