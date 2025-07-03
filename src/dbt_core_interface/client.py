# pyright: reportAny=false
"""dbt-core-interface client for interacting with a dbt-core-interface FastAPI server."""

from __future__ import annotations

import functools
import logging
import typing as t
from pathlib import Path
from urllib.parse import urljoin

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
class ServerErrorException(Exception):  # noqa: N818
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
        unregister_on_close: bool = True,
    ) -> None:
        """Initialize the client with the base URL and optional project name."""
        self.project_dir = Path(project_dir).resolve()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "dbt-core-interface-client/1.0",
                "X-dbt-Project": self.project_dir.name,
            }
        )
        self.unregister_on_close = unregister_on_close
        response = self._register_project(profiles_dir=profiles_dir, target=target)
        logger.info("Registered project '%s' with server at %s", response.added, self.base_url)

    def close(self) -> None:
        """Unregister the project on client destruction."""
        if self.unregister_on_close:
            try:
                response = self._unregister_project()
                logger.info(
                    "Unregistered project '%s' with server at %s", response.removed, self.base_url
                )
            except Exception as e:
                logger.error("Failed to unregister project '%s': %s", self.project_dir, e)

    def __enter__(self) -> DbtInterfaceClient:
        """Context manager for the client to ensure proper cleanup."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: Exception | None,
        traceback: t.Any | None,
    ) -> None:
        """Close the client and unregister the project."""
        self.close()
        self.session.close()

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, t.Any] | None = None,
        data: t.Any = None,
        json_payload: t.Any = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        url = urljoin(self.base_url, path)
        headers = headers or {}
        params = params or {}
        params["project_dir"] = str(self.project_dir)

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
                raise ServerErrorException(err.error)
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
        params: dict[str, t.Any] = {}
        if profiles_dir is not None:
            params["profiles_dir"] = profiles_dir
        if target is not None:
            params["target"] = target
        resp = self._request("GET", "/api/v1/register", params=params)
        return ServerRegisterResult.model_validate(resp.json())

    def _unregister_project(self) -> ServerUnregisterResult:
        """Unregister the current project."""
        resp = self._request("DELETE", "/api/v1/register")
        return ServerUnregisterResult.model_validate(resp.json())

    def run_sql(
        self,
        raw_sql: str,
        limit: int = 200,
        model_path: str | None = None,
    ) -> ServerRunResult:
        """Execute raw SQL against the registered dbt project."""
        params: dict[str, t.Any] = {"limit": limit}
        if model_path is not None:
            params["model_path"] = model_path
        resp = self._request(
            method="POST",
            path="/api/v1/run",
            data=raw_sql,
            headers={"Content-Type": "text/plain"},
            params=params,
        )
        return ServerRunResult.model_validate(resp.json())

    def compile_sql(
        self,
        raw_sql: str,
        model_path: str | None = None,
    ) -> ServerCompileResult:
        """Compile raw SQL without executing it."""
        params: dict[str, t.Any] = {}
        if model_path is not None:
            params["model_path"] = model_path
        resp = self._request(
            method="POST",
            path="/api/v1/compile",
            data=raw_sql,
            headers={"Content-Type": "text/plain"},
            params=params,
        )
        return ServerCompileResult.model_validate(resp.json())

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
        resp = self._request("POST", "/api/v1/lint", params=params, data=data, headers=headers)
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
        resp = self._request("POST", "/api/v1/format", params=params, data=data, headers=headers)
        return ServerFormatResult.model_validate(resp.json())

    def parse_project(
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
        resp = self._request("GET", "/api/v1/parse", params=params)
        return ServerResetResult.model_validate(resp.json())

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
            path="/api/v1/command",
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

    def status(self) -> dict[str, t.Any]:
        """Check server diagnostic status."""
        resp = self._request("GET", "/api/v1/status")
        return resp.json()

    def heartbeat(self) -> bool:
        """Check server availability."""
        resp = self._request("GET", "/api/v1/heartbeat")
        pulse = resp.json()
        return pulse["result"]["status"] == "ready"
