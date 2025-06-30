#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false,reportUnnecessaryComparison=false, reportUnreachable=false
"""Minimal dbt-core interface for in-memory manifest management and SQL execution."""

from __future__ import annotations

import atexit
import contextlib
import functools
import json
import logging
import os
import shlex
import sys
import threading
import time
import typing as t
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import get_context as get_mp_context
from pathlib import Path

import dbt.adapters.factory

_get_adapter = dbt.adapters.factory.get_adapter


def _patched_adapter_accessor(config: t.Any) -> t.Any:
    if hasattr(config, "adapter"):
        return config.adapter
    return _get_adapter(config)


dbt.adapters.factory.get_adapter = _patched_adapter_accessor

import rich.logging
from agate import Table  # pyright: ignore[reportMissingTypeStubs]
from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.factory import get_adapter_class_by_name
from dbt.config.runtime import RuntimeConfig
from dbt.context.providers import generate_runtime_macro_context, generate_runtime_model_context
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ManifestNode, SourceDefinition
from dbt.flags import set_from_args
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.parser.read_files import FileDiff, InputFile, ReadFilesFromFileSystem
from dbt.parser.sql import SqlBlockParser, SqlMacroParser
from dbt.task.sql import SqlCompileRunner
from dbt.tracking import disable_tracking
from dbt_common.clients.system import get_env
from dbt_common.context import set_invocation_context
from dbt_common.events.event_manager_client import add_logger_to_manager
from dbt_common.events.logger import LoggerConfig

if t.TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult
    from sqlfluff.core.config import FluffConfig
    from sqlfluff.core.linter.linted_dir import LintingRecord

disable_tracking()
set_invocation_context(get_env())

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(rich.logging.RichHandler())

add_logger_to_manager(
    LoggerConfig(name=__name__, logger=logger),
)

__all__ = ["DbtProject", "DbtConfiguration", "ExecutionResult", "CompilationResult"]

T = t.TypeVar("T")

if sys.version_info >= (3, 10):
    P = t.ParamSpec("P")
else:
    import typing_extensions as t_ext

    P = t_ext.ParamSpec("P")


def _get_project_dir() -> Path:
    """Get the default project directory following dbt heuristics."""
    return Path(os.getenv("DBT_PROJECT_DIR", os.getcwd())).expanduser().resolve()


def _get_profiles_dir(project_dir: Path | str | None = None) -> Path:
    """Get the default profiles directory following dbt heuristics."""
    if "DBT_PROFILES_DIR" not in os.environ:
        _project_dir = Path(project_dir or _get_project_dir())
        if _project_dir.is_dir() and _project_dir.joinpath("profiles.yml").exists():
            return _project_dir
        return Path.home() / ".dbt"
    return Path(os.environ["DBT_PROFILES_DIR"]).expanduser().resolve()


DEFAULT_PROFILES_DIR = str(_get_profiles_dir())
DEFAULT_PROJECT_DIR = str(_get_project_dir())


@dataclass(frozen=True)
class DbtConfiguration:
    """Minimal dbt configuration."""

    project_dir: str = DEFAULT_PROJECT_DIR
    profiles_dir: str = DEFAULT_PROFILES_DIR
    target: str | None = None
    threads: int = 1
    vars: dict[str, t.Any] = field(default_factory=dict)
    profile: str | None = None

    single_threaded: bool = True
    quiet: bool = True
    use_experimental_parser: bool = True
    static_parser: bool = True
    partial_parse: bool = True

    dependencies: list[str] = field(default_factory=list)
    which: str = "zezima was here"
    REQUIRE_RESOURCE_NAMES_WITHOUT_SPACES: bool = field(default_factory=bool)


_use_slots = {}
if sys.version_info >= (3, 10):
    _use_slots = {"slots": True}


@dataclass(frozen=True, **_use_slots)
class ExecutionResult:
    """Result of SQL execution."""

    adapter_response: AdapterResponse
    table: Table
    raw_code: str
    compiled_code: str


@dataclass(frozen=True, **_use_slots)
class CompilationResult:
    """Result of SQL compilation."""

    raw_code: str
    compiled_code: str
    node: ManifestNode


def _ensure_connection(f: t.Callable[P, T]) -> t.Callable[P, T]:
    """Set the adapter connection before executing decorated method."""

    @functools.wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast(DbtProject, args[0])
        _ = self.adapter.connections.set_connection_name()
        return f(*args, **kwargs)

    return wrapper


@t.final
class DbtProject:
    """Minimal dbt project interface for manifest management and SQL execution."""

    ADAPTER_TTL: int = 3600

    def __init__(
        self,
        target: str | None = None,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        threads: int = 1,
        vars: dict[str, t.Any] | None = None,
        load: bool = True,
        autoregister: bool = True,
    ) -> None:
        """Initialize the dbt project."""
        if project_dir is not None and profiles_dir is None:
            profiles_dir = str(_get_profiles_dir(project_dir).resolve())

        project_dir = project_dir or DEFAULT_PROJECT_DIR
        profiles_dir = profiles_dir or DEFAULT_PROFILES_DIR

        self._args = DbtConfiguration(
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            threads=threads,
            vars=vars or {},
        )

        set_from_args(self._args, None)  # pyright: ignore[reportArgumentType]
        self.runtime_config: RuntimeConfig = RuntimeConfig.from_args(self._args)

        self._adapter: BaseAdapter | None = None
        self._adapter_created_at: float = 0
        self._manifest: Manifest | None = None
        self._last_parsed_at: float = 0

        self._adapter_lock = threading.Lock()
        self._manifest_lock = threading.Lock()

        self._sql_parser: SqlBlockParser | None = None
        self._macro_parser: SqlMacroParser | None = None

        self._pool: ThreadPoolExecutor | None = None

        self.__compilation_cache: dict[str, CompilationResult] = {}
        self.__manifest_loader = ManifestLoader(
            self.runtime_config,
            self.runtime_config.load_dependencies(),
        )

        if load:
            self.parse_project(write_manifest=True)

        if autoregister:
            container = DbtProjectContainer()
            container.add_project(self)

    def __repr__(self) -> str:  # pyright: ignore[reportImplicitOverride]
        """Return a string representation of the DbtProject instance."""
        return f"DbtProject(name={self.project_name}, root={self.project_root}, last_parsed_at={self._last_parsed_at})"

    @classmethod
    def from_config(cls, config: DbtConfiguration) -> DbtProject:
        """Create project from configuration."""
        return cls(
            target=config.target,
            profiles_dir=config.profiles_dir,
            project_dir=config.project_dir,
            threads=config.threads,
            vars=config.vars,
        )

    @property
    def args(self) -> DbtConfiguration:
        """Get the args for the DbtProject instance."""
        return self._args

    @args.setter
    def args(self, value: DbtConfiguration) -> None:
        """Set the args for the DbtProject instance and update runtime config."""
        set_from_args(value, None)  # pyright: ignore[reportArgumentType]
        self.runtime_config = RuntimeConfig.from_args(value)
        self.__manifest_loader = ManifestLoader(
            self.runtime_config,
            self.runtime_config.load_dependencies(),
        )
        self._args = value

    @property
    def adapter(self) -> BaseAdapter:
        """Get adapter with TTL management for long-running processes."""
        with self._adapter_lock:
            if self._adapter is None or (time.time() - self._adapter_created_at) > self.ADAPTER_TTL:
                return self.create_adapter(replace=True)
        if self._adapter is None:
            raise RuntimeError("Adapter not initialized...")
        return self._adapter

    @property
    def manifest(self) -> Manifest:
        """The parsed dbt manifest for the project."""
        if self._manifest is None:
            self.parse_project(write_manifest=True)
        if self._manifest is None:
            raise RuntimeError("Manifest not loaded...")
        return self._manifest

    @property
    def project_name(self) -> str:
        """The name of the dbt project."""
        return self.runtime_config.project_name

    @property
    def project_root(self) -> Path:
        """The root directory of the dbt project."""
        return Path(self.runtime_config.project_root).resolve()

    @property
    def target_path(self) -> Path:
        """The directory where dbt will write compiled artifacts."""
        return self.project_root / self.runtime_config.target_path

    @property
    def log_path(self) -> Path:
        """The directory where dbt will write logs."""
        return self.project_root / self.runtime_config.log_path

    @property
    def packages_install_path(self) -> Path:
        """The directory where dbt will install packages."""
        return self.project_root / self.runtime_config.packages_install_path

    @property
    def model_paths(self) -> list[Path]:
        """The paths where dbt models are located."""
        return [self.project_root / p for p in self.runtime_config.model_paths]

    @property
    def snapshot_paths(self) -> list[Path]:
        """The paths where dbt snapshots are located."""
        return [self.project_root / p for p in self.runtime_config.snapshot_paths]

    @property
    def macro_paths(self) -> list[Path]:
        """The paths where dbt macros are located."""
        return [self.project_root / p for p in self.runtime_config.macro_paths]

    @property
    def dbt_project_yml(self) -> Path:
        """The path to the dbt_project.yml file."""
        return self.project_root / "dbt_project.yml"

    @property
    def profiles_yml(self) -> Path:
        """The path to the profiles.yml file."""
        return Path(self._args.profiles_dir).expanduser().resolve() / "profiles.yml"

    @property
    def pool(self) -> ThreadPoolExecutor:
        """Get thread pool with ready database connections."""
        if self._pool is None:

            def _initializer() -> None:
                set_invocation_context(get_env())
                _ = self.adapter.connections.set_connection_name()

            self._pool = ThreadPoolExecutor(
                max_workers=self.runtime_config.threads, initializer=_initializer
            )
        if not self._pool:
            raise RuntimeError("Thread pool not initialized")
        return self._pool

    def create_adapter(
        self, replace: bool = False, verify_connectivity: bool = True
    ) -> BaseAdapter:
        """Initialize or reinitialize the adapter."""
        if self._adapter is not None:
            if not replace:
                self.runtime_config.adapter = self._adapter  # pyright: ignore[reportAttributeAccessIssue]
                return self._adapter

            with contextlib.suppress(Exception):
                if self._pool:
                    self._pool.shutdown(wait=True, cancel_futures=True)
                    self._pool = None

                self._adapter.connections.cleanup_all()
                _ = atexit.unregister(self._adapter.connections.cleanup_all)

        adapter_cls = get_adapter_class_by_name(self.runtime_config.credentials.type)
        self._adapter = t.cast(
            BaseAdapter,
            adapter_cls(self.runtime_config, get_mp_context("spawn")),  # pyright: ignore[reportInvalidCast,reportCallIssue]
        )

        self._adapter_created_at = time.time()

        if verify_connectivity:
            with self._adapter.connection_named("dbt-core-interface"):
                self._adapter.debug_query()

        logger.debug(f"Initialized adapter for {self.project_name}")
        self.runtime_config.adapter = self._adapter  # pyright: ignore[reportAttributeAccessIssue]
        self._adapter.set_macro_context_generator(generate_runtime_macro_context)  # pyright: ignore[reportArgumentType]
        self.__manifest_loader.macro_hook = self._adapter.connections.set_query_header  # pyright: ignore[reportAttributeAccessIssue]

        _ = atexit.register(self._adapter.connections.cleanup_all)

        return self._adapter

    def parse_project(
        self, write_manifest: bool = False, reparse_configuration: bool = False
    ) -> None:
        """Parse the dbt project and load manifest."""
        if reparse_configuration:
            self.runtime_config = RuntimeConfig.from_args(self._args)
            self.__manifest_loader = ManifestLoader(
                self.runtime_config,
                self.runtime_config.load_dependencies(),
            )

        _ = self.create_adapter(replace=reparse_configuration, verify_connectivity=False)

        with self._manifest_lock:
            self.__manifest_loader.manifest = Manifest()
            self.__manifest_loader.manifest.state_check = (
                self.__manifest_loader.build_manifest_state_check()
            )
            self._manifest = self.__manifest_loader.saved_manifest = self.__manifest_loader.load()
            self._manifest.build_flat_graph()
            self._manifest.build_parent_and_child_maps()
            self._manifest.build_group_map()

            self._sql_parser = None
            self._macro_parser = None

            self.__manifest_loader.save_macros_to_adapter(self.adapter)
            self.__compilation_cache.clear()

            if write_manifest:
                self.write_manifest()

        logger.info(f"Parsed project: {self.project_name}")
        self._last_parsed_at = time.time()

    def parse_paths(self, *paths: Path | str) -> None:
        """Parse an explicit set of paths in the dbt project leveraging FileDiff.

        Like a scalpel for large projects when iterating on a single model. Will most likely
        tie to document/didSave via an editor extension. Paths should be relative to the
        project root or absolute paths.
        """
        changes = {"added": [], "changed": [], "deleted": []}
        for path in paths:
            path = Path(path)
            if path.is_absolute():
                path = path.relative_to(self.project_root)

            real_path = self.project_root / path
            if real_path.exists():
                content = real_path.read_text(encoding="utf-8")
                stat = real_path.stat()
                mtime = stat.st_mtime or time.time()
                changes[
                    "changed" if f"{self.project_name}://{path}" in self.manifest.files else "added"
                ].append(InputFile(str(path), content, modification_time=mtime))
            else:
                changes["deleted"].append(InputFile(str(path), "", modification_time=time.time()))

        try:
            self.__manifest_loader.file_diff = FileDiff(**changes)  # pyright: ignore[reportUnknownArgumentType]
            self.parse_project(write_manifest=False)
        finally:
            self.__manifest_loader.file_diff = None

    def generate_runtime_model_context(
        self, node_or_path: ManifestNode | Path | str, /
    ) -> dict[str, t.Any]:
        """Generate runtime jinja context for a model node."""
        if isinstance(node_or_path, (Path, str)):
            maybe_node = self.get_node_by_path(node_or_path)
            if maybe_node is None:
                raise ValueError(f"Node not found for path: {node_or_path}")
            node = maybe_node
        else:
            node = node_or_path

        return generate_runtime_model_context(
            node,
            self.runtime_config,
            self.manifest,
        )

    def ref(
        self,
        model_name: str,
        target_package: str | None = None,
        model_version: int | None = None,
        source_node: ManifestNode | None = None,
    ) -> ManifestNode | None:
        """Look up a model node by name, package, and version.

        Akin to using {{ ref() }} in SQL.
        """
        candidates: list[str | None] = [self.project_name, None]
        if target_package:
            candidates.insert(0, target_package)

        for package in candidates:
            node = self.manifest.ref_lookup.find(
                model_name, package, model_version, self.manifest, source_node
            )
            if node:
                return node

    def source(self, source_name: str, table_name: str) -> SourceDefinition | None:
        """Look up a source by name and table name.

        Akin to using {{ source() }} in SQL.
        """
        return self.manifest.source_lookup.find(f"{source_name}.{table_name}", None, self.manifest)

    @_ensure_connection
    def execute_sql(self, sql: str, compile: bool = True) -> ExecutionResult:
        """Execute SQL against the database via the adapter optionally compiling it."""
        raw_code = compiled_code = sql
        if compile:
            with self._manifest_lock:
                temp_node, cleanup = self._create_temp_node(sql)
                try:
                    compiled_result = self.compile_node(temp_node, update_depends_on=False)
                    compiled_code = compiled_result.compiled_code
                finally:
                    cleanup()

        response, table = self.adapter.execute(compiled_code, auto_begin=False, fetch=True)
        return ExecutionResult(
            adapter_response=response,  # pyright: ignore[reportUnknownArgumentType]
            table=table,  # pyright: ignore[reportUnknownArgumentType]
            raw_code=raw_code,
            compiled_code=compiled_code,
        )

    query = execute_sql

    @_ensure_connection
    def compile_sql(self, sql: str) -> CompilationResult:
        """Compile SQL without execution.

        Leverages a compilation cache to avoid redundant parsing.
        """
        if sql in self.__compilation_cache:
            return self.__compilation_cache[sql]

        with self._manifest_lock:
            temp_node, cleanup = self._create_temp_node(sql)
            try:
                response = self.compile_node(temp_node, update_depends_on=False)
                self.__compilation_cache[sql] = response
                if len(self.__compilation_cache) > 128:
                    _ = self.__compilation_cache.pop(next(iter(self.__compilation_cache)))
                return response
            finally:
                cleanup()

    @_ensure_connection
    def compile_node(self, node: ManifestNode, update_depends_on: bool = True) -> CompilationResult:
        """Compile a manifest node."""
        with contextlib.suppress(Exception):
            node.compiled_code = None  # pyright: ignore[reportAttributeAccessIssue]

        runner = SqlCompileRunner(
            config=self.runtime_config, adapter=self.adapter, node=node, node_index=1, num_nodes=1
        )

        if update_depends_on:
            process_node(self.runtime_config, self.manifest, node)

        compiled_node = runner.compile(self.manifest)

        return CompilationResult(
            raw_code=node.raw_code,
            compiled_code=compiled_node.compiled_code or node.raw_code,
            node=compiled_node,
        )

    def _create_temp_node(
        self, sql: str, node_id: str | None = None
    ) -> tuple[ManifestNode, t.Callable[[], None]]:
        """Create a temporary node for SQL execution/compilation."""
        node_id = node_id or f"temp_node_{uuid.uuid4().hex[:8]}"
        sql_node = self.sql_parser.parse_remote(sql, node_id)
        process_node(self.runtime_config, self.manifest, sql_node)

        def _cleanup() -> None:
            with contextlib.suppress(KeyError):
                del self.manifest.nodes[sql_node.unique_id]

        return sql_node, _cleanup

    @_ensure_connection
    def get_relation(self, database: str, schema: str, name: str) -> BaseRelation | None:
        """Get relation from adapter."""
        return self.adapter.get_relation(database, schema, name)

    @_ensure_connection
    def relation_exists(self, database: str, schema: str, name: str) -> bool:
        """Check if relation exists."""
        return self.get_relation(database, schema, name) is not None

    def write_manifest(self, path: Path | str | None = None) -> None:
        """Write manifest to disk."""
        if path is None:
            path = self.project_root / "target" / "manifest.json"
        else:
            path = Path(path)

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            json.dump(self.manifest.to_dict(), f, indent=2)

        logger.info(f"Wrote manifest to {path}")

    @property
    def sql_parser(self) -> SqlBlockParser:
        """Get SQL parser (lazy-loaded)."""
        if self._sql_parser is None:
            self._sql_parser = SqlBlockParser(
                project=self.runtime_config,
                manifest=self.manifest,
                root_project=self.runtime_config,
            )
        return self._sql_parser

    @property
    def macro_parser(self) -> SqlMacroParser:
        """Get macro parser (lazy-loaded)."""
        if self._macro_parser is None:
            self._macro_parser = SqlMacroParser(
                project=self.runtime_config,
                manifest=self.manifest,
            )
        return self._macro_parser

    def get_node_by_path(self, path: Path | str) -> ManifestNode | None:
        """Get a node by its path on disk."""
        path = Path(path)
        if not path.is_absolute():
            path = self.project_root / path
        path = path.expanduser().resolve()
        for node in self.manifest.nodes.values():
            if self.project_root / node.original_file_path == path:
                return node
        return None

    def create_project_watcher(self, check_interval: float = 2.0) -> DbtProjectWatcher:
        """Create a project watcher for automatic incremental updates."""
        return DbtProjectWatcher(self, check_interval)

    def command(self, cmd: str, *args: t.Any, **kwargs: t.Any) -> dbtRunnerResult:
        """Run a dbt command with the current project manifest."""
        from dbt.cli.main import dbtRunner

        runner = dbtRunner(self.manifest)
        kwargs.update(
            {
                "project_dir": str(self.dbt_project_yml.parent),
                "profiles_dir": str(self.profiles_yml.parent),
            }
        )
        expanded_cmd = [*shlex.split(cmd)]
        for arg in args:
            expanded_cmd.extend(shlex.split(str(arg)))
        with dbt.adapters.factory.adapter_management():
            return runner.invoke(expanded_cmd, **kwargs)

    _sqlfluff_mtime_cache: dict[Path, float] = {}

    def get_sqlfluff_configuration(
        self,
        path: Path | str | None = None,
        extra_config_path: Path | str | None = None,
        ignore_local_config: bool = False,
        **kwargs: t.Any,
    ) -> FluffConfig:
        """Load the SQLFluff configuration for a given path, otherwise for the project itself."""
        from sqlfluff.core.config import ConfigLoader, FluffConfig

        overrides = {k: kwargs[k] for k in kwargs if kwargs[k] is not None}
        overrides["dialect"] = self.runtime_config.credentials.type
        overrides["processes"] = 1

        loader = ConfigLoader.get_global()
        loader_cache = loader._config_cache  # pyright: ignore[reportPrivateUsage]
        for p in list(loader_cache):
            p_obj = Path(p)
            last_mtime = self._sqlfluff_mtime_cache.get(p_obj, 0.0)
            curr_mtime = p_obj.stat().st_mtime if p_obj.exists() else 0.0
            if curr_mtime > last_mtime:
                del loader_cache[p]

        if extra_config_path:
            _ = loader_cache.pop(str(extra_config_path), None)

        fluff_conf = FluffConfig.from_path(
            path=str(path or self.project_root),
            extra_config_path=str(extra_config_path) if extra_config_path else None,
            ignore_local_config=ignore_local_config,
            overrides=overrides,
        )

        for p in loader_cache:
            p_obj = Path(p)
            self._sqlfluff_mtime_cache[p_obj] = p_obj.stat().st_mtime if p_obj.exists() else 0.0

        return fluff_conf

    def lint(
        self,
        sql: Path | str | None = None,
        extra_config_path: Path | str | None = None,
        ignore_local_config: bool = False,
        fluff_conf: FluffConfig | None = None,
    ) -> LintingRecord | None:
        """Lint specified file or SQL string."""
        from sqlfluff.cli.commands import get_linter_and_formatter

        fluff_conf = fluff_conf or self.get_sqlfluff_configuration(
            sql if isinstance(sql, Path) else None,
            extra_config_path,
            ignore_local_config,
            require_dialect=False,
            nocolor=True,
        )
        lint, _ = get_linter_and_formatter(fluff_conf)

        if sql is None:
            # TODO: lint whole project
            return
        elif isinstance(sql, str):
            result = lint.lint_string_wrapped(sql)
        else:
            result = lint.lint_paths((str(sql),), ignore_files=False)

        records = result.as_records()
        return records[0] if records else None

    def format(
        self,
        sql: Path | str | None = None,
        extra_config_path: Path | None = None,
        ignore_local_config: bool = False,
        fluff_conf: FluffConfig | None = None,
    ) -> tuple[bool, str | None]:
        """Format specified file or SQL string."""
        from sqlfluff.cli.commands import get_linter_and_formatter
        from sqlfluff.core import SQLLintError

        logger.info(f"""format_command(
        {self.project_root},
        {str(sql)[:100]},
        {extra_config_path},
        {ignore_local_config})
        """)

        fluff_conf = fluff_conf or self.get_sqlfluff_configuration(
            sql if isinstance(sql, Path) else None,
            extra_config_path,
            ignore_local_config,
            require_dialect=False,
            nocolor=True,
            rules=(
                # all of the capitalisation rules
                "capitalisation,"
                # all of the layout rules
                "layout,"
                # safe rules from other groups
                "ambiguous.union,"
                "convention.not_equal,"
                "convention.coalesce,"
                "convention.select_trailing_comma,"
                "convention.is_null,"
                "jinja.padding,"
                "structure.distinct,"
            ),
        )
        lint, formatter = get_linter_and_formatter(fluff_conf)

        result_sql = None
        if sql is None:
            # TODO: format whole project
            return True, result_sql
        if isinstance(sql, str):
            logger.info(f"Formatting SQL string: {sql[:100]}")
            result = lint.lint_string_wrapped(sql, fname="stdin", fix=True)
            _, num_filtered_errors = result.count_tmp_prs_errors()
            result.discard_fixes_for_lint_errors_in_files_with_tmp_or_prs_errors()
            success = not num_filtered_errors
            if success:
                num_fixable = result.num_violations(types=SQLLintError, fixable=True)
                if num_fixable > 0:
                    logger.info(f"Fixing {num_fixable} errors in SQL string")
                    result_sql = result.paths[0].files[0].fix_string()[0]
                    logger.info(f"Result string has changes? {result_sql != sql}")
                else:
                    logger.info("No fixable errors in SQL string")
                    result_sql = sql
        else:
            logger.info(f"Formatting SQL file: {sql}")
            before_modified = datetime.fromtimestamp(sql.stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            logger.info(f"Before fixing, modified: {before_modified}")
            lint_result = lint.lint_paths(
                (str(sql),),
                fix=True,
                ignore_non_existent_files=False,
                apply_fixes=True,
                fix_even_unparsable=False,
            )
            _, num_filtered_errors = lint_result.count_tmp_prs_errors()
            lint_result.discard_fixes_for_lint_errors_in_files_with_tmp_or_prs_errors()
            success = not num_filtered_errors
            num_fixable = lint_result.num_violations(types=SQLLintError, fixable=True)
            if num_fixable > 0:
                logger.info(f"Fixing {num_fixable} errors in SQL file")
                res = lint_result.persist_changes(formatter=formatter)
                after_modified = datetime.fromtimestamp(sql.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                logger.info(f"After fixing, modified: {after_modified}")
                logger.info(
                    f"File modification time has changes? {before_modified != after_modified}"
                )
                success = all(res.values())  # pyright: ignore[reportUnknownArgumentType]
            else:
                logger.info("No fixable errors in SQL file")

        logger.info(
            f"format_command returning success={success}, result_sql={result_sql[:100] if result_sql is not None else 'n/a'}"
        )
        return success, result_sql


@t.final
class DbtProjectWatcher:
    """Watch dbt files for changes and automatically update the manifest."""

    def __init__(self, project: DbtProject, check_interval: float = 2.0) -> None:
        self._project = project
        self.check_interval = check_interval

        self.reader = ReadFilesFromFileSystem(
            all_projects=self._project.runtime_config.load_dependencies(),
            files={},
            saved_files=self._project.manifest.files,
        )

        self._mtimes: dict[Path, float] = {}
        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start monitoring files for changes."""
        if self._running:
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Project watcher started")

    def stop(self) -> None:
        """Stop monitoring files."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("Project watcher stopped")

    def _monitor_loop(self) -> None:
        """Run the main monitoring loop."""
        self._initialize_file_mtimes()
        set_invocation_context(get_env())

        while self._running and not self._stop_event.is_set():
            try:
                change_level = self._check_for_changes()
                if change_level:
                    self._project.parse_project(
                        write_manifest=False, reparse_configuration=change_level > 1
                    )
            except Exception as e:
                logger.error(f"Error in project watcher loop: {e}")

            _ = self._stop_event.wait(self.check_interval)

    def _initialize_file_mtimes(self) -> None:
        """Initialize the file modification time tracking."""
        for f_proxy in self._project.manifest.files.values():
            path = Path(f_proxy.path.absolute_path)
            if path.exists():
                self._mtimes[path] = path.stat().st_mtime
        self._mtimes[self._project.dbt_project_yml] = self._project.dbt_project_yml.stat().st_mtime
        self._mtimes[self._project.profiles_yml] = self._project.profiles_yml.stat().st_mtime
        logger.debug(f"Initialized tracking for {len(self._mtimes)} files")

    def _check_for_changes(self) -> int:
        """Check for changes in tracked files.

        A return value of 0 means no changes, 1 means files were added/removed, and 2 means
        a configuration file was modified (dbt_project.yml or profiles.yml).
        """
        for path in (self._project.dbt_project_yml, self._project.profiles_yml):
            try:
                current_mtime = path.stat().st_mtime if path.exists() else 0.0
                stamped_mtime = self._mtimes.get(path)
                if stamped_mtime is None or current_mtime > stamped_mtime:
                    self._mtimes[path] = current_mtime
                    return 2
            except OSError as e:
                logger.warning(f"Error checking file {path}: {e}")
                continue

        self.reader.read_files()

        changes_detected = 0
        for k, f_proxy in list(self.reader.files.items()):
            path = Path(f_proxy.path.absolute_path)
            try:
                if not path.exists():  # DELETED
                    _ = self._mtimes.pop(path, None)
                    _ = self.reader.files.pop(k, None)
                    changes_detected = 1

                current_mtime = path.stat().st_mtime if path.exists() else 0.0
                stamped_mtime = self._mtimes.get(path)

                if stamped_mtime is None:  # ADDED
                    changes_detected = 1
                elif current_mtime > stamped_mtime:  # CHANGED
                    changes_detected = 1

                self._mtimes[path] = current_mtime

            except OSError as e:
                logger.warning(f"Error checking file {path}: {e}")
                continue

        return changes_detected


@t.final
class DbtProjectContainer:
    """Singleton container for managing multiple DbtProject instances."""

    _instance: DbtProjectContainer | None = None
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> DbtProjectContainer:
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()

        return cls._instance

    def _initialize(self) -> None:
        self._projects: dict[Path, DbtProject] = {}  # pyright: ignore[reportUninitializedInstanceVariable]
        self._default_project: Path | None = None  # pyright: ignore[reportUninitializedInstanceVariable]
        self._lock = threading.RLock()  # pyright: ignore[reportUninitializedInstanceVariable]

    def get_project(self, name: Path | str) -> DbtProject | None:
        """Return the project registered at the given Path, or None if not found."""
        with self._lock:
            return self._projects.get(Path(name).expanduser().resolve())

    def find_project_in_tree(self, path: Path | str) -> DbtProject | None:
        """Return the project whose root is at or above the given path."""
        p = Path(path).expanduser().resolve()
        with self._lock:
            for project in self._projects.values():
                root = project.project_root.resolve()
                if p == root or root in p.parents:
                    return project
        return None

    def get_default_project(self) -> DbtProject | None:
        """Return the default project (first added), or None if no projects exist."""
        with self._lock:
            if self._default_project is None:
                return None
            return self._projects.get(self._default_project)

    def set_default_project(self, path: Path | str) -> None:
        """Set the default project by name."""
        self._default_project = Path(path).expanduser().resolve()

    def add_project(self, project: DbtProject) -> None:
        """Add a project to the container."""
        with self._lock:
            if project.project_root in self._projects:
                raise ValueError(f"Project '{project.project_root}' is already registered.")
            self._projects[project.project_root] = project
            if self._default_project is None:
                self._default_project = project.project_root

    def create_project(
        self,
        target: str | None = None,
        profiles_dir: str | None = None,
        project_dir: str | None = None,
        threads: int = 1,
        vars: dict[str, t.Any] | None = None,
    ) -> DbtProject:
        """Instantiate and register a new DbtProject."""
        project = DbtProject(
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            threads=threads,
            vars=vars or {},
        )
        self.add_project(project)
        return project

    def create_project_from_config(self, config: DbtConfiguration) -> DbtProject:
        """Instantiate a project from configuration and register it."""
        project = DbtProject.from_config(config)
        self.add_project(project)
        return project

    def drop_project(self, path: Path | str) -> DbtProject | None:
        """Unregister and clean up the project with the given name."""
        with self._lock:
            project = self._projects.pop(p := Path(path).expanduser().resolve(), None)
            if project is None:
                return
            project.adapter.connections.cleanup_all()
            if p == self._default_project:
                self._default_project = next(iter(self._projects), None)
            return project

    def drop_all(self) -> None:
        """Unregister and clean up all projects."""
        with self._lock:
            for name in list(self._projects):
                _ = self.drop_project(name)

    def reparse_all(self) -> None:
        """Re-parse all registered projects safely."""
        with self._lock:
            for project in self._projects.values():
                project.parse_project()

    def registered_projects(self) -> list[Path]:
        """Return a list of registered project paths."""
        with self._lock:
            return list(self._projects.keys())

    def __len__(self) -> int:
        return len(self._projects)

    def __getitem__(self, path: Path | str) -> DbtProject:
        project = self.get_project(path)
        if project is None:
            raise KeyError(f"No project registered under '{path}'.")
        return project

    def __contains__(self, path: Path | str) -> bool:
        return path in self._projects

    def __iter__(self) -> t.Generator[DbtProject, None, None]:
        yield from self._projects.values()

    def __repr__(self) -> str:  # pyright: ignore[reportImplicitOverride]
        return "\n".join(
            f"DbtProject(name={proj.project_name}, root={proj.project_root})"
            for proj in self._projects.values()
        )
