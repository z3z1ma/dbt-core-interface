#!/usr/bin/env python
"""The interface for interacting with dbt-core.

We package the interface as a single python module that can be imported
and used in other python projects. You do not need to include dbt-core-interface
as a dependency in your project if you do not want to. You can simply copy
the dbt_core_interface folder into your project and import it from there.
"""

# region dbt-core-interface imports & monkey patches

if 1:  # this stops ruff from complaining about the import order
    import dbt.adapters.factory

    # This is the secret sauce that makes dbt-core-interface able to
    # micromanage multiple projects in memory at once. It is a monkey patch
    # to overcome a severe design limitation in dbt-core. This will likely
    # be addressed in a future version of dbt-core.
    dbt.adapters.factory.get_adapter = lambda config: config.adapter  # type: ignore

import decimal
import functools
import importlib
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from collections import OrderedDict, UserDict
from contextlib import contextmanager, redirect_stdout
from copy import copy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache, wraps
from multiprocessing import get_context
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import unquote as urlunquote

import dbt.version

# We maintain the smallest possible surface area of dbt imports
from dbt.adapters.factory import get_adapter_class_by_name

try:
    # dbt >= 1.8
    from dbt_common.clients.system import make_directory
except ImportError:
    # dbt < 1.8
    from dbt.clients.system import make_directory
from dbt.config.runtime import RuntimeConfig
from dbt.flags import set_from_args
from dbt.node_types import NodeType
from dbt.parser.manifest import PARTIAL_PARSE_FILE_NAME, ManifestLoader, process_node
from dbt.parser.sql import SqlBlockParser, SqlMacroParser
from dbt.task.sql import SqlCompileRunner
from dbt.tracking import disable_tracking

# brute force import for dbt 1.3 back-compat
# these are here for consumers of dbt-core-interface
try:
    # dbt <= 1.3
    from dbt.contracts.graph.compiled import ManifestNode  # type: ignore
    from dbt.contracts.graph.parsed import ColumnInfo  # type: ignore
except Exception:
    # dbt > 1.3
    from dbt.contracts.graph.nodes import ColumnInfo, ManifestNode  # type: ignore

if TYPE_CHECKING:
    # These imports are only used for type checking
    from agate import Table  # type: ignore  # No stubs for agate
    from dbt.adapters.base import BaseAdapter, BaseRelation  # type: ignore

    try:
        # dbt >= 1.8
        from dbt.adapters.contracts.connection import AdapterResponse
        from dbt_common.semver import VersionSpecifier

    except ImportError:
        # dbt < 1.8
        from dbt.contracts.connection import AdapterResponse
        from dbt.semver import VersionSpecifier
    from dbt.contracts.results import ExecutionResult, RunExecutionResult
    from dbt.task.runnable import ManifestTask

try:
    import dbt_core_interface.state as dci_state
    from dbt_core_interface.sqlfluff_util import format_command, lint_command
except ImportError:
    dci_state = None
    format_command = None
    lint_command = None

from agate import Column, Number, Table, Text

try:
    # dbt >= 1.8
    from dbt_common.clients.agate_helper import Integer
except ImportError:
    try:
        # dbt < 1.8 and older Agate version
        from dbt.clients.agate_helper import Integer
    except ImportError:
        # dbt < 1.8 and newer Agate version
        from dbt.clients.agate_helper import Number as Integer

# dbt-core-interface is designed for non-standard use. There is no
# reason to track usage of this package.
disable_tracking()

urlunquote = functools.partial(urlunquote, encoding="latin1")

# Version specific dbt constants and overrides
__dbt_major_version__ = int(dbt.version.installed.major or 0)
__dbt_minor_version__ = int(dbt.version.installed.minor or 0)
__dbt_patch_version__ = int(dbt.version.installed.patch or 0)

RAW_CODE = "raw_code"
COMPILED_CODE = "compiled_code"


def default_project_dir() -> Path:
    """Get the default project directory."""
    if "DBT_PROJECT_DIR" in os.environ:
        return Path(os.environ["DBT_PROJECT_DIR"]).resolve()
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    return next((x for x in paths if (x / "dbt_project.yml").exists()), Path.cwd())


def default_profiles_dir() -> Path:
    """Get the default profiles directory."""
    if "DBT_PROFILES_DIR" in os.environ:
        return Path(os.environ["DBT_PROFILES_DIR"]).resolve()
    return Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"


DEFAULT_PROFILES_DIR = str(default_profiles_dir())
DEFAULT_PROJECT_DIR = str(default_project_dir())


def write_manifest_for_partial_parse(self: ManifestLoader):
    """Monkey patch for dbt manifest loader."""
    path = os.path.join(
        self.root_project.project_root,
        self.root_project.target_path,
        PARTIAL_PARSE_FILE_NAME,
    )
    try:
        if self.manifest.metadata.dbt_version != dbt.version.__version__:
            self.manifest.metadata.dbt_version = dbt.version.__version__
        manifest_msgpack = self.manifest.to_msgpack()
        make_directory(os.path.dirname(path))
        with open(path, "wb") as fp:
            fp.write(manifest_msgpack)
    except Exception:
        raise


if (__dbt_major_version__, __dbt_minor_version__) < (1, 4):
    # Patched so we write partial parse to correct directory
    # https://github.com/dbt-labs/dbt-core/blob/v1.3.2/core/dbt/parser/manifest.py#L548
    ManifestLoader.write_manifest_for_partial_parse = write_manifest_for_partial_parse


__all__ = [
    "DbtProject",
    "DbtProjectContainer",
    "DbtAdapterExecutionResult",
    "DbtAdapterCompilationResult",
    "DbtManifestProxy",
    "DbtConfiguration",
    "__dbt_major_version__",
    "__dbt_minor_version__",
    "__dbt_patch_version__",
    "DEFAULT_PROFILES_DIR",
    "DEFAULT_PROJECT_DIR",
    "ServerRunResult",
    "ServerCompileResult",
    "ServerResetResult",
    "ServerRegisterResult",
    "ServerUnregisterResult",
    "ServerErrorCode",
    "ServerError",
    "ServerErrorContainer",
    "run_server",
    "default_project_dir",
    "default_profiles_dir",
    "ColumnInfo",
    "ManifestNode",
]

T = TypeVar("T")
JINJA_CONTROL_SEQUENCES = ["{{", "}}", "{%", "%}", "{#", "#}"]

LOGGER = logging.getLogger(__name__)

__version__ = dbt.version.__version__

# endregion

# region dbt-core-interface core


class DbtCommand(str, Enum):
    """The dbt commands we support."""

    RUN = "run"
    BUILD = "build"
    TEST = "test"
    SEED = "seed"
    RUN_OPERATION = "run-operation"
    LIST = "list"
    SNAPSHOT = "snapshot"


@dataclass
class DbtConfiguration:
    """The configuration for dbt-core."""

    project_dir: str = DEFAULT_PROJECT_DIR
    profiles_dir: str = DEFAULT_PROFILES_DIR
    target: Optional[str] = None
    threads: int = 1
    single_threaded: bool = True
    _vars: str = "{}"
    # Mutes unwanted dbt output
    quiet: bool = True
    # We need single threaded, simple, jinja parsing -- no rust/pickling
    use_experimental_parser: bool = False
    static_parser: bool = False
    partial_parse: bool = False
    # required attributes for dbt, not used by our interface
    dependencies: List[str] = field(default_factory=list)
    REQUIRE_RESOURCE_NAMES_WITHOUT_SPACES: bool = field(default_factory=bool)
    which: str = "blah"

    def __post_init__(self) -> None:
        """Post init hook to set single_threaded and remove target if not provided."""
        if self.target is None:
            del self.target
        self.single_threaded = self.threads == 1

    @property
    def profile(self) -> str:
        """Access the profiles_dir attribute as a string."""
        return None

    @property
    def vars(self) -> str:
        """Access the vars attribute as a string."""
        return self._vars

    @vars.setter
    def vars(self, v: Union[str, Dict[str, Any]]) -> None:
        """Set the vars attribute as a string or dict.

        If dict then it will be converted to a string which is what dbt expects.
        """
        if (__dbt_major_version__, __dbt_minor_version__) >= (1, 5):
            if isinstance(v, str):
                v = json.loads(v)
        else:
            if isinstance(v, dict):
                v = json.dumps(v)
        self._vars = v


class DbtManifestProxy(UserDict):  # type: ignore
    """Proxy for manifest dictionary object.

    If we need mutation then we should create a copy of the dict or interface with the dbt-core manifest object instead.
    """

    def _readonly(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("Cannot modify DbtManifestProxy")

    __setitem__ = _readonly
    __delitem__ = _readonly
    pop = _readonly  # type: ignore
    popitem = _readonly  # type: ignore
    clear = _readonly
    update = _readonly  # type: ignore
    setdefault = _readonly  # type: ignore


@dataclass
class DbtAdapterExecutionResult:
    """Interface for execution results.

    This keeps us 1 layer removed from dbt interfaces which may change.
    """

    adapter_response: "AdapterResponse"
    table: "Table"
    raw_code: str
    compiled_code: str


@dataclass
class DbtAdapterCompilationResult:
    """Interface for compilation results.

    This keeps us 1 layer removed from dbt interfaces which may change.
    """

    raw_code: str
    compiled_code: str
    node: "ManifestNode"
    injected_code: Optional[str] = None


class DbtTaskConfiguration:
    """A container for task configuration with sane defaults.

    Users should enforce an interface for their tasks via a factory method that returns an instance of this class.
    """

    def __init__(self, profile: str, target: str, **kwargs: Any) -> None:
        """Initialize the task configuration."""
        self.profile: str = profile
        self.target: str = target
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.threads: int = kwargs.get("threads", 1)
        self.single_threaded: bool = kwargs.get("single_threaded", self.threads == 1)
        self.state_id: Optional[str] = kwargs.get("state_id")
        self.version_check: bool = kwargs.get("version_check", False)
        self.resource_types: Optional[List[str]] = kwargs.get("resource_types")
        self.models: Union[None, str, List[str]] = kwargs.get("models")
        self.select: Union[None, str, List[str]] = kwargs.get("select")
        self.exclude: Union[None, str, List[str]] = kwargs.get("exclude")
        self.selector_name: Optional[str] = kwargs.get("selector_name")
        self.state: Optional[str] = kwargs.get("state")
        self.defer: bool = kwargs.get("defer", False)
        self.fail_fast: bool = kwargs.get("fail_fast", False)
        self.full_refresh: bool = kwargs.get("full_refresh", False)
        self.store_failures: bool = kwargs.get("store_failures", False)
        self.indirect_selection: bool = kwargs.get("indirect_selection", "eager")
        self.data: bool = kwargs.get("data", False)
        self.schema: bool = kwargs.get("schema", False)
        self.show: bool = kwargs.get("show", False)
        self.output: str = kwargs.get("output", "name")
        self.output_keys: Union[None, str, List[str]] = kwargs.get("output_keys")
        self.macro: Optional[str] = kwargs.get("macro")
        self.args: str = kwargs.get("args", "{}")
        self.quiet: bool = kwargs.get("quiet", True)

    def __getattribute__(self, __name: str) -> Any:
        """ "Force all attribute access to be lower case."""
        return object.__getattribute__(self, __name.lower())

    def __getattr__(self, name: str) -> Any:
        """Get an attribute from the kwargs if it does not exist on the class.

        This is useful for passing through arbitrary arguments to dbt while still
        being able to manage some semblance of a sane interface with defaults.
        """
        return self.kwargs.get(name)

    @classmethod
    def from_runtime_config(cls, config: RuntimeConfig, **kwargs: Any) -> "DbtTaskConfiguration":
        """Create a task configuration container from a DbtProject's runtime config.

        This is a good example of where static typing is not necessary. Developers can just
        pass in whatever they want and it will be passed through to the task configuration container.
        Users of the library are free to pass in any mapping derived from their own implementation for
        their own custom task.
        """
        threads = kwargs.pop("threads", config.threads)
        kwargs.pop("single_threaded", None)  # This is a derived property
        return cls(
            config.profile_name,
            config.target_name,
            threads=threads,
            single_threaded=threads == 1,
            **kwargs,
        )


class DbtProject:
    """Container for a dbt project.

    The dbt attribute is the primary interface for dbt-core. The adapter attribute is the primary interface for the dbt adapter.
    """

    ADAPTER_TTL = 3600

    def __init__(
        self,
        target: Optional[str] = None,
        profiles_dir: str = DEFAULT_PROFILES_DIR,
        project_dir: str = DEFAULT_PROJECT_DIR,
        threads: int = 1,
        vars: str = "{}",
    ) -> None:
        """Initialize the DbtProject."""
        self.base_config = DbtConfiguration(
            threads=threads,
            target=target,
            profiles_dir=profiles_dir or DEFAULT_PROFILES_DIR,
            project_dir=project_dir or DEFAULT_PROJECT_DIR,
        )
        self.base_config.vars = vars

        # Mutexes
        self.adapter_mutex = threading.Lock()
        self.parsing_mutex = threading.Lock()
        self.manifest_mutation_mutex = threading.Lock()

        # First time initialization
        self.parse_project(init=True)

        # Utilities
        self._sql_parser: Optional[SqlBlockParser] = None
        self._macro_parser: Optional[SqlMacroParser] = None

    @classmethod
    def from_config(cls, config: DbtConfiguration) -> "DbtProject":
        """Instatiate the DbtProject directly from a DbtConfiguration instance."""
        return cls(
            target=config.target,
            profiles_dir=config.profiles_dir,
            project_dir=config.project_dir,
            threads=config.threads,
        )

    def get_adapter_cls(self) -> "BaseAdapter":
        """Get the adapter class associated with the dbt profile."""
        return get_adapter_class_by_name(self.config.credentials.type)

    def initialize_adapter(self) -> None:
        """Initialize a dbt adapter."""
        if hasattr(self, "_adapter"):
            # Clean up any existing connections, err on the side of runtime
            # resiliency, don't let this fail. Maybe there is a world where
            # it really matters, but I don't think so. Someone can make the case.
            try:
                self._adapter.connections.cleanup_all()
            except Exception as e:
                LOGGER.debug(f"Failed to cleanup adapter connections: {e}")
        # The adapter.setter verifies connection, resets TTL, and updates adapter ref on config
        # this is thread safe by virtue of the adapter_mutex on the adapter.setter
        if (__dbt_major_version__, __dbt_minor_version__) < (1, 8):
            self.adapter = self.get_adapter_cls()(self.config)
        else:
            # from dbt 1.8 adapter decoupling onwwards,
            # instantiating an Adapter requires a multiprocessing context.
            self.adapter = self.get_adapter_cls()(self.config, get_context("spawn"))

    @property
    def adapter(self) -> "BaseAdapter":
        """dbt-core adapter with TTL and automatic reinstantiation.

        This supports long running processes that may have their connection to the database terminated by
        the database server. It is transparent to the user.
        """
        if time.time() - self._adapter_created_at > self.ADAPTER_TTL:
            self.initialize_adapter()
        return self._adapter

    @adapter.setter
    def adapter(self, adapter: "BaseAdapter") -> None:
        """Verify connection and reset TTL on adapter set, update adapter prop ref on config."""
        # Ensure safe concurrent access to the adapter
        # Currently we choose to drop attempted mutations while an existing mutation is in progress
        # This is a tradeoff between safety and performance, we could also choose to block
        if self.adapter_mutex.acquire(blocking=False):
            try:
                self._adapter = adapter
                self._adapter.connections.set_connection_name()
                self._adapter_created_at = time.time()
                self.config.adapter = self.adapter  # type: ignore
            finally:
                self.adapter_mutex.release()

    def parse_project(self, init: bool = False) -> None:
        """Parse project on disk.

        Uses the config from `DbtConfiguration` in args attribute, verifies connection to adapters database,
        mutates config, adapter, and dbt attributes. Thread-safe. From an efficiency perspective, this is a
        relatively expensive operation, so we want to avoid doing it more than necessary.
        """
        # Threads will wait here if another thread is parsing the project
        # however, it probably makes sense to not parse the project once the waiter
        # has acquired the lock, TODO: Lets implement a debounce-like buffer here
        with self.parsing_mutex:
            if init:
                if (__dbt_major_version__, __dbt_minor_version__) >= (1, 8):
                    from dbt_common.clients.system import get_env
                    from dbt_common.context import set_invocation_context

                    set_invocation_context(get_env())

                set_from_args(self.base_config, self.base_config)
                # We can think of `RuntimeConfig` as a dbt-core "context" object
                # where a `Project` meets a `Profile` and is a superset of them both
                self.config = RuntimeConfig.from_args(self.base_config)
                self.initialize_adapter()

            _project_parser = ManifestLoader(
                self.config,
                self.config.load_dependencies(),
                self.adapter.connections.set_query_header,
            )
            if (__dbt_major_version__, __dbt_minor_version__) >= (1, 8):
                from dbt.context.providers import generate_runtime_macro_context

                macros_manifest = _project_parser.load_macros(
                    self.config, self.adapter.connections.set_query_header
                )
                self.adapter.set_macro_resolver(macros_manifest)
                self.adapter.set_macro_context_generator(generate_runtime_macro_context)
            self.manifest = _project_parser.load()
            self.manifest.build_flat_graph()
            _project_parser.save_macros_to_adapter(self.adapter)

            self._sql_parser = None
            self._macro_parser = None

    def safe_parse_project(self, reinit: bool = False) -> None:
        """Safe version of parse_project that will not mutate the config if parsing fails."""
        if reinit:
            self.clear_internal_caches()
        _config_pointer = copy(self.config)
        try:
            self.parse_project(init=reinit)
        except Exception as parse_error:
            self.config = _config_pointer
            raise parse_error
        self.write_manifest_artifact()

    def _verify_connection(self, adapter: "BaseAdapter") -> "BaseAdapter":
        """Verification for adapter + profile.

        Used as a passthrough, this also seeds the master connection.
        """
        try:
            adapter.connections.set_connection_name()
            adapter.debug_query()
        except Exception as query_exc:
            raise RuntimeError("Could not connect to Database") from query_exc
        else:
            return adapter

    def adapter_probe(self) -> bool:
        """Check adapter connection, useful for long running procsesses."""
        if not hasattr(self, "adapter") or self.adapter is None:
            return False
        try:
            with self.adapter.connection_named("osmosis-heartbeat"):
                self.adapter.debug_query()
        except Exception:
            # TODO: Should we preemptively reinitialize the adapter here? or leave it to userland to handle?
            return False
        return True

    def fn_threaded_conn(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Callable[..., T]:
        """For jobs which are intended to be submitted to a thread pool."""

        @wraps(fn)
        def _with_conn() -> T:
            self.adapter.connections.set_connection_name()
            return fn(*args, **kwargs)

        return _with_conn

    def generate_runtime_model_context(self, node: "ManifestNode") -> Dict[str, Any]:
        """Wrap dbt context provider."""
        # Purposefully deferred due to its many dependencies
        from dbt.context.providers import generate_runtime_model_context

        return generate_runtime_model_context(node, self.config, self.manifest)

    @property
    def project_name(self) -> str:
        """dbt project name."""
        return self.config.project_name

    @property
    def project_root(self) -> str:
        """dbt project root."""
        return self.config.project_root

    @property
    def manifest_dict(self) -> DbtManifestProxy:
        """dbt manifest dict."""
        return DbtManifestProxy(self.manifest.flat_graph)

    def write_manifest_artifact(self) -> None:
        """Write a manifest.json to disk.

        Because our project is in memory, this is useful for integrating with other tools that
        expect a manifest.json to be present in the target directory.
        """
        artifact_path = os.path.join(
            self.config.project_root, self.config.target_path, "manifest.json"
        )
        self.manifest.write(artifact_path)

    def clear_internal_caches(self) -> None:
        """Clear least recently used caches and reinstantiable container objects."""
        self.compile_code.cache_clear()
        self.unsafe_compile_code.cache_clear()

    def get_ref_node(self, target_model_name: str) -> "ManifestNode":
        """Get a `ManifestNode` from a dbt project model name.

        This is the same as one would in a {{ ref(...) }} macro call.
        """
        return cast(
            "ManifestNode",
            self.manifest.resolve_ref(
                target_model_name=target_model_name,
                target_model_package=None,
                current_project=self.config.project_name,
                node_package=self.config.project_name,
            ),
        )

    def get_source_node(self, target_source_name: str, target_table_name: str) -> "ManifestNode":
        """Get a `ManifestNode` from a dbt project source name and table name.

        This is the same as one would in a {{ source(...) }} macro call.
        """
        return cast(
            "ManifestNode",
            self.manifest.resolve_source(
                target_source_name=target_source_name,
                target_table_name=target_table_name,
                current_project=self.config.project_name,
                node_package=self.config.project_name,
            ),
        )

    def get_node_by_path(self, path: str) -> Optional["ManifestNode"]:
        """Find an existing node given relative file path.

        TODO: We can include Path obj support and make this more robust.
        """
        for node in self.manifest.nodes.values():
            if node.original_file_path == path:
                return node
        return None

    @contextmanager
    def generate_server_node(
        self, sql: str, node_name: str = "anonymous_node"
    ) -> Generator["ManifestNode", None, None]:
        """Get a transient node for SQL execution against adapter.

        This is a context manager that will clear the node after execution and leverages a mutex during manifest mutation.
        """
        # Remove {% ... %} patterns from the SQL string
        sql = re.sub(r"{%.*?%}", "", sql, flags=re.DOTALL)
        with self.manifest_mutation_mutex:
            self._clear_node(node_name)
            sql_node = self.sql_parser.parse_remote(sql, node_name)
            process_node(self.config, self.manifest, sql_node)
            yield sql_node
            self._clear_node(node_name)

    def unsafe_generate_server_node(
        self, sql: str, node_name: str = "anonymous_node"
    ) -> "ManifestNode":
        """Get a transient node for SQL execution against adapter.

        This is faster than `generate_server_node` but does not clear the node after execution.
        That is left to the caller. It is also not thread safe in and of itself and requires the caller to
        manage jitter or mutexes.
        """
        self._clear_node(node_name)
        sql_node = self.sql_parser.parse_remote(sql, node_name)
        process_node(self.config, self.manifest, sql_node)
        return sql_node

    def inject_macro(self, macro_contents: str) -> None:
        """Inject a macro into the project.

        This is useful for testing macros in isolation. It offers unique ways to integrate with dbt.
        """
        macro_overrides = {}
        for node in self.macro_parser.parse_remote(macro_contents):
            macro_overrides[node.unique_id] = node
        self.manifest.macros.update(macro_overrides)

    def get_macro_function(self, macro_name: str) -> Callable[..., Any]:
        """Get macro as a function which behaves like a Python function."""

        def _macro_fn(**kwargs: Any) -> Any:
            return self.adapter.execute_macro(macro_name, self.manifest, kwargs=kwargs)

        return _macro_fn

    def execute_macro(self, macro: str, **kwargs: Any) -> Any:
        """Wrap adapter execute_macro. Execute a macro like a python function."""
        return self.get_macro_function(macro)(**kwargs)

    def adapter_execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple["AdapterResponse", "Table"]:
        """Wrap adapter.execute. Execute SQL against database.

        This is more on-the-rails than `execute_code` which intelligently handles jinja compilation provides a proxy result.
        """
        return cast(
            Tuple["AdapterResponse", "Table"],
            self.adapter.execute(sql, auto_begin, fetch),
        )

    def execute_code(self, raw_code: str) -> DbtAdapterExecutionResult:
        """Execute dbt SQL statement against database.

        This is a proxy for `adapter_execute` and the the recommended method for executing SQL against the database.
        """
        # If no jinja chars then these are synonymous
        compiled_code = str(raw_code)
        if has_jinja(raw_code):
            # Jinja found, compile it
            compiled_code = self.compile_code(raw_code).compiled_code
        return DbtAdapterExecutionResult(
            *self.adapter_execute(compiled_code, fetch=True),
            raw_code,
            compiled_code,
        )

    def execute_from_node(self, node: "ManifestNode") -> DbtAdapterExecutionResult:
        """Execute dbt SQL statement against database from a "ManifestNode"."""
        raw_code: str = getattr(node, RAW_CODE)
        compiled_code: Optional[str] = getattr(node, COMPILED_CODE, None)
        if compiled_code:
            # Node is compiled, execute the SQL
            return self.execute_code(compiled_code)
        # Node not compiled
        if has_jinja(raw_code):
            # Node has jinja in its SQL, compile it
            compiled_code = self.compile_from_node(node).compiled_code
        # Execute the SQL
        return self.execute_code(compiled_code or raw_code)

    @lru_cache(maxsize=100)  # noqa: B019
    def compile_code(self, raw_code: str) -> DbtAdapterCompilationResult:
        """Create a node with `generate_server_node` method. Compile generated node.

        Has a retry built in because even uuidv4 cannot gaurantee uniqueness at the speed
        in which we can call this function concurrently. A retry significantly increases the stability.
        """
        temp_node_id = str(uuid.uuid4())
        with self.generate_server_node(raw_code, temp_node_id) as node:
            return self.compile_from_node(node)

    @lru_cache(maxsize=100)  # noqa: B019
    def unsafe_compile_code(self, raw_code: str, retry: int = 3) -> DbtAdapterCompilationResult:
        """Create a node with `unsafe_generate_server_node` method. Compiles the generated node.

        Has a retry built in because even uuid4 cannot gaurantee uniqueness at the speed
        in which we can call this function concurrently. A retry significantly increases the
        stability. This is certainly the fastest way to compile SQL but it is yet to be benchmarked.
        """
        temp_node_id = str(uuid.uuid4())
        try:
            node = self.compile_from_node(self.unsafe_generate_server_node(raw_code, temp_node_id))
        except Exception as compilation_error:
            if retry > 0:
                return self.compile_code(raw_code, retry - 1)
            raise compilation_error
        else:
            return node
        finally:
            self._clear_node(temp_node_id)

    def compile_from_node(self, node: "ManifestNode") -> DbtAdapterCompilationResult:
        """Compiles existing node. ALL compilation passes through this code path.

        Raw SQL is marshalled by the caller into a mock node before being passed into this method.
        Existing nodes can be passed in here directly.
        """
        compiled_node = SqlCompileRunner(
            self.config, self.adapter, node=node, node_index=1, num_nodes=1
        ).compile(self.manifest)
        return DbtAdapterCompilationResult(
            getattr(compiled_node, RAW_CODE),
            getattr(compiled_node, COMPILED_CODE),
            compiled_node,
        )

    def _clear_node(self, name: str = "anonymous_node") -> None:
        """Clear remote node from dbt project."""
        self.manifest.nodes.pop(f"{NodeType.SqlOperation}.{self.project_name}.{name}", None)

    def get_relation(self, database: str, schema: str, name: str) -> Optional["BaseRelation"]:
        """Wrap for `adapter.get_relation`."""
        return self.adapter.get_relation(database, schema, name)

    def relation_exists(self, database: str, schema: str, name: str) -> bool:
        """Interface for checking if a relation exists in the database."""
        return self.adapter.get_relation(database, schema, name) is not None

    def node_exists(self, node: "ManifestNode") -> bool:
        """Interface for checking if a node exists in the database."""
        return self.adapter.get_relation(self.create_relation_from_node(node)) is not None

    def create_relation(self, database: str, schema: str, name: str) -> "BaseRelation":
        """Wrap `adapter.Relation.create`."""
        return self.adapter.Relation.create(database, schema, name)

    def create_relation_from_node(self, node: "ManifestNode") -> "BaseRelation":
        """Wrap `adapter.Relation.create_from`."""
        return self.adapter.Relation.create_from(self.config, node)

    def get_or_create_relation(
        self, database: str, schema: str, name: str
    ) -> Tuple["BaseRelation", bool]:
        """Get relation or create if not exists.

        Returns tuple of relation and boolean result of whether it existed ie: (relation, did_exist).
        """
        ref = self.get_relation(database, schema, name)
        return (
            (ref, True)
            if ref is not None
            else (self.create_relation(database, schema, name), False)
        )

    def create_schema(self, node: "ManifestNode") -> None:
        """Create a schema in the database leveraging dbt-core's builtin macro."""
        self.execute_macro(
            "create_schema",
            kwargs={"relation": self.create_relation_from_node(node)},
        )

    def get_columns_in_node(self, node: "ManifestNode") -> List[Any]:
        """Wrap `adapter.get_columns_in_relation`."""
        return (self.adapter.get_columns_in_relation(self.create_relation_from_node(node)),)

    def get_columns(self, node: "ManifestNode") -> List[str]:
        """Get a list of columns from a compiled node."""
        columns = []
        try:
            columns.extend([c.name for c in self.get_columns_in_node(node)])
        except Exception:
            # TODO: does this fallback make sense?
            original_sql = str(getattr(node, RAW_CODE))
            setattr(node, RAW_CODE, f"SELECT * FROM ({original_sql}) WHERE 1=0")
            result = self.execute_from_node(node)
            setattr(node, RAW_CODE, original_sql), delattr(node, COMPILED_CODE)
            node.compiled = False
            columns.extend(result.table.column_names)
        return columns

    def materialize(
        self, node: "ManifestNode", temporary: bool = True
    ) -> Tuple["AdapterResponse", None]:
        """Materialize a table in the database.

        TODO: This is not fully baked. The API is stable but the implementation is not.
        """
        return self.adapter_execute(
            # Returns CTAS string so send to adapter.execute
            self.execute_macro(
                "create_table_as",
                kwargs={
                    "sql": getattr(node, COMPILED_CODE),
                    "relation": self.create_relation_from_node(node),
                    "temporary": temporary,
                },
            ),
            auto_begin=True,
        )

    @property
    def sql_parser(self) -> SqlBlockParser:
        """A dbt-core SQL parser capable of parsing and adding nodes to the manifest via `parse_remote` which will also return the added node to the caller.

        Note that post-parsing this still typically requires calls to `_process_nodes_for_ref`
        and `_process_sources_for_ref` from the `dbt.parser.manifest` module in order to compile.
        We have higher level methods that handle this for you.
        """
        if self._sql_parser is None:
            self._sql_parser = SqlBlockParser(self.config, self.manifest, self.config)
        return self._sql_parser

    @property
    def macro_parser(self) -> SqlMacroParser:
        """A dbt-core macro parser. Parse macros with `parse_remote` and add them to the manifest.

        We have a higher level method `inject_macro` that handles this for you.
        """
        if self._macro_parser is None:
            self._macro_parser = SqlMacroParser(self.config, self.manifest)
        return self._macro_parser

    def get_task_config(self, **kwargs) -> DbtTaskConfiguration:
        """Get a dbt-core task configuration."""
        threads = kwargs.pop("threads", self.config.threads)
        return DbtTaskConfiguration.from_runtime_config(
            config=self.config, threads=threads, **kwargs
        )

    def get_task_cls(self, typ: DbtCommand) -> Type["ManifestTask"]:
        """Get a dbt-core task class by type.

        This could be overridden to add custom tasks such as linting, etc.
        so long as they are subclasses of `GraphRunnableTask`.
        """
        # These are purposefully deferred imports
        from dbt.task.build import BuildTask
        from dbt.task.list import ListTask
        from dbt.task.run import RunTask
        from dbt.task.run_operation import RunOperationTask
        from dbt.task.seed import SeedTask
        from dbt.task.snapshot import SnapshotTask
        from dbt.task.test import TestTask

        return {
            DbtCommand.RUN: RunTask,
            DbtCommand.BUILD: BuildTask,
            DbtCommand.TEST: TestTask,
            DbtCommand.SEED: SeedTask,
            DbtCommand.LIST: ListTask,
            DbtCommand.SNAPSHOT: SnapshotTask,
            DbtCommand.RUN_OPERATION: RunOperationTask,
        }[typ]

    def get_task(self, typ: DbtCommand, args: DbtTaskConfiguration) -> "ManifestTask":
        """Get a dbt-core task by type."""
        if (__dbt_major_version__, __dbt_minor_version__) < (1, 5):
            task = self.get_task_cls(typ)(args, self.config)
            # Render this a no-op on this class instance so that the tasks `run`
            # method plumbing will defer to our existing in memory manifest.
            task.load_manifest = lambda *args, **kwargs: None  # type: ignore
            task.manifest = self.manifest
        else:
            task = self.get_task_cls(typ)(args, self.config, self.manifest)
        return task

    def list(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        **kwargs: Dict[str, Any],
    ) -> "ExecutionResult":
        """List resources in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        with redirect_stdout(None):
            return self.get_task(  # type: ignore
                DbtCommand.LIST,
                self.get_task_config(select=select, exclude=exclude, **kwargs),
            ).run()

    def run(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        **kwargs: Dict[str, Any],
    ) -> "RunExecutionResult":
        """Run models in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        with redirect_stdout(None):
            return cast(
                "RunExecutionResult",
                self.get_task(
                    DbtCommand.RUN,
                    self.get_task_config(select=select, exclude=exclude, **kwargs),
                ).run(),
            )

    def test(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        **kwargs: Dict[str, Any],
    ) -> "ExecutionResult":
        """Test models in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        with redirect_stdout(None):
            return self.get_task(  # type: ignore
                DbtCommand.TEST,
                self.get_task_config(select=select, exclude=exclude, **kwargs),
            ).run()

    def build(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        **kwargs: Dict[str, Any],
    ) -> "ExecutionResult":
        """Build resources in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        with redirect_stdout(None):
            return self.get_task(
                DbtCommand.BUILD,
                self.get_task_config(select=select, exclude=exclude, **kwargs),
            ).run()


def marshall_selection_args(
    select: Optional[Union[str, List[str]]] = None,
    exclude: Optional[Union[str, List[str], None]] = None,
) -> Tuple[Union[str, List[str]], Union[str, List[str]]]:
    """Marshall selection arguments to a list of strings."""
    if select is None:
        select = []
    if exclude is None:
        exclude = []
    if isinstance(select, (tuple, set, frozenset)):
        select = list(select)
    if isinstance(exclude, (tuple, set, frozenset)):
        exclude = list(exclude)
    # Permit standalone strings such as "my_model+ @some_other_model"
    # as well as lists of strings such as ["my_model+", "@some_other_model"]
    if not isinstance(select, list):
        select = [select]
    if not isinstance(exclude, list):
        exclude = [exclude]
    return select, exclude


class DbtProjectContainer:
    """Manages multiple DbtProjects.

    A DbtProject corresponds to a single project. This interface is used
    dbt projects in a single process. It enables basic multitenant servers.
    """

    def __init__(self) -> None:
        """Initialize the container."""
        self._projects: Dict[str, DbtProject] = OrderedDict()
        self._default_project: Optional[str] = None
        if dci_state is not None:
            assert dci_state.dbt_project_container is None
            dci_state.dbt_project_container = self

    def get_project(self, project_name: str) -> Optional[DbtProject]:
        """Primary interface to get a project and execute code."""
        return self._projects.get(project_name)

    def get_project_by_root_dir(self, root_dir: str) -> Optional[DbtProject]:
        """Get a project by its root directory."""
        root_dir = os.path.abspath(os.path.normpath(root_dir))
        for project in self._projects.values():
            if os.path.abspath(project.project_root) == root_dir:
                return project
        return None

    def get_default_project(self) -> Optional[DbtProject]:
        """Get the default project which is the earliest project inserted into the container."""
        default_project = self._default_project
        if not default_project:
            return None
        return self._projects.get(default_project)

    def add_project(
        self,
        target: Optional[str] = None,
        profiles_dir: str = DEFAULT_PROFILES_DIR,
        project_dir: Optional[str] = None,
        threads: int = 1,
        vars: str = "{}",
        name_override: str = "",
    ) -> DbtProject:
        """Add a DbtProject with arguments."""
        project = DbtProject(target, profiles_dir, project_dir, threads, vars)
        project_name = name_override or project.config.project_name
        if self._default_project is None:
            self._default_project = project_name
        self._projects[project_name] = project
        return project

    def add_parsed_project(self, project: DbtProject) -> DbtProject:
        """Add an already instantiated DbtProject."""
        self._projects.setdefault(project.config.project_name, project)
        if self._default_project is None:
            self._default_project = project.config.project_name
        return project

    def add_project_from_args(self, config: DbtConfiguration) -> DbtProject:
        """Add a DbtProject from a DbtConfiguration."""
        project = DbtProject.from_config(config)
        self._projects.setdefault(project.config.project_name, project)
        return project

    def drop_project(self, project_name: str) -> None:
        """Drop a DbtProject."""
        project = self.get_project(project_name)
        if project is None:
            return
        # Encourage garbage collection
        project.clear_internal_caches()
        project.adapter.connections.cleanup_all()
        self._projects.pop(project_name)
        if self._default_project == project_name:
            if len(self) > 0:
                self._default_project = list(self._projects.keys())[0]
            else:
                self._default_project = None

    def drop_all_projects(self) -> None:
        """Drop all DbtProject's in the container."""
        self._default_project = None
        for project in self._projects:
            self.drop_project(project)

    def reparse_all_projects(self) -> None:
        """Reparse all projects."""
        for project in self:
            project.safe_parse_project()

    def registered_projects(self) -> List[str]:
        """Grab all registered project names."""
        return list(self._projects.keys())

    def __len__(self) -> int:
        """Allow len(DbtProjectContainer)."""
        return len(self._projects)

    def __getitem__(self, project: str) -> DbtProject:
        """Allow DbtProjectContainer['jaffle_shop']."""
        maybe_project = self.get_project(project)
        if maybe_project is None:
            raise KeyError(project)
        return maybe_project

    def __setitem__(self, name: str, project: DbtProject) -> None:
        """Allow DbtProjectContainer['jaffle_shop'] = DbtProject."""
        if self._default_project is None:
            self._default_project = name
        self._projects[name] = project

    def __delitem__(self, project: str) -> None:
        """Allow del DbtProjectContainer['jaffle_shop']."""
        self.drop_project(project)

    def __iter__(self) -> Generator[DbtProject, None, None]:
        """Allow project for project in DbtProjectContainer."""
        for project in self._projects:
            maybe_project = self.get_project(project)
            if maybe_project is None:
                continue
            yield maybe_project

    def __contains__(self, project: str) -> bool:
        """Allow 'jaffle_shop' in DbtProjectContainer."""
        return project in self._projects

    def __repr__(self) -> str:
        """Canonical string representation of DbtProjectContainer instance."""
        return "\n".join(
            f"Project: {project.project_name}, Dir: {project.project_root}" for project in self
        )


def has_jinja(query: str) -> bool:
    """Check if a query contains any Jinja control sequences."""
    return any(seq in query for seq in JINJA_CONTROL_SEQUENCES)


def semvar_to_tuple(semvar: "VersionSpecifier") -> Tuple[int, int, int]:
    """Convert a semvar to a tuple of ints."""
    return (int(semvar.major or 0), int(semvar.minor or 0), int(semvar.patch or 0))


# endregion

# region: dbt-core-interface server

SERVER_MUTEX = threading.Lock()


@dataclass
class ServerRunResult:
    """The result of running a query."""

    column_names: List[str]
    rows: List[List[Any]]
    raw_code: str
    executed_code: str


@dataclass
class ServerCompileResult:
    """The result of compiling a project."""

    result: str


@dataclass
class ServerResetResult:
    """The result of resetting the Server database."""

    result: str


@dataclass
class ServerRegisterResult:
    """The result of registering a project."""

    added: str
    projects: List[str]


@dataclass
class ServerUnregisterResult:
    """The result of unregistering a project."""

    removed: str
    projects: List[str]


class ServerErrorCode(Enum):
    """The error codes that can be returned by the Server API."""

    FailedToReachServer = -1
    CompileSqlFailure = 1
    ExecuteSqlFailure = 2
    ProjectParseFailure = 3
    ProjectNotRegistered = 4
    ProjectHeaderNotSupplied = 5
    MissingRequiredParams = 6


@dataclass
class ServerError:
    """An error that can be serialized to JSON."""

    code: ServerErrorCode
    message: str
    data: Dict[str, Any]


@dataclass
class ServerErrorContainer:
    """A container for an ServerError that can be serialized to JSON."""

    error: ServerError


def server_serializer(o):
    """Encode JSON. Handles server-specific types."""
    if isinstance(o, decimal.Decimal):
        return float(o)
    if isinstance(o, ServerErrorCode):
        return o.value
    return str(o)


def remove_comments(string) -> str:  # noqa: C901
    """Remove comments from a string."""
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|//[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else:  # otherwise, we will return the 1st group
            return match.group(1)  # captured quoted-string

    multiline_comments_removed = regex.sub(_replacer, string) + "\n"
    output = ""
    for line in multiline_comments_removed.splitlines(keepends=True):
        if line.strip().startswith("--"):
            continue
        s_quote_c = 0
        d_quote_c = 0
        cmt_dash = 0
        split_ix = -1
        for i, c in enumerate(line):
            if cmt_dash >= 2:
                # found 2 sequential dashes, split here
                split_ix = i - cmt_dash
                break
            if c == '"':
                # inc quote count
                d_quote_c += 1
            elif c == "'":
                # inc quote count
                s_quote_c += 1
            elif c == "-" and d_quote_c % 2 == 0 and s_quote_c % 2 == 0:
                # dash and not in a quote, inc dash count
                cmt_dash += 1
                continue
            # reset dash count each iteration
            cmt_dash = 0
        if split_ix > 0:
            output += line[:split_ix] + "\n"
        else:
            output += line
    return "".join(output)


class DbtInterfaceServerPlugin:
    """Used to inject the dbt-core-interface runner into the request context."""

    name = "dbt-interface-server"
    api = 2

    def __init__(self, runner: Optional[DbtProject] = None):
        """Initialize the plugin with the runner to inject into the request context."""
        self.runners = DbtProjectContainer()
        if runner:
            self.runners.add_parsed_project(runner)

    def apply(self, callback, route):
        """Apply the plugin to the route callback."""

        def wrapper(*args, **kwargs):
            start = time.time()
            body = callback(*args, **kwargs, runners=self.runners)
            end = time.time()
            response.headers["X-dbt-Exec-Time"] = str(end - start)
            return body

        return wrapper


@route("/run", method="POST")
def run_sql(runners: DbtProjectContainer) -> Union[ServerRunResult, ServerErrorContainer, str]:
    """Run SQL against a dbt project."""
    # Project Support
    project_runner = (
        runners.get_project(request.get_header("X-dbt-Project")) or runners.get_default_project()
    )
    if not project_runner:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=(
                        "Project is not registered. Make a POST request to the /register endpoint"
                        " first to register a runner"
                    ),
                    data={"registered_projects": runners.registered_projects()},
                )
            )
        )

    # Query Construction
    query = remove_comments(request.body.read().decode("utf-8"))
    limit = request.query.get("limit", 200)
    query_with_limit = (
        # we need to support `TOP` too
        f"select * from ({query}) as __server_query limit {limit}"
    )

    try:
        run_fn = project_runner.fn_threaded_conn(project_runner.execute_code, query_with_limit)
        result = run_fn()
    except Exception as execution_err:
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ExecuteSqlFailure,
                    message=str(execution_err),
                    data=execution_err.__dict__,
                )
            )
        )

    # Re-extract compiled query and return data structure
    compiled_query = re.search(
        r"select \* from \(([\w\W]+)\) as __server_query", result.compiled_code
    ).groups()[0]

    new_columns = []
    for column in result.table.columns:
        if isinstance(column.data_type, Integer):
            # Convert the column to text if it contains integer values
            converted_column = Column(
                column._index,
                column._name,
                Text(),
                [convert_int_to_str(value) for value in column._rows],
                column._keys,
            )
        else:
            converted_column = column
        new_columns.append(converted_column)
    new_table = Table(
        rows=new_columns[0]._rows,  # Assuming all columns have the same number of rows
        column_names=[column._name for column in new_columns],
        column_types=[column._data_type for column in new_columns],
        row_names=new_columns[0]._keys,
    )
    return asdict(
        ServerRunResult(
            rows=[list(row) for row in new_table.rows],
            column_names=new_table.column_names,
            executed_code=compiled_query.strip(),
            raw_code=query,
        )
    )


def convert_int_to_str(value):
    return str(value) if isinstance(value, int) else value


@route("/compile", method="POST")
def compile_sql(
    runners: DbtProjectContainer,
) -> Union[ServerCompileResult, ServerErrorContainer, str]:
    """Compiles a SQL query."""
    # Project Support
    project_runner = (
        runners.get_project(request.get_header("X-dbt-Project")) or runners.get_default_project()
    )
    if not project_runner:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=(
                        "Project is not registered. Make a POST request to the /register endpoint"
                        " first to register a runner"
                    ),
                    data={"registered_projects": runners.registered_projects()},
                )
            )
        )

    # Query Compilation
    query: str = request.body.read().decode("utf-8").strip()
    if has_jinja(query):
        try:
            compiled_query = project_runner.compile_code(query).compiled_code
        except Exception as compile_err:
            return asdict(
                ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.CompileSqlFailure,
                        message=str(compile_err),
                        data=compile_err.__dict__,
                    )
                )
            )
    else:
        compiled_query = query

    return asdict(ServerCompileResult(result=compiled_query))


@route(["/parse", "/reset"])
def reset(runners: DbtProjectContainer) -> Union[ServerResetResult, ServerErrorContainer, str]:
    """Reset the runner and clear the cache."""
    # Project Support
    project_runner = (
        runners.get_project(request.get_header("X-dbt-Project")) or runners.get_default_project()
    )
    if not project_runner:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=(
                        "Project is not registered. Make a POST request to the /register endpoint"
                        " first to register a runner"
                    ),
                    data={"registered_projects": runners.registered_projects()},
                )
            )
        )

    # Determines if we should clear caches and reset config before re-seeding runner
    reset = str(request.query.get("reset", "false")).lower() == "true"

    # Get targets
    old_target = getattr(project_runner.base_config, "target", project_runner.config.target_name)
    new_target = request.query.get("target", old_target)

    if not reset and old_target == new_target:
        # Async (target same)
        if SERVER_MUTEX.acquire(blocking=False):
            LOGGER.debug("Mutex locked")
            parse_job = threading.Thread(
                target=_reset, args=(project_runner, reset, old_target, new_target)
            )
            parse_job.start()
            return asdict(ServerResetResult(result="Initializing project parsing"))
        else:
            LOGGER.debug("Mutex is locked, reparse in progress")
            return asdict(ServerResetResult(result="Currently reparsing project"))
    else:
        # Sync (target changed or reset is true)
        if SERVER_MUTEX.acquire(blocking=old_target != new_target):
            LOGGER.debug("Mutex locked")
            return asdict(_reset(project_runner, reset, old_target, new_target))
        else:
            LOGGER.debug("Mutex is locked, reparse in progress")
            return asdict(ServerResetResult(result="Currently reparsing project"))


def _reset(
    runner: DbtProject, reset: bool, old_target: str, new_target: str
) -> Union[ServerResetResult, ServerErrorContainer]:
    """Reset the project runner.

    Can be called asynchronously or synchronously.
    """
    target_did_change = old_target != new_target
    try:
        runner.base_config.target = new_target
        LOGGER.debug("Starting reparse")
        runner.safe_parse_project(reinit=reset or target_did_change)
    except Exception as reparse_err:
        LOGGER.debug("Reparse error")
        runner.base_config.target = old_target
        rv = ServerErrorContainer(
            error=ServerError(
                code=ServerErrorCode.ProjectParseFailure,
                message=str(reparse_err),
                data=reparse_err.__dict__,
            )
        )
    else:
        LOGGER.debug("Reparse success")
        rv = ServerResetResult(
            result=(
                f"Profile target changed from {old_target} to {new_target}!"
                if target_did_change
                else f"Reparsed project with profile {old_target}!"
            )
        )
    finally:
        LOGGER.debug("Unlocking mutex")
        SERVER_MUTEX.release()
    return rv


@route("/register", method="POST")
def register(runners: DbtProjectContainer) -> Union[ServerResetResult, ServerErrorContainer, str]:
    """Register a new project runner."""
    # Project Support
    project = request.get_header("X-dbt-Project")
    if not project:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectHeaderNotSupplied,
                    message=(
                        "Project header `X-dbt-Project` was not supplied but is required for this"
                        " endpoint"
                    ),
                    data=dict(request.headers),
                )
            )
        )
    if project in runners:
        # Idempotent
        return asdict(ServerRegisterResult(added=project, projects=runners.registered_projects()))

    # Inputs
    kwargs = {}
    params = {}
    if request.query:
        params.update(dict(request.query.decode()))
    if request.json:
        params.update(dict(request.json))
    kwargs.setdefault("project_dir", params.get("project-dir") or params.get("project_dir"))
    kwargs.setdefault("profiles_dir", params.get("profiles-dir") or params.get("profiles_dir"))
    kwargs.setdefault("target", params.get("target"))
    for k in ("project_dir",):
        if kwargs.get(k) is None:
            response.status = 400
            return asdict(
                ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.MissingRequiredParams,
                        message=f"Missing required parameter {k}",
                        data=dict(request.headers),
                    )
                )
            )

    try:
        new_runner = DbtProject(**kwargs)
    except Exception as init_err:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectParseFailure,
                    message=str(init_err),
                    data=init_err.__dict__,
                )
            )
        )

    runners[project] = new_runner
    runners.add_parsed_project

    # If we got this far, then it means we were able to register.  Save
    # our registration state.
    with open("/tmp/savestate.json", "wt") as output:
        kwargs["project"] = project
        json.dump(kwargs, output)

    return asdict(ServerRegisterResult(added=project, projects=runners.registered_projects()))


@route("/unregister", method="POST")
def unregister(runners: DbtProjectContainer) -> Union[ServerResetResult, ServerErrorContainer, str]:
    """Unregister a project runner from the server."""
    # Project Support
    project = request.get_header("X-dbt-Project")
    if not project:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectHeaderNotSupplied,
                    message=(
                        "Project header `X-dbt-Project` was not supplied but is required for this"
                        " endpoint"
                    ),
                    data=dict(request.headers),
                )
            )
        )
    if project not in runners:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=(
                        "Project is not registered. Make a POST request to the /register endpoint"
                        " first to register a runner"
                    ),
                    data={"registered_projects": runners.registered_projects()},
                )
            )
        )
    runners.drop_project(project)
    return asdict(ServerUnregisterResult(removed=project, projects=runners.registered_projects()))


@route(["/health", "/api/health"], methods="GET")
def health_check(runners: DbtProjectContainer) -> dict:
    """Health check endpoint."""
    # Project Support
    project_runner = (
        runners.get_project(request.get_header("X-dbt-Project")) or runners.get_default_project()
    )
    if not project_runner:
        response.status = 400
        return asdict(
            ServerErrorContainer(
                error=ServerError(
                    code=ServerErrorCode.ProjectNotRegistered,
                    message=(
                        "Project is not registered. Make a POST request to the /register endpoint"
                        " first to register a runner"
                    ),
                    data={"registered_projects": runners.registered_projects()},
                )
            )
        )
    return {
        "result": {
            "status": "ready",
            "project_name": project_runner.config.project_name,
            "target_name": project_runner.config.target_name,
            "profile_name": project_runner.config.project_name,
            "logs": project_runner.config.log_path,
            "timestamp": str(datetime.utcnow()),
            "error": None,
        },
        "id": str(uuid.uuid4()),
        "dbt-interface-server": __name__,
    }


@route(["/heartbeat", "/api/heartbeat"], methods="GET")
def heart_beat(runners: DbtProjectContainer) -> dict:
    """Heart beat endpoint."""
    return {"result": {"status": "ready"}}


if lint_command:

    @route("/lint", method="POST")
    def lint_sql(
        runners: DbtProjectContainer,
    ):
        LOGGER.info(f"lint_sql()")
        # Project Support
        project_runner = (
            runners.get_project(request.get_header("X-dbt-Project"))
            or runners.get_default_project()
        )
        LOGGER.info(f"got project: {project_runner}")
        if not project_runner:
            response.status = 400
            return asdict(
                ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.ProjectNotRegistered,
                        message=(
                            "Project is not registered. Make a POST request to the /register"
                            " endpoint first to register a runner"
                        ),
                        data={"registered_projects": runners.registered_projects()},
                    )
                )
            )

        sql_path = request.query.get("sql_path")
        LOGGER.info(f"sql_path: {sql_path}")
        if sql_path:
            # Lint a file
            LOGGER.info(f"linting file: {sql_path}")
            sql = Path(sql_path)
        else:
            # Lint a string
            LOGGER.info(f"linting string")
            sql = request.body.getvalue().decode("utf-8")
        if not sql:
            response.status = 400
            return {
                "error": {
                    "data": {},
                    "message": (
                        "No SQL provided. Either provide a SQL file path or a SQL string to lint."
                    ),
                }
            }
        try:
            LOGGER.info(f"Calling lint_command()")
            temp_result = lint_command(
                Path(project_runner.config.project_root),
                sql=sql,
                extra_config_path=(
                    Path(request.query.get("extra_config_path"))
                    if request.query.get("extra_config_path")
                    else None
                ),
            )
            result = temp_result["violations"] if temp_result is not None else []
        except Exception as lint_err:
            logging.exception("Linting failed")
            response.status = 500
            return {
                "error": {
                    "data": {},
                    "message": str(lint_err),
                }
            }
        else:
            LOGGER.info(f"Linting succeeded")
            lint_result = {"result": [error for error in result]}
        return lint_result


if format_command:

    @route("/format", method="POST")
    def format_sql(
        runners: DbtProjectContainer,
    ):
        LOGGER.info(f"format_sql()")
        # Project Support
        project_runner = (
            runners.get_project(request.get_header("X-dbt-Project"))
            or runners.get_default_project()
        )
        LOGGER.info(f"got project: {project_runner}")
        if not project_runner:
            response.status = 400
            return asdict(
                ServerErrorContainer(
                    error=ServerError(
                        code=ServerErrorCode.ProjectNotRegistered,
                        message=(
                            "Project is not registered. Make a POST request to the /register"
                            " endpoint first to register a runner"
                        ),
                        data={"registered_projects": runners.registered_projects()},
                    )
                )
            )

        sql_path = request.query.get("sql_path")
        LOGGER.info(f"sql_path: {sql_path}")
        if sql_path:
            # Format a file
            # NOTE: Formatting a string is not supported.
            LOGGER.info(f"formatting file: {sql_path}")
            sql = Path(sql_path)
        else:
            # Format a string
            LOGGER.info(f"formatting string")
            sql = request.body.getvalue().decode("utf-8")
        if not sql:
            response.status = 400
            return {
                "error": {
                    "data": {},
                    "message": (
                        "No SQL provided. Either provide a SQL file path or a SQL string to lint."
                    ),
                }
            }
        try:
            LOGGER.info(f"Calling format_command()")
            temp_result, formatted_sql = format_command(
                Path(project_runner.config.project_root),
                sql=sql,
                extra_config_path=(
                    Path(request.query.get("extra_config_path"))
                    if request.query.get("extra_config_path")
                    else None
                ),
            )
        except Exception as format_err:
            logging.exception("Formatting failed")
            response.status = 500
            return {
                "error": {
                    "data": {},
                    "message": str(format_err),
                }
            }
        else:
            LOGGER.info(f"Formatting succeeded")
            format_result = {"result": temp_result, "sql": formatted_sql}
        return format_result


def run_server(runner: Optional[DbtProject] = None, host="localhost", port=8581):
    """Run the dbt core interface server.

    See supported servers below. By default, the server will run with the
    `WSGIRefServer` which is a pure Python server. If you want to use a different server,
    you will need to install the dependencies for that server.

    (CGIServer, FlupFCGIServer, WSGIRefServer, WaitressServer,
    CherryPyServer, CherootServer, PasteServer, FapwsServer,
    TornadoServer, AppEngineServer, TwistedServer, DieselServer,
    MeinheldServer, GunicornServer, EventletServer, GeventServer,
    BjoernServer, AiohttpServer, AiohttpUVLoopServer, AutoServer)
    """
    if runner:
        ServerPlugin.runners.add_parsed_project(runner)
    run(host=host, port=port)


# endregion


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.ERROR)

    # Configure logging for 'dbt_core_interface' and 'dbt_core_interface.sqlfluff_util'
    for logger_name in ["dbt_core_interface", "dbt_core_interface.sqlfluff_util"]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description="Run the dbt interface server. Defaults to the WSGIRefServer"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="The host to run the server on. Defaults to localhost",
    )
    parser.add_argument(
        "--port",
        default=8581,
        help="The port to run the server on. Defaults to 8581",
    )
    args = parser.parse_args()
    ServerPlugin = DbtInterfaceServerPlugin()
    install(ServerPlugin)
    install(JSONPlugin(json_dumps=lambda body: json.dumps(body, default=server_serializer)))

    if lint_command and importlib.util.find_spec("sqlfluff_templater_dbt"):
        LOGGER.error(
            "sqlfluff-templater-dbt is not compatible with dbt-core-interface server. "
            "Please uninstall it to continue."
        )
        sys.exit(1)

    # Reload our state if necessary
    if os.path.isfile("/tmp/savestate.json"):
        LOGGER.info("Found savestate file, trying to load it...")

        with open("/tmp/savestate.json", "rt") as input:
            kwargs = json.load(input)
            project = kwargs["project"]
            del kwargs["project"]

            try:
                new_runner = DbtProject(**kwargs)
                ServerPlugin.runners[project] = new_runner
                ServerPlugin.runners.add_parsed_project

            except Exception as init_error:
                LOGGER.error("Failed to load savestate: %s", init_error)
                LOGGER.error("We'll continue on without trying to restore.")

    run_server(host=args.host, port=args.port)
