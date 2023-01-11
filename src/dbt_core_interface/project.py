from dbt.tracking import disable_tracking


# This is required since we are importing dbt directly
# and using it in a non-standard way
disable_tracking()

import dbt.adapters.factory
from dbt.version import installed as dbt_installed_version


# Version specific dbt constants and overrides
__dbt_major_version__ = int(dbt_installed_version.major or 0)
__dbt_minor_version__ = int(dbt_installed_version.minor or 0)
__dbt_patch_version__ = int(dbt_installed_version.patch or 0)
if (__dbt_major_version__, __dbt_minor_version__, __dbt_patch_version__) > (1, 3, 0):
    RAW_CODE = "raw_code"
    COMPILED_CODE = "compiled_code"
else:
    RAW_CODE = "raw_code"
    COMPILED_CODE = "compiled_code"
if (__dbt_major_version__, __dbt_minor_version__, __dbt_patch_version__) < (1, 5, 0):
    import dbt.events.functions

    # I expect a change in dbt 1.5.0 that will make this monkey patch unnecessary
    dbt.events.functions.fire_event = lambda e: None


# See ... for more info on this monkey patch
dbt.adapters.factory.get_adapter = lambda config: config.adapter

import json
import os
import threading
import time
import uuid
from collections import UserDict
from contextlib import contextmanager, redirect_stdout
from copy import copy
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache, wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

# We maintain the smallest possible surface area of dbt imports
from dbt.adapters.factory import get_adapter_class_by_name
from dbt.config.runtime import RuntimeConfig
from dbt.flags import DEFAULT_PROFILES_DIR, set_from_args
from dbt.node_types import NodeType
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.parser.sql import SqlBlockParser, SqlMacroParser
from dbt.task.sql import SqlCompileRunner

from dbt_core_interface.utils import has_jinja


if TYPE_CHECKING:
    # These imports are only used for type checking
    from agate import Table
    from dbt.adapters.base import BaseRelation
    from dbt.adapters.factory import Adapter
    from dbt.contracts.connection import AdapterResponse
    from dbt.contracts.graph.manifest import (
        ManifestNode,
        MaybeNonSource,
        MaybeParsedSource,
    )
    from dbt.contracts.graph.parsed import ColumnInfo
    from dbt.task.runnable import GraphRunnableTask


__all__ = [
    "DbtProject",
    "DbtAdapterExecutionResult",
    "DbtAdapterCompilationResult",
    "DbtManifestProxy",
    "DbtConfiguration",
    "__dbt_major_version__",
    "__dbt_minor_version__",
    "__dbt_patch_version__",
]

T = TypeVar("T")


class DbtCommand(str, Enum):
    RUN = "run"
    BUILD = "build"
    TEST = "test"
    SEED = "seed"
    RUN_OPERATION = "run-operation"
    LIST = "list"
    SNAPSHOT = "snapshot"


@dataclass
class DbtConfiguration:
    project_dir: str
    profiles_dir: str = DEFAULT_PROFILES_DIR
    target: Optional[str] = None
    threads: int = 1
    single_threaded: bool = True
    _vars: str = "{}"
    # Mutes unwanted dbt output
    quiet: bool = True
    # A required attribute for dbt, not used by our interface
    dependencies: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.target is None:
            del self.target
        self.single_threaded = self.threads == 1

    @property
    def vars(self) -> str:
        return self._vars

    @vars.setter
    def vars(self, v: Union[str, dict]) -> None:
        if isinstance(v, dict):
            v = json.dumps(v)
        self._vars = v


class DbtManifestProxy(UserDict):
    """Proxy for manifest dictionary (`flat_graph`), if we need mutation then we should
    create a copy of the dict or interface with the dbt-core manifest object instead."""

    def _readonly(self, *args, **kwargs):
        raise RuntimeError("Cannot modify DbtManifestProxy")

    __setitem__ = _readonly
    __delitem__ = _readonly
    pop = _readonly
    popitem = _readonly
    clear = _readonly
    update = _readonly
    setdefault = _readonly


@dataclass
class DbtAdapterExecutionResult:
    """Interface for execution results, this keeps us 1 layer removed from dbt interfaces which may change."""

    adapter_response: "AdapterResponse"
    table: "Table"
    raw_code: str
    compiled_code: str


@dataclass
class DbtAdapterCompilationResult:
    """Interface for compilation results, this keeps us 1 layer removed from dbt interfaces which may change."""

    raw_code: str
    compiled_code: str
    node: "ManifestNode"
    injected_code: Optional[str] = None


class DbtTaskConfiguration:
    """A container for task configuration with sane defaults.
    Users should enforce an interface for their tasks via a
    factory method that returns an instance of this class."""

    def __init__(self, profile: str, target: str, **kwargs):
        self.profile = profile  # type: str
        self.target = target  # type: str
        self.kwargs = kwargs or {}  # type: Dict[str, Any]
        self.threads = kwargs.get("threads", 1)  # type: int
        self.single_threaded = kwargs.get(
            "single_threaded", self.threads == 1
        )  # type: bool
        self.state_id = kwargs.get("state_id", None)  # type: Optional[str]
        self.version_check = kwargs.get("version_check", False)  # type: bool
        self.resource_types = kwargs.get(
            "resource_types", None
        )  # type: Optional[List[str]]
        self.models = kwargs.get("models", None)  # type: Union[None, str, List[str]]
        self.select = kwargs.get("select", None)  # type: Union[None, str, List[str]]
        self.exclude = kwargs.get("exclude", None)  # type: Union[None, str, List[str]]
        self.selector_name = kwargs.get("selector_name", None)  # type: Optional[str]
        self.state = kwargs.get("state", None)  # type: Optional[str]
        self.defer = kwargs.get("defer", False)  # type: bool
        self.fail_fast = kwargs.get("fail_fast", False)  # type: bool
        self.full_refresh = kwargs.get("full_refresh", False)  # type: bool
        self.store_failures = kwargs.get("store_failures", False)  # type: bool
        self.indirect_selection = kwargs.get("indirect_selection", False)  # type: bool
        self.data = kwargs.get("data", False)  # type: bool
        # or data_type
        self.schema = kwargs.get("schema", False)  # type: bool
        # or schema_type
        self.show = kwargs.get("show", False)  # type: bool
        self.output = kwargs.get("output", "name")  # type: Optional[str]
        self.output_keys = kwargs.get(
            "output_keys", None
        )  # type: Union[None, str, List[str]]
        self.macro = kwargs.get("macro", None)  # type: Optional[str]
        self.args = kwargs.get("args", "{}")  # type:str
        self.quiet = kwargs.get("quiet", True)  # type: bool

    @classmethod
    def with_config(cls, config: RuntimeConfig, **kwargs):
        """Create a task configuration container from a DbtProject's runtime config.
        This is a good example of where static typing is not necessary. Developers can just
        pass in whatever they want and it will be passed through to the task configuration container.
        Users of the library are free to pass in any mapping derived from their own implementation for
        their own hyper specific task. We are not in the business of remaking dbt-core's task configuration.
        """
        threads = kwargs.pop("threads", config.threads)
        kwargs.pop("single_threaded", None)  # this is a derived property
        return cls(
            config.profile_name,
            config.target_name,
            threads=threads,
            **kwargs,
            single_threaded=threads == 1,
        )


class DbtProject:
    """Container for a dbt project. The dbt attribute is the primary interface for
    dbt-core. The adapter attribute is the primary interface for the dbt adapter."""

    ADAPTER_TTL = 3600

    def __init__(
        self,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        project_dir: Optional[str] = None,
        threads: Optional[int] = 1,
        vars: Optional[str] = "{}",
    ) -> None:
        self.base_config = DbtConfiguration(
            threads=threads,
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
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

    def get_adapter_cls(self) -> "Adapter":
        """Get the adapter class associated with the dbt profile."""
        return get_adapter_class_by_name(self.config.credentials.type)

    def initialize_adapter(self):
        """Initialize a dbt adapter."""
        if hasattr(self, "_adapter"):
            # Clean up any existing connections, err on the side of runtime
            # resiliency, don't let this fail. Maybe there is a world where
            # it really matters, but I don't think so. Someone can make the case.
            try:
                self._adapter.connections.cleanup_all()
            except Exception:
                # TODO: Log this
                pass
        # The adapter.setter verifies connection, resets TTL, and updates adapter ref on config
        # this is thread safe by virtue of the adapter_mutex on the adapter.setter
        self.adapter = self.get_adapter_cls()(self.config)

    @property
    def adapter(self):
        """dbt-core adapter with TTL and automatic reinstantiation. This supports
        long running processes that may have their connection to the database
        terminated by the database server. It is transparent to the user."""
        if time.time() - self._adapter_created_at > self.ADAPTER_TTL:
            self.initialize_adapter()
        return self._adapter

    @adapter.setter
    def adapter(self, adapter: "Adapter"):
        """Verify connection and reset TTL on adapter set, update adapter prop ref on config."""
        # Ensure safe concurrent access to the adapter
        # Currently we choose to drop attempted mutations while an existing mutation is in progress
        # This is a tradeoff between safety and performance, we could also choose to block
        if self.adapter_mutex.acquire(blocking=False):
            try:
                self._adapter = adapter
                self._adapter.connections.set_connection_name()
                self._adapter_created_at = time.time()
                self.config.adapter = self.adapter
            finally:
                self.adapter_mutex.release()

    def parse_project(self, init: bool = False) -> None:
        """Parses project on disk from `DbtConfiguration` in args attribute, verifies connection
        to adapters database, mutates config, adapter, and dbt attributes. Thread-safe. From an
        efficiency perspective, this is a relatively expensive operation, so we want to avoid
        doing it more than necessary.
        """

        # Threads will wait here if another thread is parsing the project
        # however, it probably makes sense to not parse the project once the waiter
        # has acquired the lock, TODO: Lets implement a debounce-like buffer here
        with self.parsing_mutex:
            if init:
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

            self.manifest = _project_parser.load()
            self.manifest.build_flat_graph()
            _project_parser.save_macros_to_adapter(self.adapter)

            self._sql_parser = None
            self._macro_parser = None

    def safe_parse_project(self, reinit: bool = False) -> None:
        """A safe version of parse_project that will not mutate the config if parsing fails."""
        if reinit:
            self.clear_internal_caches()
        _config_pointer = copy(self.config)
        try:
            self.parse_project(init=reinit)
        except Exception as parse_error:
            self.config = _config_pointer
            raise parse_error
        self.write_manifest_artifact()

    def _verify_connection(self, adapter: "Adapter") -> "Adapter":
        """Verification for adapter + profile. Used as a passthrough,
        This also seeds the master connection."""
        try:
            adapter.connections.set_connection_name()
            adapter.debug_query()
        except Exception as query_exc:
            raise RuntimeError("Could not connect to Database") from query_exc
        else:
            return adapter

    def adapter_probe(self) -> bool:
        """Check adapter connection, useful for long running processes such as the server or workbench"""
        if not hasattr(self, "adapter") or self.adapter is None:
            return False
        try:
            with self.adapter.connection_named("osmosis-heartbeat"):
                self.adapter.debug_query()
        except Exception:
            # TODO: Should we preemptively reinitialize the adapter here? or leave it to userland to handle?
            return False
        return True

    def fn_threaded_conn(
        self, fn: Callable[..., T], *args, **kwargs
    ) -> Callable[..., T]:
        """Used for jobs which are intended to be submitted to a thread pool."""

        @wraps(fn)
        def _with_conn() -> T:
            self.adapter.connections.set_connection_name()
            return fn(*args, **kwargs)

        return _with_conn

    def generate_runtime_model_context(self, node: "ManifestNode"):
        """Wraps dbt context provider."""
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
        """Write a manifest.json to disk. Because our project is in memory, this is useful for
        integrating with other tools that expect a manifest.json to be present in the target directory.
        """
        artifact_path = os.path.join(
            self.config.project_root, self.config.target_path, "manifest.json"
        )
        self.manifest.write(artifact_path)

    def clear_internal_caches(self) -> None:
        """Clear least recently used caches and reinstantiable container objects."""
        self.get_ref_node.cache_clear()
        self.get_source_node.cache_clear()
        self.get_macro_function.cache_clear()
        self.get_node_by_path.cache_clear()
        self.get_columns.cache_clear()
        self.compile_code.cache_clear()

    @lru_cache(maxsize=10)
    def get_ref_node(self, target_model_name: str) -> "MaybeNonSource":
        """Get a `ManifestNode` from a dbt project model name
        as one would in a {{ ref(...) }} macro call."""
        return self.manifest.resolve_ref(
            target_model_name=target_model_name,
            target_model_package=None,
            current_project=self.config.project_name,
            node_package=self.config.project_name,
        )

    @lru_cache(maxsize=10)
    def get_source_node(
        self, target_source_name: str, target_table_name: str
    ) -> "MaybeParsedSource":
        """Get a `ManifestNode` from a dbt project source name and table name
        as one would in a {{ source(...) }} macro call."""
        return self.manifest.resolve_source(
            target_source_name=target_source_name,
            target_table_name=target_table_name,
            current_project=self.config.project_name,
            node_package=self.config.project_name,
        )

    @lru_cache(maxsize=10)
    def get_node_by_path(self, path: str) -> Optional["ManifestNode"]:
        """Find an existing node given relative file path. TODO: We can include
        Path obj support and make this more robust.
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
        This is a context manager that will clear the node after execution
        and leverages a mutex during manifest mutation."""
        with self.manifest_mutation_mutex:
            self._clear_node(node_name)
            sql_node = self.sql_parser.parse_remote(sql, node_name)
            process_node(self.config, self.manifest, sql_node)
            yield sql_node
            self._clear_node(node_name)

    def unsafe_generate_server_node(
        self, sql: str, node_name: str = "anonymous_node"
    ) -> "ManifestNode":
        """Get a transient node for SQL execution against adapter. This is faster than
        `generate_server_node` but does not clear the node after execution. That is left to the caller.
        It is also not thread safe in and of itself and requires the caller to manage jitter or mutexes.
        """
        self._clear_node(node_name)
        sql_node = self.sql_parser.parse_remote(sql, node_name)
        process_node(self.config, self.manifest, sql_node)
        return sql_node

    def inject_macro(self, macro_contents: str) -> None:
        """Inject a macro into the project. This is useful for testing macros in isolation.
        It offers unique ways to integrate with dbt."""
        macro_overrides = {}
        for node in self.macro_parser.parse_remote(macro_contents):
            macro_overrides[node.unique_id] = node
        self.manifest.macros.update(macro_overrides)

    @lru_cache(maxsize=100)
    def get_macro_function(self, macro_name: str) -> Callable[[Dict[str, Any]], Any]:
        """Get macro as a function which behaves like a Python function.

        make_schema_fn = get_macro_function("make_schema")\n
        make_schema_fn(name="test_schema_1")\n
        make_schema_fn(name="test_schema_2")"""

        def _macro_fn(**kwargs):
            return self.adapter.execute_macro(macro_name, self.manifest, **kwargs)

        return _macro_fn

    def execute_macro(self, macro: str, **kwargs) -> Any:
        """Wraps adapter execute_macro. Execute a macro like a python function.

        execute_macro("make_schema", name="test_schema_1")"""
        return self.get_macro_function(macro)(**kwargs)

    def adapter_execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple["AdapterResponse", "Table"]:
        """Wraps adapter.execute. Execute SQL against database. This is more on-the-rails
        than `execute_code` which intelligently handles jinja compilation provides a proxy result.
        """
        return self.adapter.execute(sql, auto_begin, fetch)

    def execute_code(self, raw_code: str) -> DbtAdapterExecutionResult:
        """Execute dbt SQL statement against database. This is a proxy for `adapter_execute` and
        the the recommended method for executing SQL against the database."""
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

    @lru_cache(maxsize=100)
    def compile_code(self, raw_code: str) -> DbtAdapterCompilationResult:
        """Creates a node with `generate_server_node` method. Compile generated node.
        Has a retry built in because even uuidv4 cannot gaurantee uniqueness at the speed
        in which we can call this function concurrently. A retry significantly increases the stability.
        """
        temp_node_id = str(uuid.uuid4())
        with self.generate_server_node(raw_code, temp_node_id) as node:
            return self.compile_from_node(node)

    @lru_cache(maxsize=100)
    def unsafe_compile_code(
        self, raw_code: str, retry: int = 3
    ) -> DbtAdapterCompilationResult:
        """Creates a node with `unsafe_generate_server_node` method. Compiles the generated node.
        Has a retry built in because even uuid4 cannot gaurantee uniqueness at the speed
        in which we can call this function concurrently. A retry significantly increases the
        stability. This is certainly the fastest way to compile SQL but it is yet to be benchmarked.
        """
        temp_node_id = str(uuid.uuid4())
        try:
            node = self.compile_from_node(
                self.unsafe_generate_server_node(raw_code, temp_node_id)
            )
        except Exception as compilation_error:
            if retry > 0:
                return self.compile_code(raw_code, retry - 1)
            raise compilation_error
        else:
            return node
        finally:
            self._clear_node(temp_node_id)

    def compile_from_node(self, node: "ManifestNode") -> DbtAdapterCompilationResult:
        """Compiles existing node. ALL compilation passes through this code path. Raw SQL is marshalled
        by the caller into a mock node before being passed into this method. Existing nodes can
        be passed in here directly.
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
        """Clears remote node from dbt project."""
        self.manifest.nodes.pop(
            f"{NodeType.SqlOperation}.{self.project_name}.{name}", None
        )

    def get_relation(
        self, database: str, schema: str, name: str
    ) -> Optional["BaseRelation"]:
        """Wrapper for `adapter.get_relation`."""
        return self.adapter.get_relation(database, schema, name)

    def relation_exists(self, database: str, schema: str, name: str) -> bool:
        """A simple interface for checking if a relation exists in the database."""
        return self.adapter.get_relation(database, schema, name) is not None

    def node_exists(self, node: "ManifestNode") -> bool:
        """A simple interface for checking if a node exists in the database."""
        return (
            self.adapter.get_relation(self.create_relation_from_node(node)) is not None
        )

    def create_relation(self, database: str, schema: str, name: str) -> "BaseRelation":
        """Wrapper for `adapter.Relation.create`."""
        return self.adapter.Relation.create(database, schema, name)

    def create_relation_from_node(self, node: "ManifestNode") -> "BaseRelation":
        """Wrapper for `adapter.Relation.create_from`."""
        return self.adapter.Relation.create_from(self.config, node)

    def get_columns_in_node(self, node: "ManifestNode") -> List[str]:
        """Wrapper for `adapter.get_columns_in_relation`."""
        return self.adapter.get_columns_in_relation(
            self.create_relation_from_node(node)
        )

    @lru_cache(maxsize=10)
    def get_columns(self, node: "ManifestNode") -> List["ColumnInfo"]:
        """Get a list of columns from a compiled node.
        TODO: This is not fully baked. The API is stable but the implementation is not.
        """
        columns = []
        try:
            columns.extend([c.name for c in self.get_columns_in_node(node)])
        except Exception:
            original_sql = str(getattr(node, RAW_CODE))
            # TODO: account for `TOP` syntax?
            setattr(node, RAW_CODE, f"select * from ({original_sql}) limit 0")
            result = self.execute_from_node(node)
            setattr(node, RAW_CODE, original_sql)
            delattr(node, COMPILED_CODE)
            columns.extend(result.table.column_names)
        return columns

    def get_or_create_relation(
        self, database: str, schema: str, name: str
    ) -> Tuple["BaseRelation", bool]:
        """Get relation or create if not exists. Returns tuple of relation and
        boolean result of whether it existed ie: (relation, did_exist)."""
        ref = self.get_relation(database, schema, name)
        return (
            (ref, True)
            if ref is not None
            else (self.create_relation(database, schema, name), False)
        )

    def create_schema(self, node: "ManifestNode"):
        """Create a schema in the database leveraging dbt-core's builtin macro."""
        return self.execute_macro(
            "create_schema",
            kwargs={"relation": self.create_relation_from_node(node)},
        )

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
        """A dbt-core SQL parser capable of parsing and adding nodes to the manifest via `parse_remote` which will
        also return the added node to the caller. Note that post-parsing this still typically requires calls to
        `_process_nodes_for_ref` and `_process_sources_for_ref` from the `dbt.parser.manifest` module in order to compile.
        We have higher level methods that handle this for you.
        """
        if self._sql_parser is None:
            self._sql_parser = SqlBlockParser(self.config, self.manifest, self.config)
        return self._sql_parser

    @property
    def macro_parser(self) -> SqlMacroParser:
        """A dbt-core macro parser. Parse macros with `parse_remote` and add them to the manifest. We have a higher
        level method `inject_macro` that handles this for you."""
        if self._macro_parser is None:
            self._macro_parser = SqlMacroParser(self.config, self.manifest)
        return self._macro_parser

    def get_task_cls(self, typ: DbtCommand) -> Type["GraphRunnableTask"]:
        """Get a dbt-core task class by type.
        This could be overridden to add custom tasks such as linting, etc.
        so long as they are subclasses of `GraphRunnableTask`."""

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

    def get_task(self, typ: str, args: DbtTaskConfiguration) -> "GraphRunnableTask":
        """Get a dbt-core task by type."""
        task = self.get_task_cls(typ)(args, self.config)
        # Render this a no-op on this class instance so that the tasks `run`
        # method plumbing will defer to our existing in memory manifest.
        task.load_manifest = lambda *args, **kwargs: None  # type: ignore
        task.manifest = self.manifest
        return task

    def list(
        self,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        threads: int = 1,
        **kwargs,
    ):
        """List resources in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        with redirect_stdout(None):
            return self.get_task(
                DbtCommand.LIST,
                DbtTaskConfiguration.with_config(
                    self.config,
                    select=select,
                    exclude=exclude,
                    threads=threads or self.config.threads,
                    **kwargs,
                ),
            ).run()

    def run(
        self,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        threads: int = 1,
        **kwargs,
    ):
        """Run models in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        return self.get_task(
            DbtCommand.RUN,
            DbtTaskConfiguration.with_config(
                self.config,
                select=select,
                exclude=exclude,
                threads=threads or self.config.threads,
                **kwargs,
            ),
        ).run()

    def test(
        self,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        threads: int = 1,
        **kwargs,
    ):
        """Test models in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        return self.get_task(
            DbtCommand.TEST,
            DbtTaskConfiguration.with_config(
                self.config,
                select=select,
                exclude=exclude,
                threads=threads or self.config.threads,
                **kwargs,
            ),
        ).run()

    def build(
        self,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        threads: int = 1,
        **kwargs,
    ):
        """Build resources in the dbt project."""
        select, exclude = marshall_selection_args(select, exclude)
        return self.get_task(
            DbtCommand.BUILD,
            DbtTaskConfiguration.with_config(
                self.config,
                select=select,
                exclude=exclude,
                threads=threads or self.config.threads,
                **kwargs,
            ),
        ).run()


def marshall_selection_args(select, exclude):
    """Marshall selection arguments to a list of strings."""
    if select is None:
        select = []
    if exclude is None:
        exclude = []
    # Permit standalone strings such as "my_model+ @some_other_model"
    # as well as lists of strings such as ["my_model+", "@some_other_model"]
    if not isinstance(select, Iterable):
        select = [select]
    if not isinstance(exclude, Iterable):
        exclude = [exclude]
    return select, exclude
