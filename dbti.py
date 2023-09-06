#!/usr/bin/env python
'The interface for interacting with dbt-core.\n\nWe package the interface as a single python module that can be imported\nand used in other python projects. You do not need to include dbt-core-interface\nas a dependency in your project if you do not want to. You can simply copy\nthe dbt_core_interface folder into your project and import it from there.\n'
_AV='primary_key'
_AU='Running diff'
_AT='Project header `X-dbt-Project` was not supplied but is required for this endpoint'
_AS='dbt-interface-server'
_AR='bind_addr'
_AQ='Key has type %r (not a string)'
_AP='%s is read-only.'
_AO='Last-Modified'
_AN='Content-Range'
_AM='This request is not connected to a route.'
_AL='text/html; charset=UTF-8'
_AK='route.url_args'
_AJ='bottle.route'
_AI='bottle.app'
_AH='app_reset'
_AG='before_request'
_AF='Read-Only property.'
_AE='single_threaded'
_AD='AdapterResponse'
_AC='compiled_code'
_AB='timestamp'
_AA='localhost'
_A9='keyfile'
_A8='certfile'
_A7='Do not use this API directly.'
_A6='bottle.request.ext.%s'
_A5='HTTP_'
_A4='127.0.0.1'
_A3='CONTENT_LENGTH'
_A2='application/json'
_A1='bottle.exc_info'
_A0='ignore'
_z='after_request'
_y='config'
_x='PROXY'
_w='strict'
_v='wsgiref'
_u='temporary'
_t='sql'
_s='create_table_as'
_r='create_schema'
_q='anonymous_node'
_p='threads'
_o='_rebase'
_n='error'
_m='rb'
_l='\r'
_k='CONTENT_TYPE'
_j='wsgi.errors'
_i='target'
_h='HEAD'
_g='REQUEST_METHOD'
_f='default'
_e='{}'
_d='ManifestNode'
_c='registered_projects'
_b='Project is not registered. Make a POST request to the /register endpoint first to register a runner'
_a='BOTTLE_CHILD'
_Z='wsgi.input'
_Y='QUERY_STRING'
_X='SCRIPT_NAME'
_W='catchall'
_V='utf-8'
_U='__main__'
_T='POST'
_S='GET'
_R='relation'
_Q='X-dbt-Project'
_P='close'
_O=':'
_N='Content-Type'
_M='PATH_INFO'
_L='?'
_K='latin1'
_J='Content-Length'
_I='name'
_H='.'
_G='\n'
_F='environ'
_E='utf8'
_D='/'
_C=False
_B=True
_A=None
if 1:import dbt.adapters.factory;dbt.adapters.factory.get_adapter=lambda config:config.adapter
import _thread as thread,base64,calendar,cgi,configparser,decimal,email.utils,functools,hashlib,hmac,http.client as httplib,itertools,json,logging,mimetypes,os,pickle,re,sys,tempfile,threading,time,uuid,warnings,weakref
from collections import OrderedDict,UserDict
from collections.abc import MutableMapping as DictMixin
from contextlib import contextmanager,redirect_stdout
from copy import copy
from dataclasses import asdict,dataclass,field
from datetime import date as datedate
from datetime import datetime,timedelta
from enum import Enum
from functools import lru_cache,wraps
from http.cookies import CookieError,Morsel,SimpleCookie
from inspect import getfullargspec
from io import BytesIO
from json import dumps as json_dumps
from json import loads as json_lds
from pathlib import Path
from tempfile import NamedTemporaryFile
from traceback import format_exc,print_exc
from types import FunctionType
from types import ModuleType as new_module
from typing import TYPE_CHECKING,Any,Callable,Dict,Generator,List,Optional,Tuple,Type,TypeVar,Union,cast
from unicodedata import normalize
from urllib.parse import SplitResult as UrlSplitResult
from urllib.parse import quote as urlquote
from urllib.parse import unquote as urlunquote
from urllib.parse import urlencode,urljoin
import dbt.version
from dbt.adapters.factory import get_adapter_class_by_name
from dbt.clients.system import make_directory
from dbt.config.runtime import RuntimeConfig
from dbt.node_types import NodeType
from dbt.parser.manifest import PARTIAL_PARSE_FILE_NAME,ManifestLoader,process_node
from dbt.parser.sql import SqlBlockParser,SqlMacroParser
from dbt.task.sql import SqlCompileRunner
from dbt.tracking import disable_tracking
from dbt.flags import set_from_args
try:from dbt.contracts.graph.parsed import ColumnInfo;from dbt.contracts.graph.compiled import ManifestNode
except Exception:from dbt.contracts.graph.nodes import ColumnInfo,ManifestNode
if TYPE_CHECKING:from agate import Table;from dbt.adapters.base import BaseAdapter,BaseRelation;from dbt.contracts.connection import AdapterResponse;from dbt.contracts.results import ExecutionResult,RunExecutionResult;from dbt.semver import VersionSpecifier;from dbt.task.runnable import ManifestTask
disable_tracking()
urlunquote=functools.partial(urlunquote,encoding=_K)
__dbt_major_version__=int(dbt.version.installed.major or 0)
__dbt_minor_version__=int(dbt.version.installed.minor or 0)
__dbt_patch_version__=int(dbt.version.installed.patch or 0)
if(__dbt_major_version__,__dbt_minor_version__,__dbt_patch_version__)>(1,3,0):RAW_CODE='raw_code';COMPILED_CODE=_AC
else:RAW_CODE='raw_code';COMPILED_CODE=_AC
if(__dbt_major_version__,__dbt_minor_version__,__dbt_patch_version__)<(1,5,0):import dbt.events.functions;dbt.events.functions.fire_event=lambda *args,**kwargs:_A
def default_project_dir():
	'Get the default project directory.';A='DBT_PROJECT_DIR'
	if A in os.environ:return Path(os.environ[A]).resolve()
	paths=list(Path.cwd().parents);paths.insert(0,Path.cwd());return next((x for x in paths if (x/'dbt_project.yml').exists()),Path.cwd())
def default_profiles_dir():
	'Get the default profiles directory.';A='DBT_PROFILES_DIR'
	if A in os.environ:return Path(os.environ[A]).resolve()
	return Path.cwd()if (Path.cwd()/'profiles.yml').exists()else Path.home()/'.dbt'
DEFAULT_PROFILES_DIR=str(default_profiles_dir())
DEFAULT_PROJECT_DIR=str(default_project_dir())
def write_manifest_for_partial_parse(self):
	'Monkey patch for dbt manifest loader.';path=os.path.join(self.root_project.project_root,self.root_project.target_path,PARTIAL_PARSE_FILE_NAME)
	try:
		if self.manifest.metadata.dbt_version!=dbt.version.__version__:self.manifest.metadata.dbt_version=dbt.version.__version__
		manifest_msgpack=self.manifest.to_msgpack();make_directory(os.path.dirname(path))
		with open(path,'wb')as fp:fp.write(manifest_msgpack)
	except Exception:raise
if(__dbt_major_version__,__dbt_minor_version__)<(1,4):ManifestLoader.write_manifest_for_partial_parse=write_manifest_for_partial_parse
__all__=['DbtProject','DbtProjectContainer','DbtAdapterExecutionResult','DbtAdapterCompilationResult','DbtManifestProxy','DbtConfiguration','__dbt_major_version__','__dbt_minor_version__','__dbt_patch_version__','DEFAULT_PROFILES_DIR','DEFAULT_PROJECT_DIR','ServerRunResult','ServerCompileResult','ServerResetResult','ServerRegisterResult','ServerUnregisterResult','ServerErrorCode','ServerError','ServerErrorContainer','ServerPlugin','run_server','default_project_dir','default_profiles_dir','ColumnInfo',_d]
T=TypeVar('T')
JINJA_CONTROL_SEQUENCES=['{{','}}','{%','%}','{#','#}']
LOGGER=logging.getLogger(__name__)
__version__=dbt.version.__version__
class DbtCommand(str,Enum):'The dbt commands we support.';RUN='run';BUILD='build';TEST='test';SEED='seed';RUN_OPERATION='run-operation';LIST='list';SNAPSHOT='snapshot'
@dataclass
class DbtConfiguration:
	'The configuration for dbt-core.';project_dir:str=DEFAULT_PROJECT_DIR;profiles_dir:str=DEFAULT_PROFILES_DIR;target:Optional[str]=_A;threads:int=1;single_threaded:bool=_B;_vars:str=_e;quiet:bool=_B;use_experimental_parser:bool=_C;static_parser:bool=_C;partial_parse:bool=_C;dependencies:List[str]=field(default_factory=list)
	def __post_init__(self):
		'Post init hook to set single_threaded and remove target if not provided.'
		if self.target is _A:del self.target
		self.single_threaded=self.threads==1
	@property
	def profile(self):'Access the profiles_dir attribute as a string.';return _A
	@property
	def vars(self):'Access the vars attribute as a string.';return self._vars
	@vars.setter
	def vars(self,v):
		'Set the vars attribute as a string or dict.\n\n        If dict then it will be converted to a string which is what dbt expects.\n        '
		if(__dbt_major_version__,__dbt_minor_version__)>=(1,5):
			if isinstance(v,str):v=json.loads(v)
		elif isinstance(v,dict):v=json.dumps(v)
		self._vars=v
class DbtManifestProxy(UserDict):
	'Proxy for manifest dictionary object.\n\n    If we need mutation then we should create a copy of the dict or interface with the dbt-core manifest object instead.\n    '
	def _readonly(self,*args,**kwargs):raise RuntimeError('Cannot modify DbtManifestProxy')
	__setitem__=_readonly;__delitem__=_readonly;pop=_readonly;popitem=_readonly;clear=_readonly;update=_readonly;setdefault=_readonly
@dataclass
class DbtAdapterExecutionResult:'Interface for execution results.\n\n    This keeps us 1 layer removed from dbt interfaces which may change.\n    ';adapter_response:_AD;table:'Table';raw_code:str;compiled_code:str
@dataclass
class DbtAdapterCompilationResult:'Interface for compilation results.\n\n    This keeps us 1 layer removed from dbt interfaces which may change.\n    ';raw_code:str;compiled_code:str;node:_d;injected_code:Optional[str]=_A
class DbtTaskConfiguration:
	'A container for task configuration with sane defaults.\n\n    Users should enforce an interface for their tasks via a factory method that returns an instance of this class.\n    '
	def __init__(self,profile,target,**kwargs):'Initialize the task configuration.';self.profile=profile;self.target=target;self.kwargs=kwargs or{};self.threads=kwargs.get(_p,1);self.single_threaded=kwargs.get(_AE,self.threads==1);self.state_id=kwargs.get('state_id');self.version_check=kwargs.get('version_check',_C);self.resource_types=kwargs.get('resource_types');self.models=kwargs.get('models');self.select=kwargs.get('select');self.exclude=kwargs.get('exclude');self.selector_name=kwargs.get('selector_name');self.state=kwargs.get('state');self.defer=kwargs.get('defer',_C);self.fail_fast=kwargs.get('fail_fast',_C);self.full_refresh=kwargs.get('full_refresh',_C);self.store_failures=kwargs.get('store_failures',_C);self.indirect_selection=kwargs.get('indirect_selection','eager');self.data=kwargs.get('data',_C);self.schema=kwargs.get('schema',_C);self.show=kwargs.get('show',_C);self.output=kwargs.get('output',_I);self.output_keys=kwargs.get('output_keys');self.macro=kwargs.get('macro');self.args=kwargs.get('args',_e);self.quiet=kwargs.get('quiet',_B)
	def __getattribute__(self,__name):'"Force all attribute access to be lower case.';return object.__getattribute__(self,__name.lower())
	def __getattr__(self,name):'Get an attribute from the kwargs if it does not exist on the class.\n\n        This is useful for passing through arbitrary arguments to dbt while still\n        being able to manage some semblance of a sane interface with defaults.\n        ';return self.kwargs.get(name)
	@classmethod
	def from_runtime_config(cls,config,**kwargs):"Create a task configuration container from a DbtProject's runtime config.\n\n        This is a good example of where static typing is not necessary. Developers can just\n        pass in whatever they want and it will be passed through to the task configuration container.\n        Users of the library are free to pass in any mapping derived from their own implementation for\n        their own custom task.\n        ";threads=kwargs.pop(_p,config.threads);kwargs.pop(_AE,_A);return cls(config.profile_name,config.target_name,threads=threads,single_threaded=threads==1,**kwargs)
class DbtProject:
	'Container for a dbt project.\n\n    The dbt attribute is the primary interface for dbt-core. The adapter attribute is the primary interface for the dbt adapter.\n    ';ADAPTER_TTL=3600
	def __init__(self,target=_A,profiles_dir=DEFAULT_PROFILES_DIR,project_dir=DEFAULT_PROJECT_DIR,threads=1,vars=_e):'Initialize the DbtProject.';self.base_config=DbtConfiguration(threads=threads,target=target,profiles_dir=profiles_dir or DEFAULT_PROFILES_DIR,project_dir=project_dir or DEFAULT_PROJECT_DIR);self.base_config.vars=vars;self.adapter_mutex=threading.Lock();self.parsing_mutex=threading.Lock();self.manifest_mutation_mutex=threading.Lock();self.parse_project(init=_B);self._sql_parser=_A;self._macro_parser=_A
	@classmethod
	def from_config(cls,config):'Instatiate the DbtProject directly from a DbtConfiguration instance.';return cls(target=config.target,profiles_dir=config.profiles_dir,project_dir=config.project_dir,threads=config.threads)
	def get_adapter_cls(self):'Get the adapter class associated with the dbt profile.';return get_adapter_class_by_name(self.config.credentials.type)
	def initialize_adapter(self):
		'Initialize a dbt adapter.'
		if hasattr(self,'_adapter'):
			try:self._adapter.connections.cleanup_all()
			except Exception as e:LOGGER.debug(f"Failed to cleanup adapter connections: {e}")
		self.adapter=self.get_adapter_cls()(self.config)
	@property
	def adapter(self):
		'dbt-core adapter with TTL and automatic reinstantiation.\n\n        This supports long running processes that may have their connection to the database terminated by\n        the database server. It is transparent to the user.\n        '
		if time.time()-self._adapter_created_at>self.ADAPTER_TTL:self.initialize_adapter()
		return self._adapter
	@adapter.setter
	def adapter(self,adapter):
		'Verify connection and reset TTL on adapter set, update adapter prop ref on config.'
		if self.adapter_mutex.acquire(blocking=_C):
			try:self._adapter=adapter;self._adapter.connections.set_connection_name();self._adapter_created_at=time.time();self.config.adapter=self.adapter
			finally:self.adapter_mutex.release()
	def parse_project(self,init=_C):
		'Parse project on disk.\n\n        Uses the config from `DbtConfiguration` in args attribute, verifies connection to adapters database,\n        mutates config, adapter, and dbt attributes. Thread-safe. From an efficiency perspective, this is a\n        relatively expensive operation, so we want to avoid doing it more than necessary.\n        '
		with self.parsing_mutex:
			if init:set_from_args(self.base_config,self.base_config);self.config=RuntimeConfig.from_args(self.base_config);self.initialize_adapter()
			_project_parser=ManifestLoader(self.config,self.config.load_dependencies(),self.adapter.connections.set_query_header);self.manifest=_project_parser.load();self.manifest.build_flat_graph();_project_parser.save_macros_to_adapter(self.adapter);self._sql_parser=_A;self._macro_parser=_A
	def safe_parse_project(self,reinit=_C):
		'Safe version of parse_project that will not mutate the config if parsing fails.'
		if reinit:self.clear_internal_caches()
		_config_pointer=copy(self.config)
		try:self.parse_project(init=reinit)
		except Exception as parse_error:self.config=_config_pointer;raise parse_error
		self.write_manifest_artifact()
	def _verify_connection(self,adapter):
		'Verification for adapter + profile.\n\n        Used as a passthrough, this also seeds the master connection.\n        '
		try:adapter.connections.set_connection_name();adapter.debug_query()
		except Exception as query_exc:raise RuntimeError('Could not connect to Database') from query_exc
		else:return adapter
	def adapter_probe(self):
		'Check adapter connection, useful for long running procsesses.'
		if not hasattr(self,'adapter')or self.adapter is _A:return _C
		try:
			with self.adapter.connection_named('osmosis-heartbeat'):self.adapter.debug_query()
		except Exception:return _C
		return _B
	def fn_threaded_conn(self,fn,*args,**kwargs):
		'For jobs which are intended to be submitted to a thread pool.'
		@wraps(fn)
		def _with_conn():self.adapter.connections.set_connection_name();return fn(*(args),**kwargs)
		return _with_conn
	def generate_runtime_model_context(self,node):'Wrap dbt context provider.';from dbt.context.providers import generate_runtime_model_context;return generate_runtime_model_context(node,self.config,self.manifest)
	@property
	def project_name(self):'dbt project name.';return self.config.project_name
	@property
	def project_root(self):'dbt project root.';return self.config.project_root
	@property
	def manifest_dict(self):'dbt manifest dict.';return DbtManifestProxy(self.manifest.flat_graph)
	def write_manifest_artifact(self):'Write a manifest.json to disk.\n\n        Because our project is in memory, this is useful for integrating with other tools that\n        expect a manifest.json to be present in the target directory.\n        ';artifact_path=os.path.join(self.config.project_root,self.config.target_path,'manifest.json');self.manifest.write(artifact_path)
	def clear_internal_caches(self):'Clear least recently used caches and reinstantiable container objects.';self.compile_code.cache_clear();self.unsafe_compile_code.cache_clear()
	def get_ref_node(self,target_model_name):'Get a `ManifestNode` from a dbt project model name.\n\n        This is the same as one would in a {{ ref(...) }} macro call.\n        ';return cast(_d,self.manifest.resolve_ref(target_model_name=target_model_name,target_model_package=_A,current_project=self.config.project_name,node_package=self.config.project_name))
	def get_source_node(self,target_source_name,target_table_name):'Get a `ManifestNode` from a dbt project source name and table name.\n\n        This is the same as one would in a {{ source(...) }} macro call.\n        ';return cast(_d,self.manifest.resolve_source(target_source_name=target_source_name,target_table_name=target_table_name,current_project=self.config.project_name,node_package=self.config.project_name))
	def get_node_by_path(self,path):
		'Find an existing node given relative file path.\n\n        TODO: We can include Path obj support and make this more robust.\n        '
		for node in self.manifest.nodes.values():
			if node.original_file_path==path:return node
		return _A
	@contextmanager
	def generate_server_node(self,sql,node_name=_q):
		'Get a transient node for SQL execution against adapter.\n\n        This is a context manager that will clear the node after execution and leverages a mutex during manifest mutation.\n        '
		with self.manifest_mutation_mutex:self._clear_node(node_name);sql_node=self.sql_parser.parse_remote(sql,node_name);process_node(self.config,self.manifest,sql_node);yield sql_node;self._clear_node(node_name)
	def unsafe_generate_server_node(self,sql,node_name=_q):'Get a transient node for SQL execution against adapter.\n\n        This is faster than `generate_server_node` but does not clear the node after execution.\n        That is left to the caller. It is also not thread safe in and of itself and requires the caller to\n        manage jitter or mutexes.\n        ';self._clear_node(node_name);sql_node=self.sql_parser.parse_remote(sql,node_name);process_node(self.config,self.manifest,sql_node);return sql_node
	def inject_macro(self,macro_contents):
		'Inject a macro into the project.\n\n        This is useful for testing macros in isolation. It offers unique ways to integrate with dbt.\n        ';macro_overrides={}
		for node in self.macro_parser.parse_remote(macro_contents):macro_overrides[node.unique_id]=node
		self.manifest.macros.update(macro_overrides)
	def get_macro_function(self,macro_name):
		'Get macro as a function which behaves like a Python function.'
		def _macro_fn(**kwargs):return self.adapter.execute_macro(macro_name,self.manifest,kwargs=kwargs)
		return _macro_fn
	def execute_macro(self,macro,**kwargs):'Wrap adapter execute_macro. Execute a macro like a python function.';return self.get_macro_function(macro)(**kwargs)
	def adapter_execute(self,sql,auto_begin=_C,fetch=_C):'Wrap adapter.execute. Execute SQL against database.\n\n        This is more on-the-rails than `execute_code` which intelligently handles jinja compilation provides a proxy result.\n        ';return cast(Tuple[_AD,'Table'],self.adapter.execute(sql,auto_begin,fetch))
	def execute_code(self,raw_code):
		'Execute dbt SQL statement against database.\n\n        This is a proxy for `adapter_execute` and the the recommended method for executing SQL against the database.\n        ';compiled_code=str(raw_code)
		if has_jinja(raw_code):compiled_code=self.compile_code(raw_code).compiled_code
		return DbtAdapterExecutionResult(*self.adapter_execute(compiled_code,fetch=_B),raw_code,compiled_code)
	def execute_from_node(self,node):
		'Execute dbt SQL statement against database from a "ManifestNode".';raw_code=getattr(node,RAW_CODE);compiled_code=getattr(node,COMPILED_CODE,_A)
		if compiled_code:return self.execute_code(compiled_code)
		if has_jinja(raw_code):compiled_code=self.compile_from_node(node).compiled_code
		return self.execute_code(compiled_code or raw_code)
	@lru_cache(maxsize=100)
	def compile_code(self,raw_code):
		'Create a node with `generate_server_node` method. Compile generated node.\n\n        Has a retry built in because even uuidv4 cannot gaurantee uniqueness at the speed\n        in which we can call this function concurrently. A retry significantly increases the stability.\n        ';temp_node_id=str(uuid.uuid4())
		with self.generate_server_node(raw_code,temp_node_id)as node:return self.compile_from_node(node)
	@lru_cache(maxsize=100)
	def unsafe_compile_code(self,raw_code,retry=3):
		'Create a node with `unsafe_generate_server_node` method. Compiles the generated node.\n\n        Has a retry built in because even uuid4 cannot gaurantee uniqueness at the speed\n        in which we can call this function concurrently. A retry significantly increases the\n        stability. This is certainly the fastest way to compile SQL but it is yet to be benchmarked.\n        ';temp_node_id=str(uuid.uuid4())
		try:node=self.compile_from_node(self.unsafe_generate_server_node(raw_code,temp_node_id))
		except Exception as compilation_error:
			if retry>0:return self.compile_code(raw_code,retry-1)
			raise compilation_error
		else:return node
		finally:self._clear_node(temp_node_id)
	def compile_from_node(self,node):'Compiles existing node. ALL compilation passes through this code path.\n\n        Raw SQL is marshalled by the caller into a mock node before being passed into this method.\n        Existing nodes can be passed in here directly.\n        ';compiled_node=SqlCompileRunner(self.config,self.adapter,node=node,node_index=1,num_nodes=1).compile(self.manifest);return DbtAdapterCompilationResult(getattr(compiled_node,RAW_CODE),getattr(compiled_node,COMPILED_CODE),compiled_node)
	def _clear_node(self,name=_q):'Clear remote node from dbt project.';self.manifest.nodes.pop(f"{NodeType.SqlOperation}.{self.project_name}.{name}",_A)
	def get_relation(self,database,schema,name):'Wrap for `adapter.get_relation`.';return self.adapter.get_relation(database,schema,name)
	def relation_exists(self,database,schema,name):'Interface for checking if a relation exists in the database.';return self.adapter.get_relation(database,schema,name)is not _A
	def node_exists(self,node):'Interface for checking if a node exists in the database.';return self.adapter.get_relation(self.create_relation_from_node(node))is not _A
	def create_relation(self,database,schema,name):'Wrap `adapter.Relation.create`.';return self.adapter.Relation.create(database,schema,name)
	def create_relation_from_node(self,node):'Wrap `adapter.Relation.create_from`.';return self.adapter.Relation.create_from(self.config,node)
	def get_or_create_relation(self,database,schema,name):'Get relation or create if not exists.\n\n        Returns tuple of relation and boolean result of whether it existed ie: (relation, did_exist).\n        ';ref=self.get_relation(database,schema,name);return(ref,_B)if ref is not _A else(self.create_relation(database,schema,name),_C)
	def create_schema(self,node):"Create a schema in the database leveraging dbt-core's builtin macro.";self.execute_macro(_r,kwargs={_R:self.create_relation_from_node(node)})
	def get_columns_in_node(self,node):'Wrap `adapter.get_columns_in_relation`.';return self.adapter.get_columns_in_relation(self.create_relation_from_node(node)),
	def get_columns(self,node):
		'Get a list of columns from a compiled node.';columns=[]
		try:columns.extend([c.name for c in self.get_columns_in_node(node)])
		except Exception:original_sql=str(getattr(node,RAW_CODE));setattr(node,RAW_CODE,f"SELECT * FROM ({original_sql}) WHERE 1=0");result=self.execute_from_node(node);setattr(node,RAW_CODE,original_sql),delattr(node,COMPILED_CODE);node.compiled=_C;columns.extend(result.table.column_names)
		return columns
	def materialize(self,node,temporary=_B):'Materialize a table in the database.\n\n        TODO: This is not fully baked. The API is stable but the implementation is not.\n        ';return self.adapter_execute(self.execute_macro(_s,kwargs={_t:getattr(node,COMPILED_CODE),_R:self.create_relation_from_node(node),_u:temporary}),auto_begin=_B)
	@property
	def sql_parser(self):
		'A dbt-core SQL parser capable of parsing and adding nodes to the manifest via `parse_remote` which will also return the added node to the caller.\n\n        Note that post-parsing this still typically requires calls to `_process_nodes_for_ref`\n        and `_process_sources_for_ref` from the `dbt.parser.manifest` module in order to compile.\n        We have higher level methods that handle this for you.\n        '
		if self._sql_parser is _A:self._sql_parser=SqlBlockParser(self.config,self.manifest,self.config)
		return self._sql_parser
	@property
	def macro_parser(self):
		'A dbt-core macro parser. Parse macros with `parse_remote` and add them to the manifest.\n\n        We have a higher level method `inject_macro` that handles this for you.\n        '
		if self._macro_parser is _A:self._macro_parser=SqlMacroParser(self.config,self.manifest)
		return self._macro_parser
	def get_task_config(self,**kwargs):'Get a dbt-core task configuration.';threads=kwargs.pop(_p,self.config.threads);return DbtTaskConfiguration.from_runtime_config(config=self.config,threads=threads,**kwargs)
	def get_task_cls(self,typ):'Get a dbt-core task class by type.\n\n        This could be overridden to add custom tasks such as linting, etc.\n        so long as they are subclasses of `GraphRunnableTask`.\n        ';from dbt.task.build import BuildTask;from dbt.task.list import ListTask;from dbt.task.run import RunTask;from dbt.task.run_operation import RunOperationTask;from dbt.task.seed import SeedTask;from dbt.task.snapshot import SnapshotTask;from dbt.task.test import TestTask;return{DbtCommand.RUN:RunTask,DbtCommand.BUILD:BuildTask,DbtCommand.TEST:TestTask,DbtCommand.SEED:SeedTask,DbtCommand.LIST:ListTask,DbtCommand.SNAPSHOT:SnapshotTask,DbtCommand.RUN_OPERATION:RunOperationTask}[typ]
	def get_task(self,typ,args):
		'Get a dbt-core task by type.'
		if(__dbt_major_version__,__dbt_minor_version__)<(1,5):task=self.get_task_cls(typ)(args,self.config);task.load_manifest=lambda *args,**kwargs:_A;task.manifest=self.manifest
		else:task=self.get_task_cls(typ)(args,self.config,self.manifest)
		return task
	def list(self,select=_A,exclude=_A,**kwargs):
		'List resources in the dbt project.';select,exclude=marshall_selection_args(select,exclude)
		with redirect_stdout(_A):return self.get_task(DbtCommand.LIST,self.get_task_config(select=select,exclude=exclude,**kwargs)).run()
	def run(self,select=_A,exclude=_A,**kwargs):
		'Run models in the dbt project.';select,exclude=marshall_selection_args(select,exclude)
		with redirect_stdout(_A):return cast('RunExecutionResult',self.get_task(DbtCommand.RUN,self.get_task_config(select=select,exclude=exclude,**kwargs)).run())
	def test(self,select=_A,exclude=_A,**kwargs):
		'Test models in the dbt project.';select,exclude=marshall_selection_args(select,exclude)
		with redirect_stdout(_A):return self.get_task(DbtCommand.TEST,self.get_task_config(select=select,exclude=exclude,**kwargs)).run()
	def build(self,select=_A,exclude=_A,**kwargs):
		'Build resources in the dbt project.';select,exclude=marshall_selection_args(select,exclude)
		with redirect_stdout(_A):return self.get_task(DbtCommand.BUILD,self.get_task_config(select=select,exclude=exclude,**kwargs)).run()
def marshall_selection_args(select=_A,exclude=_A):
	'Marshall selection arguments to a list of strings.'
	if select is _A:select=[]
	if exclude is _A:exclude=[]
	if isinstance(select,(tuple,set,frozenset)):select=list(select)
	if isinstance(exclude,(tuple,set,frozenset)):exclude=list(exclude)
	if not isinstance(select,list):select=[select]
	if not isinstance(exclude,list):exclude=[exclude]
	return select,exclude
class DbtProjectContainer:
	'Manages multiple DbtProjects.\n\n    A DbtProject corresponds to a single project. This interface is used\n    dbt projects in a single process. It enables basic multitenant servers.\n    '
	def __init__(self):'Initialize the container.';self._projects=OrderedDict();self._default_project=_A
	def get_project(self,project_name):'Primary interface to get a project and execute code.';return self._projects.get(project_name)
	def get_project_by_root_dir(self,root_dir):
		'Get a project by its root directory.';root_dir=os.path.abspath(os.path.normpath(root_dir))
		for project in self._projects.values():
			if os.path.abspath(project.project_root)==root_dir:return project
		return _A
	def get_default_project(self):
		'Get the default project which is the earliest project inserted into the container.';default_project=self._default_project
		if not default_project:return _A
		return self._projects.get(default_project)
	def add_project(self,target=_A,profiles_dir=DEFAULT_PROFILES_DIR,project_dir=_A,threads=1,vars=_e,name_override=''):
		'Add a DbtProject with arguments.';project=DbtProject(target,profiles_dir,project_dir,threads,vars);project_name=name_override or project.config.project_name
		if self._default_project is _A:self._default_project=project_name
		self._projects[project_name]=project;return project
	def add_parsed_project(self,project):
		'Add an already instantiated DbtProject.';self._projects.setdefault(project.config.project_name,project)
		if self._default_project is _A:self._default_project=project.config.project_name
		return project
	def add_project_from_args(self,config):'Add a DbtProject from a DbtConfiguration.';project=DbtProject.from_config(config);self._projects.setdefault(project.config.project_name,project);return project
	def drop_project(self,project_name):
		'Drop a DbtProject.';project=self.get_project(project_name)
		if project is _A:return
		project.clear_internal_caches();project.adapter.connections.cleanup_all();self._projects.pop(project_name)
		if self._default_project==project_name:
			if len(self)>0:self._default_project=list(self._projects.keys())[0]
			else:self._default_project=_A
	def drop_all_projects(self):
		"Drop all DbtProject's in the container.";self._default_project=_A
		for project in self._projects:self.drop_project(project)
	def reparse_all_projects(self):
		'Reparse all projects.'
		for project in self:project.safe_parse_project()
	def registered_projects(self):'Grab all registered project names.';return list(self._projects.keys())
	def __len__(self):'Allow len(DbtProjectContainer).';return len(self._projects)
	def __getitem__(self,project):
		"Allow DbtProjectContainer['jaffle_shop'].";maybe_project=self.get_project(project)
		if maybe_project is _A:raise KeyError(project)
		return maybe_project
	def __setitem__(self,name,project):
		"Allow DbtProjectContainer['jaffle_shop'] = DbtProject."
		if self._default_project is _A:self._default_project=name
		self._projects[name]=project
	def __delitem__(self,project):"Allow del DbtProjectContainer['jaffle_shop'].";self.drop_project(project)
	def __iter__(self):
		'Allow project for project in DbtProjectContainer.'
		for project in self._projects:
			maybe_project=self.get_project(project)
			if maybe_project is _A:continue
			yield maybe_project
	def __contains__(self,project):"Allow 'jaffle_shop' in DbtProjectContainer.";return project in self._projects
	def __repr__(self):'Canonical string representation of DbtProjectContainer instance.';return _G.join(f"Project: {project.project_name}, Dir: {project.project_root}"for project in self)
def has_jinja(query):'Check if a query contains any Jinja control sequences.';return any(seq in query for seq in JINJA_CONTROL_SEQUENCES)
def semvar_to_tuple(semvar):'Convert a semvar to a tuple of ints.';return int(semvar.major or 0),int(semvar.minor or 0),int(semvar.patch or 0)
def _cli_parse(args):B='append';A='store_true';from argparse import ArgumentParser;parser=ArgumentParser(prog=args[0],usage='%(prog)s [options] package.module:app');opt=parser.add_argument;opt('--version',action=A,help='show version number.');opt('-b','--bind',metavar='ADDRESS',help='bind socket to ADDRESS.');opt('-s','--server',default=_v,help='use SERVER as backend.');opt('-p','--plugin',action=B,help='install additional plugin/s.');opt('-c','--conf',action=B,metavar='FILE',help='load config values from FILE.');opt('-C','--param',action=B,metavar='NAME=VALUE',help='override config values.');opt('--debug',action=A,help='start server in debug mode.');opt('--reload',action=A,help='auto-reload on file changes.');opt('app',help='WSGI app entry point.',nargs=_L);cli_args=parser.parse_args(args[1:]);return cli_args,parser
def _cli_patch(cli_args):
	parsed_args,_=_cli_parse(cli_args);opts=parsed_args
	if opts.server:
		if opts.server.startswith('gevent'):import gevent.monkey;gevent.monkey.patch_all()
		elif opts.server.startswith('eventlet'):import eventlet;eventlet.monkey_patch()
py=sys.version_info
py3k=py.major>2
def getargspec(func):spec=getfullargspec(func);kwargs=makelist(spec[0])+makelist(spec.kwonlyargs);return kwargs,spec[1],spec[2],spec[3]
basestring=str
unicode=str
json_loads=lambda s:json_lds(touni(s))
callable=lambda x:hasattr(x,'__call__')
imap=map
def _raise(*a):raise a[0](a[1]).with_traceback(a[2])
def tob(s,enc=_E):
	if isinstance(s,unicode):return s.encode(enc)
	return b''if s is _A else bytes(s)
def touni(s,enc=_E,err=_w):
	if isinstance(s,bytes):return s.decode(enc,err)
	return unicode(''if s is _A else s)
tonat=touni if py3k else tob
def _stderr(*args):
	try:print(*(args),file=sys.stderr)
	except (OSError,AttributeError):pass
def update_wrapper(wrapper,wrapped,*a,**ka):
	try:functools.update_wrapper(wrapper,wrapped,*(a),**ka)
	except AttributeError:pass
def depr(major,minor,cause,fix):
	text='Warning: Use of deprecated feature or API. (Deprecated in Bottle-%d.%d)\nCause: %s\nFix: %s\n'%(major,minor,cause,fix)
	if DEBUG==_w:raise DeprecationWarning(text)
	warnings.warn(text,DeprecationWarning,stacklevel=3);return DeprecationWarning(text)
def makelist(data):
	if isinstance(data,(tuple,list,set,dict)):return list(data)
	elif data:return[data]
	else:return[]
class DictProperty:
	'Property that maps to a key in a local dict-like attribute.'
	def __init__(self,attr,key=_A,read_only=_C):self.attr,self.key,self.read_only=attr,key,read_only
	def __call__(self,func):functools.update_wrapper(self,func,updated=[]);self.getter,self.key=func,self.key or func.__name__;return self
	def __get__(self,obj,cls):
		if obj is _A:return self
		key,storage=self.key,getattr(obj,self.attr)
		if key not in storage:storage[key]=self.getter(obj)
		return storage[key]
	def __set__(self,obj,value):
		if self.read_only:raise AttributeError(_AF)
		getattr(obj,self.attr)[self.key]=value
	def __delete__(self,obj):
		if self.read_only:raise AttributeError(_AF)
		del getattr(obj,self.attr)[self.key]
class cached_property:
	'A property that is only computed once per instance and then replaces\n    itself with an ordinary attribute. Deleting the attribute resets the\n    property.'
	def __init__(self,func):update_wrapper(self,func);self.func=func
	def __get__(self,obj,cls):
		if obj is _A:return self
		value=obj.__dict__[self.func.__name__]=self.func(obj);return value
class lazy_attribute:
	'A property that caches itself to the class object.'
	def __init__(self,func):functools.update_wrapper(self,func,updated=[]);self.getter=func
	def __get__(self,obj,cls):value=self.getter(cls);setattr(cls,self.__name__,value);return value
class BottleException(Exception):'A base class for exceptions used by bottle.'
class RouteError(BottleException):'This is a base class for all routing related exceptions'
class RouteReset(BottleException):'If raised by a plugin or request handler, the route is reset and all\n    plugins are re-applied.'
class RouterUnknownModeError(RouteError):0
class RouteSyntaxError(RouteError):'The route parser found something not supported by this router.'
class RouteBuildError(RouteError):'The route could not be built.'
def _re_flatten(p):
	'Turn all capturing groups in a regular expression pattern into\n    non-capturing groups.'
	if'('not in p:return p
	return re.sub('(\\\\*)(\\(\\?P<[^>]+>|\\((?!\\?))',lambda m:m.group(0)if len(m.group(1))%2 else m.group(1)+'(?:',p)
class Router:
	'A Router is an ordered collection of route->target pairs. It is used to\n    efficiently match WSGI requests against a number of routes and return\n    the first target that satisfies the request. The target may be anything,\n    usually a string, ID or callable object. A route consists of a path-rule\n    and a HTTP method.\n\n    The path-rule is either a static path (e.g. `/contact`) or a dynamic\n    path that contains wildcards (e.g. `/wiki/<page>`). The wildcard syntax\n    and details on the matching order are described in docs:`routing`.\n    ';default_pattern='[^/]+';default_filter='re';_MAX_GROUPS_PER_PATTERN=99
	def __init__(self,strict=_C):self.rules=[];self._groups={};self.builder={};self.static={};self.dyna_routes={};self.dyna_regexes={};self.strict_order=strict;self.filters={'re':lambda conf:(_re_flatten(conf or self.default_pattern),_A,_A),'int':lambda conf:('-?\\d+',int,lambda x:str(int(x))),'float':lambda conf:('-?[\\d.]+',float,lambda x:str(float(x))),'path':lambda conf:('.+?',_A,_A)}
	def add_filter(self,name,func):'Add a filter. The provided function is called with the configuration\n        string as parameter and must return a (regexp, to_python, to_url) tuple.\n        The first element is a string, the last two are callables or None.';self.filters[name]=func
	rule_syntax=re.compile('(\\\\*)(?:(?::([a-zA-Z_][a-zA-Z_0-9]*)?()(?:#(.*?)#)?)|(?:<([a-zA-Z_][a-zA-Z_0-9]*)?(?::([a-zA-Z_]*)(?::((?:\\\\.|[^\\\\>])+)?)?)?>))')
	def _itertokens(self,rule):
		offset,prefix=0,''
		for match in self.rule_syntax.finditer(rule):
			prefix+=rule[offset:match.start()];g=match.groups()
			if g[2]is not _A:depr(0,13,'Use of old route syntax.','Use <name> instead of :name in routes.')
			if len(g[0])%2:prefix+=match.group(0)[len(g[0]):];offset=match.end();continue
			if prefix:yield(prefix,_A,_A)
			name,filtr,conf=g[4:7]if g[2]is _A else g[1:4];yield(name,filtr or _f,conf or _A);offset,prefix=match.end(),''
		if offset<=len(rule)or prefix:yield(prefix+rule[offset:],_A,_A)
	def add(self,rule,method,target,name=_A):
		'Add a new rule or replace the target for an existing rule.';anons=0;keys=[];pattern='';filters=[];builder=[];is_static=_B
		for (key,mode,conf) in self._itertokens(rule):
			if mode:
				is_static=_C
				if mode==_f:mode=self.default_filter
				mask,in_filter,out_filter=self.filters[mode](conf)
				if not key:pattern+='(?:%s)'%mask;key='anon%d'%anons;anons+=1
				else:pattern+=f"(?P<{key}>{mask})";keys.append(key)
				if in_filter:filters.append((key,in_filter))
				builder.append((key,out_filter or str))
			elif key:pattern+=re.escape(key);builder.append((_A,key))
		self.builder[rule]=builder
		if name:self.builder[name]=builder
		if is_static and not self.strict_order:self.static.setdefault(method,{});self.static[method][self.build(rule)]=target,_A;return
		try:re_pattern=re.compile('^(%s)$'%pattern);re_match=re_pattern.match
		except re.error as e:raise RouteSyntaxError(f"Could not add Route: {rule} ({e})")
		if filters:
			def getargs(path):
				url_args=re_match(path).groupdict()
				for (name,wildcard_filter) in filters:
					try:url_args[name]=wildcard_filter(url_args[name])
					except ValueError:raise HTTPError(400,'Path has wrong format.')
				return url_args
		elif re_pattern.groupindex:
			def getargs(path):return re_match(path).groupdict()
		else:getargs=_A
		flatpat=_re_flatten(pattern);whole_rule=rule,flatpat,target,getargs
		if(flatpat,method)in self._groups:
			if DEBUG:msg='Route <%s %s> overwrites a previously defined route';warnings.warn(msg%(method,rule),RuntimeWarning)
			self.dyna_routes[method][self._groups[flatpat,method]]=whole_rule
		else:self.dyna_routes.setdefault(method,[]).append(whole_rule);self._groups[flatpat,method]=len(self.dyna_routes[method])-1
		self._compile(method)
	def _compile(self,method):
		all_rules=self.dyna_routes[method];comborules=self.dyna_regexes[method]=[];maxgroups=self._MAX_GROUPS_PER_PATTERN
		for x in range(0,len(all_rules),maxgroups):some=all_rules[x:x+maxgroups];combined=(flatpat for(_,flatpat,_,_)in some);combined='|'.join('(^%s$)'%flatpat for flatpat in combined);combined=re.compile(combined).match;rules=[(target,getargs)for(_,_,target,getargs)in some];comborules.append((combined,rules))
	def build(self,_name,*anons,**query):
		'Build an URL by filling the wildcards in a rule.';builder=self.builder.get(_name)
		if not builder:raise RouteBuildError('No route with that name.',_name)
		try:
			for (i,value) in enumerate(anons):query['anon%d'%i]=value
			url=''.join([f(query.pop(n))if n else f for(n,f)in builder]);return url if not query else url+_L+urlencode(query)
		except KeyError as E:raise RouteBuildError('Missing URL argument: %r'%E.args[0])
	def match(self,environ):
		'Return a (target, url_args) tuple or raise HTTPError(400/404/405).';A='ANY';verb=environ[_g].upper();path=environ[_M]or _D;methods=(_x,_h,_S,A)if verb==_h else(_x,verb,A)
		for method in methods:
			if method in self.static and path in self.static[method]:target,getargs=self.static[method][path];return target,getargs(path)if getargs else{}
			elif method in self.dyna_regexes:
				for (combined,rules) in self.dyna_regexes[method]:
					match=combined(path)
					if match:target,getargs=rules[match.lastindex-1];return target,getargs(path)if getargs else{}
		allowed=set();nocheck=set(methods)
		for method in set(self.static)-nocheck:
			if path in self.static[method]:allowed.add(method)
		for method in set(self.dyna_regexes)-allowed-nocheck:
			for (combined,rules) in self.dyna_regexes[method]:
				match=combined(path)
				if match:allowed.add(method)
		if allowed:allow_header=','.join(sorted(allowed));raise HTTPError(405,'Method not allowed.',Allow=allow_header)
		raise HTTPError(404,'Not found: '+repr(path))
class Route:
	'This class wraps a route callback along with route specific metadata and\n    configuration and applies Plugins on demand. It is also responsible for\n    turning an URL path rule into a regular expression usable by the Router.\n    '
	def __init__(self,app,rule,method,callback,name=_A,plugins=_A,skiplist=_A,**config):self.app=app;self.rule=rule;self.method=method;self.callback=callback;self.name=name or _A;self.plugins=plugins or[];self.skiplist=skiplist or[];self.config=app.config._make_overlay();self.config.load_dict(config)
	@cached_property
	def call(self):'The route callback with all plugins applied. This property is\n        created on demand and then cached to speed up subsequent requests.';return self._make_callback()
	def reset(self):'Forget any cached values. The next time :attr:`call` is accessed,\n        all plugins are re-applied.';self.__dict__.pop('call',_A)
	def prepare(self):'Do all on-demand work immediately (useful for debugging).';self.call
	def all_plugins(self):
		'Yield all Plugins affecting this route.';unique=set()
		for p in reversed(self.app.plugins+self.plugins):
			if _B in self.skiplist:break
			name=getattr(p,_I,_C)
			if name and(name in self.skiplist or name in unique):continue
			if p in self.skiplist or type(p)in self.skiplist:continue
			if name:unique.add(name)
			yield p
	def _make_callback(self):
		callback=self.callback
		for plugin in self.all_plugins():
			try:
				if hasattr(plugin,'apply'):callback=plugin.apply(callback,self)
				else:callback=plugin(callback)
			except RouteReset:return self._make_callback()
			if callback is not self.callback:update_wrapper(callback,self.callback)
		return callback
	def get_undecorated_callback(self):
		'Return the callback. If the callback is a decorated function, try to\n        recover the original function.';func=self.callback;func=getattr(func,'__func__'if py3k else'im_func',func);closure_attr='__closure__'if py3k else'func_closure'
		while hasattr(func,closure_attr)and getattr(func,closure_attr):
			attributes=getattr(func,closure_attr);func=attributes[0].cell_contents
			if not isinstance(func,FunctionType):func=filter(lambda x:isinstance(x,FunctionType),map(lambda x:x.cell_contents,attributes));func=list(func)[0]
		return func
	def get_callback_args(self):'Return a list of argument names the callback (most likely) accepts\n        as keyword arguments. If the callback is a decorated function, try\n        to recover the original function before inspection.';return getargspec(self.get_undecorated_callback())[0]
	def get_config(self,key,default=_A):'Lookup a config field and return its value, first checking the\n        route.config, then route.app.config.';depr(0,13,'Route.get_config() is deprecated.','The Route.config property already includes values from the application config for missing keys. Access it directly.');return self.config.get(key,default)
	def __repr__(self):cb=self.get_undecorated_callback();return f"<{self.method} {self.rule} -> {cb.__module__}:{cb.__name__}>"
class Bottle:
	'Each Bottle object represents a single, distinct web application and\n    consists of routes, callbacks, plugins, resources and configuration.\n    Instances are callable WSGI applications.\n\n    :param catchall: If true (default), handle all exceptions. Turn off to\n                     let debugging middleware handle exceptions.\n    '
	@lazy_attribute
	def _global_config(cls):cfg=ConfigDict();cfg.meta_set(_W,'validate',bool);return cfg
	def __init__(self,**kwargs):
		self.config=self._global_config._make_overlay();self.config._add_change_listener(functools.partial(self.trigger_hook,_y));self.config.update({_W:_B})
		if kwargs.get(_W)is _C:depr(0,13,'Bottle(catchall) keyword argument.',"The 'catchall' setting is now part of the app configuration. Fix: `app.config['catchall'] = False`");self.config[_W]=_C
		if kwargs.get('autojson')is _C:depr(0,13,'Bottle(autojson) keyword argument.',"The 'autojson' setting is now part of the app configuration. Fix: `app.config['json.enable'] = False`");self.config['json.disable']=_B
		self._mounts=[];self.resources=ResourceManager();self.routes=[];self.router=Router();self.error_handler={};self.plugins=[];self.install(JSONPlugin());self.install(TemplatePlugin())
	catchall=DictProperty(_y,_W);__hook_names=_AG,_z,_AH,_y;__hook_reversed={_z}
	@cached_property
	def _hooks(self):return{name:[]for name in self.__hook_names}
	def add_hook(self,name,func):
		'Attach a callback to a hook. Three hooks are currently implemented:\n\n        before_request\n            Executed once before each request. The request context is\n            available, but no routing has happened yet.\n        after_request\n            Executed once after each request regardless of its outcome.\n        app_reset\n            Called whenever :meth:`Bottle.reset` is called.\n        '
		if name in self.__hook_reversed:self._hooks[name].insert(0,func)
		else:self._hooks[name].append(func)
	def remove_hook(self,name,func):
		'Remove a callback from a hook.'
		if name in self._hooks and func in self._hooks[name]:self._hooks[name].remove(func);return _B
	def trigger_hook(self,__name,*args,**kwargs):'Trigger a hook and return a list of results.';return[hook(*(args),**kwargs)for hook in self._hooks[__name][:]]
	def hook(self,name):
		'Return a decorator that attaches a callback to a hook. See\n        :meth:`add_hook` for details.'
		def decorator(func):self.add_hook(name,func);return func
		return decorator
	def _mount_wsgi(self,prefix,app,**options):
		segments=[p for p in prefix.split(_D)if p]
		if not segments:raise ValueError('WSGI applications cannot be mounted to "/".')
		path_depth=len(segments)
		def mountpoint_wrapper():
			try:
				request.path_shift(path_depth);rs=HTTPResponse([])
				def start_response(status,headerlist,exc_info=_A):
					if exc_info:_raise(*(exc_info))
					if py3k:status=status.encode(_K).decode(_E);headerlist=[(k,v.encode(_K).decode(_E))for(k,v)in headerlist]
					rs.status=status
					for (name,value) in headerlist:rs.add_header(name,value)
					return rs.body.append
				body=app(request.environ,start_response);rs.body=itertools.chain(rs.body,body)if rs.body else body;return rs
			finally:request.path_shift(-path_depth)
		options.setdefault('skip',_B);options.setdefault('method',_x);options.setdefault('mountpoint',{'prefix':prefix,_i:app});options['callback']=mountpoint_wrapper;self.route('/%s/<:re:.*>'%_D.join(segments),**options)
		if not prefix.endswith(_D):self.route(_D+_D.join(segments),**options)
	def _mount_app(self,prefix,app,**options):
		A='_mount.app'
		if app in self._mounts or A in app.config:depr(0,13,'Application mounted multiple times. Falling back to WSGI mount.','Clone application before mounting to a different location.');return self._mount_wsgi(prefix,app,**options)
		if options:depr(0,13,'Unsupported mount options. Falling back to WSGI mount.','Do not specify any route options when mounting bottle application.');return self._mount_wsgi(prefix,app,**options)
		if not prefix.endswith(_D):depr(0,13,"Prefix must end in '/'. Falling back to WSGI mount.","Consider adding an explicit redirect from '/prefix' to '/prefix/' in the parent application.");return self._mount_wsgi(prefix,app,**options)
		self._mounts.append(app);app.config['_mount.prefix']=prefix;app.config[A]=self
		for route in app.routes:route.rule=prefix+route.rule.lstrip(_D);self.add_route(route)
	def mount(self,prefix,app,**options):
		"Mount an application (:class:`Bottle` or plain WSGI) to a specific\n        URL prefix. Example::\n\n            parent_app.mount('/prefix/', child_app)\n\n        :param prefix: path prefix or `mount-point`.\n        :param app: an instance of :class:`Bottle` or a WSGI application.\n\n        Plugins from the parent application are not applied to the routes\n        of the mounted child application. If you need plugins in the child\n        application, install them separately.\n\n        While it is possible to use path wildcards within the prefix path\n        (:class:`Bottle` childs only), it is highly discouraged.\n\n        The prefix path must end with a slash. If you want to access the\n        root of the child application via `/prefix` in addition to\n        `/prefix/`, consider adding a route with a 307 redirect to the\n        parent application.\n        "
		if not prefix.startswith(_D):raise ValueError("Prefix must start with '/'")
		if isinstance(app,Bottle):return self._mount_app(prefix,app,**options)
		else:return self._mount_wsgi(prefix,app,**options)
	def merge(self,routes):
		"Merge the routes of another :class:`Bottle` application or a list of\n        :class:`Route` objects into this application. The routes keep their\n        'owner', meaning that the :data:`Route.app` attribute is not\n        changed."
		if isinstance(routes,Bottle):routes=routes.routes
		for route in routes:self.add_route(route)
	def install(self,plugin):
		'Add a plugin to the list of plugins and prepare it for being\n        applied to all routes of this application. A plugin may be a simple\n        decorator or an object that implements the :class:`Plugin` API.\n        '
		if hasattr(plugin,'setup'):plugin.setup(self)
		if not callable(plugin)and not hasattr(plugin,'apply'):raise TypeError('Plugins must be callable or implement .apply()')
		self.plugins.append(plugin);self.reset();return plugin
	def uninstall(self,plugin):
		'Uninstall plugins. Pass an instance to remove a specific plugin, a type\n        object to remove all plugins that match that type, a string to remove\n        all plugins with a matching ``name`` attribute or ``True`` to remove all\n        plugins. Return the list of removed plugins.';removed,remove=[],plugin
		for (i,plugin) in list(enumerate(self.plugins))[::-1]:
			if remove is _B or remove is plugin or remove is type(plugin)or getattr(plugin,_I,_B)==remove:
				removed.append(plugin);del self.plugins[i]
				if hasattr(plugin,_P):plugin.close()
		if removed:self.reset()
		return removed
	def reset(self,route=_A):
		'Reset all routes (force plugins to be re-applied) and clear all\n        caches. If an ID or route object is given, only that specific route\n        is affected.'
		if route is _A:routes=self.routes
		elif isinstance(route,Route):routes=[route]
		else:routes=[self.routes[route]]
		for route in routes:route.reset()
		if DEBUG:
			for route in routes:route.prepare()
		self.trigger_hook(_AH)
	def close(self):
		'Close the application and all installed plugins.'
		for plugin in self.plugins:
			if hasattr(plugin,_P):plugin.close()
	def run(self,**kwargs):'Calls :func:`run` with the same parameters.';run(self,**kwargs)
	def match(self,environ):'Search for a matching route and return a (:class:`Route`, urlargs)\n        tuple. The second value is a dictionary with parameters extracted\n        from the URL. Raise :exc:`HTTPError` (404/405) on a non-match.';return self.router.match(environ)
	def get_url(self,routename,**kargs):'Return a string that matches a named route';scriptname=request.environ.get(_X,'').strip(_D)+_D;location=self.router.build(routename,**kargs).lstrip(_D);return urljoin(urljoin(_D,scriptname),location)
	def add_route(self,route):
		'Add a route object, but do not change the :data:`Route.app`\n        attribute.';self.routes.append(route);self.router.add(route.rule,route.method,route,name=route.name)
		if DEBUG:route.prepare()
	def route(self,path=_A,method=_S,callback=_A,name=_A,apply=_A,skip=_A,**config):
		"A decorator to bind a function to a request URL. Example::\n\n            @app.route('/hello/<name>')\n            def hello(name):\n                return 'Hello %s' % name\n\n        The ``<name>`` part is a wildcard. See :class:`Router` for syntax\n        details.\n\n        :param path: Request path or a list of paths to listen to. If no\n          path is specified, it is automatically generated from the\n          signature of the function.\n        :param method: HTTP method (`GET`, `POST`, `PUT`, ...) or a list of\n          methods to listen to. (default: `GET`)\n        :param callback: An optional shortcut to avoid the decorator\n          syntax. ``route(..., callback=func)`` equals ``route(...)(func)``\n        :param name: The name for this route. (default: None)\n        :param apply: A decorator or plugin or a list of plugins. These are\n          applied to the route callback in addition to installed plugins.\n        :param skip: A list of plugins, plugin classes or names. Matching\n          plugins are not installed to this route. ``True`` skips all.\n\n        Any additional keyword arguments are stored as route-specific\n        configuration and passed to plugins (see :meth:`Plugin.apply`).\n        "
		if callable(path):path,callback=_A,path
		plugins=makelist(apply);skiplist=makelist(skip)
		def decorator(callback):
			if isinstance(callback,basestring):callback=load(callback)
			for rule in makelist(path)or yieldroutes(callback):
				for verb in makelist(method):verb=verb.upper();route=Route(self,rule,verb,callback,name=name,plugins=plugins,skiplist=skiplist,**config);self.add_route(route)
			return callback
		return decorator(callback)if callback else decorator
	def get(self,path=_A,method=_S,**options):'Equals :meth:`route`.';return self.route(path,method,**options)
	def post(self,path=_A,method=_T,**options):'Equals :meth:`route` with a ``POST`` method parameter.';return self.route(path,method,**options)
	def put(self,path=_A,method='PUT',**options):'Equals :meth:`route` with a ``PUT`` method parameter.';return self.route(path,method,**options)
	def delete(self,path=_A,method='DELETE',**options):'Equals :meth:`route` with a ``DELETE`` method parameter.';return self.route(path,method,**options)
	def patch(self,path=_A,method='PATCH',**options):'Equals :meth:`route` with a ``PATCH`` method parameter.';return self.route(path,method,**options)
	def error(self,code=500,callback=_A):
		"Register an output handler for a HTTP error code. Can\n        be used as a decorator or called directly ::\n\n            def error_handler_500(error):\n                return 'error_handler_500'\n\n            app.error(code=500, callback=error_handler_500)\n\n            @app.error(404)\n            def error_handler_404(error):\n                return 'error_handler_404'\n\n        "
		def decorator(callback):
			if isinstance(callback,basestring):callback=load(callback)
			self.error_handler[int(code)]=callback;return callback
		return decorator(callback)if callback else decorator
	def default_error_handler(self,res):return tob(template(ERROR_PAGE_TEMPLATE,e=res,template_settings=dict(name='__ERROR_PAGE_TEMPLATE')))
	def _handle(self,environ):
		path=environ['bottle.raw_path']=environ[_M]
		if py3k:environ[_M]=path.encode(_K).decode(_E,_A0)
		environ[_AI]=self;request.bind(environ);response.bind()
		try:
			while _B:
				out=_A
				try:self.trigger_hook(_AG);route,args=self.router.match(environ);environ['route.handle']=route;environ[_AJ]=route;environ[_AK]=args;out=route.call(**args);break
				except HTTPResponse as E:out=E;break
				except RouteReset:depr(0,13,'RouteReset exception deprecated','Call route.call() after route.reset() and return the result.');route.reset();continue
				finally:
					if isinstance(out,HTTPResponse):out.apply(response)
					try:self.trigger_hook(_z)
					except HTTPResponse as E:out=E;out.apply(response)
		except (KeyboardInterrupt,SystemExit,MemoryError):raise
		except Exception as E:
			if not self.catchall:raise
			stacktrace=format_exc();environ[_j].write(stacktrace);environ[_j].flush();environ[_A1]=sys.exc_info();out=HTTPError(500,'Internal Server Error',E,stacktrace);out.apply(response)
		return out
	def _cast(self,out,peek=_A):
		'Try to convert the parameter into something WSGI compatible and set\n        correct HTTP headers when possible.\n        Support: False, str, unicode, dict, HTTPResponse, HTTPError, file-like,\n        iterable of strings and iterable of unicodes\n        ';A='wsgi.file_wrapper'
		if not out:
			if _J not in response:response[_J]=0
			return[]
		if isinstance(out,(tuple,list))and isinstance(out[0],(bytes,unicode)):out=out[0][0:0].join(out)
		if isinstance(out,unicode):out=out.encode(response.charset)
		if isinstance(out,bytes):
			if _J not in response:response[_J]=len(out)
			return[out]
		if isinstance(out,HTTPError):out.apply(response);out=self.error_handler.get(out.status_code,self.default_error_handler)(out);return self._cast(out)
		if isinstance(out,HTTPResponse):out.apply(response);return self._cast(out.body)
		if hasattr(out,'read'):
			if A in request.environ:return request.environ[A](out)
			elif hasattr(out,_P)or not hasattr(out,'__iter__'):return WSGIFileWrapper(out)
		try:
			iout=iter(out);first=next(iout)
			while not first:first=next(iout)
		except StopIteration:return self._cast('')
		except HTTPResponse as E:first=E
		except (KeyboardInterrupt,SystemExit,MemoryError):raise
		except Exception as error:
			if not self.catchall:raise
			first=HTTPError(500,'Unhandled exception',error,format_exc())
		if isinstance(first,HTTPResponse):return self._cast(first)
		elif isinstance(first,bytes):new_iter=itertools.chain([first],iout)
		elif isinstance(first,unicode):encoder=lambda x:x.encode(response.charset);new_iter=imap(encoder,itertools.chain([first],iout))
		else:msg='Unsupported response type: %s'%type(first);return self._cast(HTTPError(500,msg))
		if hasattr(out,_P):new_iter=_closeiter(new_iter,out.close)
		return new_iter
	def wsgi(self,environ,start_response):
		'The bottle WSGI-interface.'
		try:
			out=self._cast(self._handle(environ))
			if response._status_code in(100,101,204,304)or environ[_g]==_h:
				if hasattr(out,_P):out.close()
				out=[]
			exc_info=environ.get(_A1)
			if exc_info is not _A:del environ[_A1]
			start_response(response._wsgi_status_line(),response.headerlist,exc_info);return out
		except (KeyboardInterrupt,SystemExit,MemoryError):raise
		except Exception as E:
			if not self.catchall:raise
			err='<h1>Critical error while processing request: %s</h1>'%html_escape(environ.get(_M,_D))
			if DEBUG:err+='<h2>Error:</h2>\n<pre>\n%s\n</pre>\n<h2>Traceback:</h2>\n<pre>\n%s\n</pre>\n'%(html_escape(repr(E)),html_escape(format_exc()))
			environ[_j].write(err);environ[_j].flush();headers=[(_N,_AL)];start_response('500 INTERNAL SERVER ERROR',headers,sys.exc_info());return[tob(err)]
	def __call__(self,environ,start_response):"Each instance of :class:'Bottle' is a WSGI application.";return self.wsgi(environ,start_response)
	def __enter__(self):'Use this application as default for all module-level shortcuts.';default_app.push(self);return self
	def __exit__(self,exc_type,exc_value,traceback):default_app.pop()
	def __setattr__(self,name,value):
		if name in self.__dict__:raise AttributeError('Attribute %s already defined. Plugin conflict?'%name)
		self.__dict__[name]=value
class BaseRequest:
	"A wrapper for WSGI environment dictionaries that adds a lot of\n    convenient access methods and properties. Most of them are read-only.\n\n    Adding new attributes to a request actually adds them to the environ\n    dictionary (as 'bottle.request.ext.<name>'). This is the recommended\n    way to store and access request-specific data.\n    ";__slots__=_F,;MEMFILE_MAX=102400
	def __init__(self,environ=_A):'Wrap a WSGI environ dictionary.';self.environ={}if environ is _A else environ;self.environ['bottle.request']=self
	@DictProperty(_F,_AI,read_only=_B)
	def app(self):'Bottle application handling this request.';raise RuntimeError('This request is not connected to an application.')
	@DictProperty(_F,_AJ,read_only=_B)
	def route(self):'The bottle :class:`Route` object that matches this request.';raise RuntimeError(_AM)
	@DictProperty(_F,_AK,read_only=_B)
	def url_args(self):'The arguments extracted from the URL.';raise RuntimeError(_AM)
	@property
	def path(self):'The value of ``PATH_INFO`` with exactly one prefixed slash (to fix\n        broken clients and avoid the "empty path" edge case).';return _D+self.environ.get(_M,'').lstrip(_D)
	@property
	def method(self):'The ``REQUEST_METHOD`` value as an uppercase string.';return self.environ.get(_g,_S).upper()
	@DictProperty(_F,'bottle.request.headers',read_only=_B)
	def headers(self):'A :class:`WSGIHeaderDict` that provides case-insensitive access to\n        HTTP request headers.';return WSGIHeaderDict(self.environ)
	def get_header(self,name,default=_A):'Return the value of a request header, or a given default value.';return self.headers.get(name,default)
	@DictProperty(_F,'bottle.request.cookies',read_only=_B)
	def cookies(self):'Cookies parsed into a :class:`FormsDict`. Signed cookies are NOT\n        decoded. Use :meth:`get_cookie` if you expect signed cookies.';cookies=SimpleCookie(self.environ.get('HTTP_COOKIE','')).values();return FormsDict((c.key,c.value)for c in cookies)
	def get_cookie(self,key,default=_A,secret=_A,digestmod=hashlib.sha256):
		'Return the content of a cookie. To read a `Signed Cookie`, the\n        `secret` must match the one used to create the cookie (see\n        :meth:`BaseResponse.set_cookie`). If anything goes wrong (missing\n        cookie or wrong signature), return a default value.';value=self.cookies.get(key)
		if secret:
			if value and value.startswith('!')and _L in value:
				sig,msg=map(tob,value[1:].split(_L,1));hash=hmac.new(tob(secret),msg,digestmod=digestmod).digest()
				if _lscmp(sig,base64.b64encode(hash)):
					dst=pickle.loads(base64.b64decode(msg))
					if dst and dst[0]==key:return dst[1]
			return default
		return value or default
	@DictProperty(_F,'bottle.request.query',read_only=_B)
	def query(self):
		'The :attr:`query_string` parsed into a :class:`FormsDict`. These\n        values are sometimes called "URL arguments" or "GET parameters", but\n        not to be confused with "URL wildcards" as they are provided by the\n        :class:`Router`.';get=self.environ['bottle.get']=FormsDict();pairs=_parse_qsl(self.environ.get(_Y,''))
		for (key,value) in pairs:get[key]=value
		return get
	@DictProperty(_F,'bottle.request.forms',read_only=_B)
	def forms(self):
		'Form values parsed from an `url-encoded` or `multipart/form-data`\n        encoded POST or PUT request body. The result is returned as a\n        :class:`FormsDict`. All keys and values are strings. File uploads\n        are stored separately in :attr:`files`.';forms=FormsDict();forms.recode_unicode=self.POST.recode_unicode
		for (name,item) in self.POST.allitems():
			if not isinstance(item,FileUpload):forms[name]=item
		return forms
	@DictProperty(_F,'bottle.request.params',read_only=_B)
	def params(self):
		'A :class:`FormsDict` with the combined values of :attr:`query` and\n        :attr:`forms`. File uploads are stored in :attr:`files`.';params=FormsDict()
		for (key,value) in self.query.allitems():params[key]=value
		for (key,value) in self.forms.allitems():params[key]=value
		return params
	@DictProperty(_F,'bottle.request.files',read_only=_B)
	def files(self):
		'File uploads parsed from `multipart/form-data` encoded POST or PUT\n        request body. The values are instances of :class:`FileUpload`.\n\n        ';files=FormsDict();files.recode_unicode=self.POST.recode_unicode
		for (name,item) in self.POST.allitems():
			if isinstance(item,FileUpload):files[name]=item
		return files
	@DictProperty(_F,'bottle.request.json',read_only=_B)
	def json(self):
		'If the ``Content-Type`` header is ``application/json`` or\n        ``application/json-rpc``, this property holds the parsed content\n        of the request body. Only requests smaller than :attr:`MEMFILE_MAX`\n        are processed to avoid memory exhaustion.\n        Invalid JSON raises a 400 error response.\n        ';ctype=self.environ.get(_k,'').lower().split(';')[0]
		if ctype in(_A2,'application/json-rpc'):
			b=self._get_body_string(self.MEMFILE_MAX)
			if not b:return _A
			try:return json_loads(b)
			except (ValueError,TypeError):raise HTTPError(400,'Invalid JSON')
		return _A
	def _iter_body(self,read,bufsize):
		maxread=max(0,self.content_length)
		while maxread:
			part=read(min(maxread,bufsize))
			if not part:break
			yield part;maxread-=len(part)
	@staticmethod
	def _iter_chunked(read,bufsize):
		err=HTTPError(400,'Error while parsing chunked transfer body.');rn,sem,bs=tob('\r\n'),tob(';'),tob('')
		while _B:
			header=read(1)
			while header[-2:]!=rn:
				c=read(1);header+=c
				if not c:raise err
				if len(header)>bufsize:raise err
			size,_,_=header.partition(sem)
			try:maxread=int(tonat(size.strip()),16)
			except ValueError:raise err
			if maxread==0:break
			buff=bs
			while maxread>0:
				if not buff:buff=read(min(maxread,bufsize))
				part,buff=buff[:maxread],buff[maxread:]
				if not part:raise err
				yield part;maxread-=len(part)
			if read(2)!=rn:raise err
	@DictProperty(_F,'bottle.request.body',read_only=_B)
	def _body(self):
		try:read_func=self.environ[_Z].read
		except KeyError:self.environ[_Z]=BytesIO();return self.environ[_Z]
		body_iter=self._iter_chunked if self.chunked else self._iter_body;body,body_size,is_temp_file=BytesIO(),0,_C
		for part in body_iter(read_func,self.MEMFILE_MAX):
			body.write(part);body_size+=len(part)
			if not is_temp_file and body_size>self.MEMFILE_MAX:body,tmp=NamedTemporaryFile(mode='w+b'),body;body.write(tmp.getvalue());del tmp;is_temp_file=_B
		self.environ[_Z]=body;body.seek(0);return body
	def _get_body_string(self,maxread):
		'Read body into a string. Raise HTTPError(413) on requests that are\n        too large.';A='Request entity too large'
		if self.content_length>maxread:raise HTTPError(413,A)
		data=self.body.read(maxread+1)
		if len(data)>maxread:raise HTTPError(413,A)
		return data
	@property
	def body(self):'The HTTP request body as a seek-able file-like object. Depending on\n        :attr:`MEMFILE_MAX`, this is either a temporary file or a\n        :class:`io.BytesIO` instance. Accessing this property for the first\n        time reads and replaces the ``wsgi.input`` environ variable.\n        Subsequent accesses just do a `seek(0)` on the file object.';self._body.seek(0);return self._body
	@property
	def chunked(self):'True if Chunked transfer encoding was.';return'chunked'in self.environ.get('HTTP_TRANSFER_ENCODING','').lower()
	GET=query
	@DictProperty(_F,'bottle.request.post',read_only=_B)
	def POST(self):
		'The values of :attr:`forms` and :attr:`files` combined into a single\n        :class:`FormsDict`. Values are either strings (form values) or\n        instances of :class:`cgi.FieldStorage` (file uploads).\n        ';post=FormsDict()
		if not self.content_type.startswith('multipart/'):
			body=tonat(self._get_body_string(self.MEMFILE_MAX),_K)
			for (key,value) in _parse_qsl(body):post[key]=value
			return post
		safe_env={_Y:''}
		for key in (_g,_k,_A3):
			if key in self.environ:safe_env[key]=self.environ[key]
		args=dict(fp=self.body,environ=safe_env,keep_blank_values=_B)
		if py3k:args['encoding']=_E;post.recode_unicode=_C
		data=cgi.FieldStorage(**args);self['_cgi.FieldStorage']=data;data=data.list or[]
		for item in data:
			if item.filename is _A:post[item.name]=item.value
			else:post[item.name]=FileUpload(item.file,item.name,item.filename,item.headers)
		return post
	@property
	def url(self):'The full request URI including hostname and scheme. If your app\n        lives behind a reverse proxy or load balancer and you get confusing\n        results, make sure that the ``X-Forwarded-Host`` header is set\n        correctly.';return self.urlparts.geturl()
	@DictProperty(_F,'bottle.request.urlparts',read_only=_B)
	def urlparts(self):
		'The :attr:`url` string as an :class:`urlparse.SplitResult` tuple.\n        The tuple contains (scheme, host, path, query_string and fragment),\n        but the fragment is always empty because it is not visible to the\n        server.';A='http';env=self.environ;http=env.get('HTTP_X_FORWARDED_PROTO')or env.get('wsgi.url_scheme',A);host=env.get('HTTP_X_FORWARDED_HOST')or env.get('HTTP_HOST')
		if not host:
			host=env.get('SERVER_NAME',_A4);port=env.get('SERVER_PORT')
			if port and port!=('80'if http==A else'443'):host+=_O+port
		path=urlquote(self.fullpath);return UrlSplitResult(http,host,path,env.get(_Y),'')
	@property
	def fullpath(self):'Request path including :attr:`script_name` (if present).';return urljoin(self.script_name,self.path.lstrip(_D))
	@property
	def query_string(self):'The raw :attr:`query` part of the URL (everything in between ``?``\n        and ``#``) as a string.';return self.environ.get(_Y,'')
	@property
	def script_name(self):"The initial portion of the URL's `path` that was removed by a higher\n        level (server or routing middleware) before the application was\n        called. This script path is returned with leading and tailing\n        slashes.";script_name=self.environ.get(_X,'').strip(_D);return _D+script_name+_D if script_name else _D
	def path_shift(self,shift=1):'Shift path segments from :attr:`path` to :attr:`script_name` and\n         vice versa.\n\n        :param shift: The number of path segments to shift. May be negative\n                      to change the shift direction. (default: 1)\n        ';script,path=path_shift(self.environ.get(_X,_D),self.path,shift);self[_X],self[_M]=script,path
	@property
	def content_length(self):'The request body length as an integer. The client is responsible to\n        set this header. Otherwise, the real length of the body is unknown\n        and -1 is returned. In this case, :attr:`body` will be empty.';return int(self.environ.get(_A3)or-1)
	@property
	def content_type(self):'The Content-Type header as a lowercase-string (default: empty).';return self.environ.get(_k,'').lower()
	@property
	def is_xhr(self):'True if the request was triggered by a XMLHttpRequest. This only\n        works with JavaScript libraries that support the `X-Requested-With`\n        header (most of the popular libraries do).';requested_with=self.environ.get('HTTP_X_REQUESTED_WITH','');return requested_with.lower()=='xmlhttprequest'
	@property
	def is_ajax(self):'Alias for :attr:`is_xhr`. "Ajax" is not the right term.';return self.is_xhr
	@property
	def auth(self):
		'HTTP authentication data as a (user, password) tuple. This\n        implementation currently supports basic (not digest) authentication\n        only. If the authentication happened at a higher level (e.g. in the\n        front web-server or a middleware), the password field is None, but\n        the user field is looked up from the ``REMOTE_USER`` environ\n        variable. On any errors, None is returned.';basic=parse_auth(self.environ.get('HTTP_AUTHORIZATION',''))
		if basic:return basic
		ruser=self.environ.get('REMOTE_USER')
		if ruser:return ruser,_A
		return _A
	@property
	def remote_route(self):
		'A list of all IPs that were involved in this request, starting with\n        the client IP and followed by zero or more proxies. This does only\n        work if all proxies support the ```X-Forwarded-For`` header. Note\n        that this information can be forged by malicious clients.';proxy=self.environ.get('HTTP_X_FORWARDED_FOR')
		if proxy:return[ip.strip()for ip in proxy.split(',')]
		remote=self.environ.get('REMOTE_ADDR');return[remote]if remote else[]
	@property
	def remote_addr(self):'The client IP as a string. Note that this information can be forged\n        by malicious clients.';route=self.remote_route;return route[0]if route else _A
	def copy(self):'Return a new :class:`Request` with a shallow :attr:`environ` copy.';return Request(self.environ.copy())
	def get(self,value,default=_A):return self.environ.get(value,default)
	def __getitem__(self,key):return self.environ[key]
	def __delitem__(self,key):self[key]='';del self.environ[key]
	def __iter__(self):return iter(self.environ)
	def __len__(self):return len(self.environ)
	def keys(self):return self.environ.keys()
	def __setitem__(self,key,value):
		'Change an environ value and clear all caches that depend on it.';A='params'
		if self.environ.get('bottle.request.readonly'):raise KeyError('The environ dictionary is read-only.')
		self.environ[key]=value;todelete=()
		if key==_Z:todelete='body','forms','files',A,'post','json'
		elif key==_Y:todelete='query',A
		elif key.startswith(_A5):todelete='headers','cookies'
		for key in todelete:self.environ.pop('bottle.request.'+key,_A)
	def __repr__(self):return f"<{self.__class__.__name__}: {self.method} {self.url}>"
	def __getattr__(self,name):
		'Search in self.environ for additional user defined attributes.'
		try:var=self.environ[_A6%name];return var.__get__(self)if hasattr(var,'__get__')else var
		except KeyError:raise AttributeError('Attribute %r not defined.'%name)
	def __setattr__(self,name,value):
		if name==_F:return object.__setattr__(self,name,value)
		key=_A6%name
		if hasattr(self,name):raise AttributeError('Attribute already defined: %s'%name)
		self.environ[key]=value
	def __delattr__(self,name):
		try:del self.environ[_A6%name]
		except KeyError:raise AttributeError('Attribute not defined: %s'%name)
def _hkey(key):
	if _G in key or _l in key or'\x00'in key:raise ValueError('Header names must not contain control characters: %r'%key)
	return key.title().replace('_','-')
def _hval(value):
	value=tonat(value)
	if _G in value or _l in value or'\x00'in value:raise ValueError('Header value must not contain control characters: %r'%value)
	return value
class HeaderProperty:
	def __init__(self,name,reader=_A,writer=_A,default=''):self.name,self.default=name,default;self.reader,self.writer=reader,writer;self.__doc__='Current value of the %r header.'%name.title()
	def __get__(self,obj,_):
		if obj is _A:return self
		value=obj.get_header(self.name,self.default);return self.reader(value)if self.reader else value
	def __set__(self,obj,value):obj[self.name]=self.writer(value)if self.writer else value
	def __delete__(self,obj):del obj[self.name]
class BaseResponse:
	"Storage class for a response body as well as headers and cookies.\n\n    This class does support dict-like case-insensitive item-access to\n    headers, but is NOT a dict. Most notably, iterating over a response\n    yields parts of the body and not the headers.\n\n    :param body: The response body as one of the supported types.\n    :param status: Either an HTTP status code (e.g. 200) or a status line\n                   including the reason phrase (e.g. '200 OK').\n    :param headers: A dictionary or a list of name-value pairs.\n\n    Additional keyword arguments are added to the list of headers.\n    Underscores in the header name are replaced with dashes.\n    ";default_status=200;default_content_type=_AL;bad_headers={204:frozenset((_N,_J)),304:frozenset(('Allow','Content-Encoding','Content-Language',_J,_AN,_N,'Content-Md5',_AO))}
	def __init__(self,body='',status=_A,headers=_A,**more_headers):
		self._cookies=_A;self._headers={};self.body=body;self.status=status or self.default_status
		if headers:
			if isinstance(headers,dict):headers=headers.items()
			for (name,value) in headers:self.add_header(name,value)
		if more_headers:
			for (name,value) in more_headers.items():self.add_header(name,value)
	def copy(self,cls=_A):
		'Returns a copy of self.';cls=cls or BaseResponse;copy=cls();copy.status=self.status;copy._headers={k:v[:]for(k,v)in self._headers.items()}
		if self._cookies:
			cookies=copy._cookies=SimpleCookie()
			for (k,v) in self._cookies.items():cookies[k]=v.value;cookies[k].update(v)
		return copy
	def __iter__(self):return iter(self.body)
	def close(self):
		if hasattr(self.body,_P):self.body.close()
	@property
	def status_line(self):'The HTTP status line as a string (e.g. ``404 Not Found``).';return self._status_line
	@property
	def status_code(self):'The HTTP status code as an integer (e.g. 404).';return self._status_code
	def _set_status(self,status):
		if isinstance(status,int):code,status=status,_HTTP_STATUS_LINES.get(status)
		elif' 'in status:
			if _G in status or _l in status or'\x00'in status:raise ValueError('Status line must not include control chars.')
			status=status.strip();code=int(status.split()[0])
		else:raise ValueError('String status line without a reason phrase.')
		if not 100<=code<=999:raise ValueError('Status code out of range.')
		self._status_code=code;self._status_line=str(status or'%d Unknown'%code)
	def _get_status(self):return self._status_line
	status=property(_get_status,_set_status,_A,' A writeable property to change the HTTP response status. It accepts\n            either a numeric code (100-999) or a string with a custom reason\n            phrase (e.g. "404 Brain not found"). Both :data:`status_line` and\n            :data:`status_code` are updated accordingly. The return value is\n            always a status string. ');del _get_status,_set_status
	@property
	def headers(self):'An instance of :class:`HeaderDict`, a case-insensitive dict-like\n        view on the response headers.';hdict=HeaderDict();hdict.dict=self._headers;return hdict
	def __contains__(self,name):return _hkey(name)in self._headers
	def __delitem__(self,name):del self._headers[_hkey(name)]
	def __getitem__(self,name):return self._headers[_hkey(name)][-1]
	def __setitem__(self,name,value):self._headers[_hkey(name)]=[_hval(value)]
	def get_header(self,name,default=_A):'Return the value of a previously defined header. If there is no\n        header with that name, return a default value.';return self._headers.get(_hkey(name),[default])[-1]
	def set_header(self,name,value):'Create a new response header, replacing any previously defined\n        headers with the same name.';self._headers[_hkey(name)]=[_hval(value)]
	def add_header(self,name,value):'Add an additional response header, not removing duplicates.';self._headers.setdefault(_hkey(name),[]).append(_hval(value))
	def iter_headers(self):'Yield (header, value) tuples, skipping headers that are not\n        allowed with the current response status code.';return self.headerlist
	def _wsgi_status_line(self):
		'WSGI conform status line (latin1-encodeable)'
		if py3k:return self._status_line.encode(_E).decode(_K)
		return self._status_line
	@property
	def headerlist(self):
		'WSGI conform list of (header, value) tuples.';out=[];headers=list(self._headers.items())
		if _N not in self._headers:headers.append((_N,[self.default_content_type]))
		if self._status_code in self.bad_headers:bad_headers=self.bad_headers[self._status_code];headers=[h for h in headers if h[0]not in bad_headers]
		out+=[(name,val)for(name,vals)in headers for val in vals]
		if self._cookies:
			for c in self._cookies.values():out.append(('Set-Cookie',_hval(c.OutputString())))
		if py3k:out=[(k,v.encode(_E).decode(_K))for(k,v)in out]
		return out
	content_type=HeaderProperty(_N);content_length=HeaderProperty(_J,reader=int,default=-1);expires=HeaderProperty('Expires',reader=lambda x:datetime.utcfromtimestamp(parse_date(x)),writer=lambda x:http_date(x))
	@property
	def charset(self,default='UTF-8'):
		'Return the charset specified in the content-type header (default: utf8).';A='charset='
		if A in self.content_type:return self.content_type.split(A)[-1].split(';')[0].strip()
		return default
	def set_cookie(self,name,value,secret=_A,digestmod=hashlib.sha256,**options):
		'Create a new cookie or replace an old one. If the `secret` parameter is\n        set, create a `Signed Cookie` (described below).\n\n        :param name: the name of the cookie.\n        :param value: the value of the cookie.\n        :param secret: a signature key required for signed cookies.\n\n        Additionally, this method accepts all RFC 2109 attributes that are\n        supported by :class:`cookie.Morsel`, including:\n\n        :param maxage: maximum age in seconds. (default: None)\n        :param expires: a datetime object or UNIX timestamp. (default: None)\n        :param domain: the domain that is allowed to read the cookie.\n          (default: current domain)\n        :param path: limits the cookie to a given path (default: current path)\n        :param secure: limit the cookie to HTTPS connections (default: off).\n        :param httponly: prevents client-side javascript to read this cookie\n          (default: off, requires Python 2.6 or newer).\n        :param samesite: Control or disable third-party use for this cookie.\n          Possible values: `lax`, `strict` or `none` (default).\n\n        If neither `expires` nor `maxage` is set (default), the cookie will\n        expire at the end of the browser session (as soon as the browser\n        window is closed).\n\n        Signed cookies may store any pickle-able object and are\n        cryptographically signed to prevent manipulation. Keep in mind that\n        cookies are limited to 4kb in most browsers.\n\n        Warning: Pickle is a potentially dangerous format. If an attacker\n        gains access to the secret key, he could forge cookies that execute\n        code on server side if unpickled. Using pickle is discouraged and\n        support for it will be removed in later versions of bottle.\n\n        Warning: Signed cookies are not encrypted (the client can still see\n        the content) and not copy-protected (the client can restore an old\n        cookie). The main intention is to make pickling and unpickling\n        save, not to store secret information at client side.\n        ';B='none';A='samesite'
		if not self._cookies:self._cookies=SimpleCookie()
		if py<(3,8,0):Morsel._reserved.setdefault(A,'SameSite')
		if secret:
			if not isinstance(value,basestring):depr(0,13,'Pickling of arbitrary objects into cookies is deprecated.','Only store strings in cookies. JSON strings are fine, too.')
			encoded=base64.b64encode(pickle.dumps([name,value],-1));sig=base64.b64encode(hmac.new(tob(secret),encoded,digestmod=digestmod).digest());value=touni(tob('!')+sig+tob(_L)+encoded)
		elif not isinstance(value,basestring):raise TypeError('Secret key required for non-string cookies.')
		if len(name)+len(value)>3800:raise ValueError('Content does not fit into a cookie.')
		self._cookies[name]=value
		for (key,value) in options.items():
			if key in('max_age','maxage'):
				key='max-age'
				if isinstance(value,timedelta):value=value.seconds+value.days*24*3600
			if key=='expires':value=http_date(value)
			if key in('same_site',A):
				key,value=A,(value or B).lower()
				if value not in('lax',_w,B):raise CookieError('Invalid value for SameSite')
			if key in('secure','httponly')and not value:continue
			self._cookies[name][key]=value
	def delete_cookie(self,key,**kwargs):'Delete a cookie. Be sure to use the same `domain` and `path`\n        settings as used to create the cookie.';kwargs['max_age']=-1;kwargs['expires']=0;self.set_cookie(key,'',**kwargs)
	def __repr__(self):
		out=''
		for (name,value) in self.headerlist:out+=f"{name.title()}: {value.strip()}\n"
		return out
def _local_property():
	ls=threading.local()
	def fget(_):
		try:return ls.var
		except AttributeError:raise RuntimeError('Request context not initialized.')
	def fset(_,value):ls.var=value
	def fdel(_):del ls.var
	return property(fget,fset,fdel,'Thread-local property')
class LocalRequest(BaseRequest):'A thread-local subclass of :class:`BaseRequest` with a different\n    set of attributes for each thread. There is usually only one global\n    instance of this class (:data:`request`). If accessed during a\n    request/response cycle, this instance always refers to the *current*\n    request (even on a multithreaded server).';bind=BaseRequest.__init__;environ=_local_property()
class LocalResponse(BaseResponse):'A thread-local subclass of :class:`BaseResponse` with a different\n    set of attributes for each thread. There is usually only one global\n    instance of this class (:data:`response`). Its attributes are used\n    to build the HTTP response at the end of the request/response cycle.\n    ';bind=BaseResponse.__init__;_status_line=_local_property();_status_code=_local_property();_cookies=_local_property();_headers=_local_property();body=_local_property()
Request=BaseRequest
Response=BaseResponse
class HTTPResponse(Response,BottleException):
	def __init__(self,body='',status=_A,headers=_A,**more_headers):super().__init__(body,status,headers,**more_headers)
	def apply(self,other):other._status_code=self._status_code;other._status_line=self._status_line;other._headers=self._headers;other._cookies=self._cookies;other.body=self.body
class HTTPError(HTTPResponse):
	default_status=500
	def __init__(self,status=_A,body=_A,exception=_A,traceback=_A,**more_headers):self.exception=exception;self.traceback=traceback;super().__init__(body,status,**more_headers)
class PluginError(BottleException):0
class JSONPlugin:
	name='json';api=2
	def __init__(self,json_dumps=json_dumps):self.json_dumps=json_dumps
	def setup(self,app):app.config._define('json.enable',default=_B,validate=bool,help='Enable or disable automatic dict->json filter.');app.config._define('json.ascii',default=_C,validate=bool,help='Use only 7-bit ASCII characters in output.');app.config._define('json.indent',default=_B,validate=bool,help='Add whitespace to make json more readable.');app.config._define('json.dump_func',default=_A,help='If defined, use this function to transform dict into json. The other options no longer apply.')
	def apply(self,callback,route):
		dumps=self.json_dumps
		if not self.json_dumps:return callback
		@functools.wraps(callback)
		def wrapper(*a,**ka):
			try:rv=callback(*(a),**ka)
			except HTTPResponse as resp:rv=resp
			if isinstance(rv,dict):json_response=dumps(rv);response.content_type=_A2;return json_response
			elif isinstance(rv,HTTPResponse)and isinstance(rv.body,dict):rv.body=dumps(rv.body);rv.content_type=_A2
			return rv
		return wrapper
class TemplatePlugin:
	'This plugin applies the :func:`view` decorator to all routes with a\n    `template` config parameter. If the parameter is a tuple, the second\n    element must be a dict with additional options (e.g. `template_engine`)\n    or default variables for the template.';name='template';api=2
	def setup(self,app):app.tpl=self
	def apply(self,callback,route):
		conf=route.config.get('template')
		if isinstance(conf,(tuple,list))and len(conf)==2:return view(conf[0],**conf[1])(callback)
		elif isinstance(conf,str):return view(conf)(callback)
		else:return callback
class _ImportRedirect:
	def __init__(self,name,impmask):'Create a virtual package that redirects imports (see PEP 302).';self.name=name;self.impmask=impmask;self.module=sys.modules.setdefault(name,new_module(name));self.module.__dict__.update({'__file__':__file__,'__path__':[],'__all__':[],'__loader__':self});sys.meta_path.append(self)
	def find_spec(self,fullname,path,target=_A):
		if _H not in fullname:return
		if fullname.rsplit(_H,1)[0]!=self.name:return
		from importlib.util import spec_from_loader;return spec_from_loader(fullname,self)
	def find_module(self,fullname,path=_A):
		if _H not in fullname:return
		if fullname.rsplit(_H,1)[0]!=self.name:return
		return self
	def load_module(self,fullname):
		if fullname in sys.modules:return sys.modules[fullname]
		modname=fullname.rsplit(_H,1)[1];realname=self.impmask%modname;__import__(realname);module=sys.modules[fullname]=sys.modules[realname];setattr(self.module,modname,module);module.__loader__=self;return module
class MultiDict(DictMixin):
	'This dict stores multiple values per key, but behaves exactly like a\n    normal dict in that it returns only the newest value for any given key.\n    There are special methods available to access the full list of values.\n    '
	def __init__(self,*a,**k):self.dict={k:[v]for(k,v)in dict(*(a),**k).items()}
	def __len__(self):return len(self.dict)
	def __iter__(self):return iter(self.dict)
	def __contains__(self,key):return key in self.dict
	def __delitem__(self,key):del self.dict[key]
	def __getitem__(self,key):return self.dict[key][-1]
	def __setitem__(self,key,value):self.append(key,value)
	def keys(self):return self.dict.keys()
	if py3k:
		def values(self):return(v[-1]for v in self.dict.values())
		def items(self):return((k,v[-1])for(k,v)in self.dict.items())
		def allitems(self):return((k,v)for(k,vl)in self.dict.items()for v in vl)
		iterkeys=keys;itervalues=values;iteritems=items;iterallitems=allitems
	else:
		def values(self):return[v[-1]for v in self.dict.values()]
		def items(self):return[(k,v[-1])for(k,v)in self.dict.items()]
		def iterkeys(self):return self.dict.iterkeys()
		def itervalues(self):return(v[-1]for v in self.dict.itervalues())
		def iteritems(self):return((k,v[-1])for(k,v)in self.dict.iteritems())
		def iterallitems(self):return((k,v)for(k,vl)in self.dict.iteritems()for v in vl)
		def allitems(self):return[(k,v)for(k,vl)in self.dict.iteritems()for v in vl]
	def get(self,key,default=_A,index=-1,type=_A):
		'Return the most recent value for a key.\n\n        :param default: The default value to be returned if the key is not\n               present or the type conversion fails.\n        :param index: An index for the list of available values.\n        :param type: If defined, this callable is used to cast the value\n                into a specific type. Exception are suppressed and result in\n                the default value to be returned.\n        '
		try:val=self.dict[key][index];return type(val)if type else val
		except Exception:pass
		return default
	def append(self,key,value):'Add a new value to the list of values for this key.';self.dict.setdefault(key,[]).append(value)
	def replace(self,key,value):'Replace the list of values with a single value.';self.dict[key]=[value]
	def getall(self,key):'Return a (possibly empty) list of values for a key.';return self.dict.get(key)or[]
	getone=get;getlist=getall
class FormsDict(MultiDict):
	"This :class:`MultiDict` subclass is used to store request form data.\n    Additionally to the normal dict-like item access methods (which return\n    unmodified data as native strings), this container also supports\n    attribute-like access to its values. Attributes are automatically de-\n    or recoded to match :attr:`input_encoding` (default: 'utf8'). Missing\n    attributes default to an empty string.";input_encoding=_E;recode_unicode=_B
	def _fix(self,s,encoding=_A):
		if isinstance(s,unicode)and self.recode_unicode:return s.encode(_K).decode(encoding or self.input_encoding)
		elif isinstance(s,bytes):return s.decode(encoding or self.input_encoding)
		else:return s
	def decode(self,encoding=_A):
		'Returns a copy with all keys and values de- or recoded to match\n        :attr:`input_encoding`. Some libraries (e.g. WTForms) want a\n        unicode dictionary.';copy=FormsDict();enc=copy.input_encoding=encoding or self.input_encoding;copy.recode_unicode=_C
		for (key,value) in self.allitems():copy.append(self._fix(key,enc),self._fix(value,enc))
		return copy
	def getunicode(self,name,default=_A,encoding=_A):
		'Return the value as a unicode string, or the default.'
		try:return self._fix(self[name],encoding)
		except (UnicodeError,KeyError):return default
	def __getattr__(self,name,default=unicode()):
		if name.startswith('__')and name.endswith('__'):return super().__getattr__(name)
		return self.getunicode(name,default=default)
class HeaderDict(MultiDict):
	'A case-insensitive version of :class:`MultiDict` that defaults to\n    replace the old value instead of appending it.'
	def __init__(self,*a,**ka):
		self.dict={}
		if a or ka:self.update(*(a),**ka)
	def __contains__(self,key):return _hkey(key)in self.dict
	def __delitem__(self,key):del self.dict[_hkey(key)]
	def __getitem__(self,key):return self.dict[_hkey(key)][-1]
	def __setitem__(self,key,value):self.dict[_hkey(key)]=[_hval(value)]
	def append(self,key,value):self.dict.setdefault(_hkey(key),[]).append(_hval(value))
	def replace(self,key,value):self.dict[_hkey(key)]=[_hval(value)]
	def getall(self,key):return self.dict.get(_hkey(key))or[]
	def get(self,key,default=_A,index=-1):return MultiDict.get(self,_hkey(key),default,index)
	def filter(self,names):
		for name in (_hkey(n)for n in names):
			if name in self.dict:del self.dict[name]
class WSGIHeaderDict(DictMixin):
	"This dict-like class wraps a WSGI environ dict and provides convenient\n    access to HTTP_* fields. Keys and values are native strings\n    (2.x bytes or 3.x unicode) and keys are case-insensitive. If the WSGI\n    environment contains non-native string values, these are de- or encoded\n    using a lossless 'latin1' character set.\n\n    The API will remain stable even on changes to the relevant PEPs.\n    Currently PEP 333, 444 and 3333 are supported. (PEP 444 is the only one\n    that uses non-native strings.)\n    ";cgikeys=_k,_A3
	def __init__(self,environ):self.environ=environ
	def _ekey(self,key):
		'Translate header field name to CGI/WSGI environ key.';key=key.replace('-','_').upper()
		if key in self.cgikeys:return key
		return _A5+key
	def raw(self,key,default=_A):'Return the header value as is (may be bytes or unicode).';return self.environ.get(self._ekey(key),default)
	def __getitem__(self,key):
		val=self.environ[self._ekey(key)]
		if py3k:
			if isinstance(val,unicode):val=val.encode(_K).decode(_E)
			else:val=val.decode(_E)
		return val
	def __setitem__(self,key,value):raise TypeError(_AP%self.__class__)
	def __delitem__(self,key):raise TypeError(_AP%self.__class__)
	def __iter__(self):
		for key in self.environ:
			if key[:5]==_A5:yield _hkey(key[5:])
			elif key in self.cgikeys:yield _hkey(key)
	def keys(self):return[x for x in self]
	def __len__(self):return len(self.keys())
	def __contains__(self,key):return self._ekey(key)in self.environ
_UNSET=object()
class ConfigDict(dict):
	'A dict-like configuration storage with additional support for\n    namespaces, validators, meta-data, overlays and more.\n\n    This dict-like class is heavily optimized for read access. All read-only\n    methods as well as item access should be as fast as the built-in dict.\n    ';__slots__='_meta','_change_listener','_overlays','_virtual_keys','_source','__weakref__'
	def __init__(self):self._meta={};self._change_listener=[];self._overlays=[];self._source=_A;self._virtual_keys=set()
	def load_module(self,path,squash=_B):
		'Load values from a Python module.\n\n        Example modue ``config.py``::\n\n             DEBUG = True\n             SQLITE = {\n                 "db": ":memory:"\n             }\n\n\n        >>> c = ConfigDict()\n        >>> c.load_module(\'config\')\n        {DEBUG: True, \'SQLITE.DB\': \'memory\'}\n        >>> c.load_module("config", False)\n        {\'DEBUG\': True, \'SQLITE\': {\'DB\': \'memory\'}}\n\n        :param squash: If true (default), dictionary values are assumed to\n                       represent namespaces (see :meth:`load_dict`).\n        ';config_obj=load(path);obj={key:getattr(config_obj,key)for key in dir(config_obj)if key.isupper()}
		if squash:self.load_dict(obj)
		else:self.update(obj)
		return self
	def load_config(self,filename,**options):
		'Load values from an ``*.ini`` style config file.\n\n        A configuration file consists of sections, each led by a\n        ``[section]`` header, followed by key/value entries separated by\n        either ``=`` or ``:``. Section names and keys are case-insensitive.\n        Leading and trailing whitespace is removed from keys and values.\n        Values can be omitted, in which case the key/value delimiter may\n        also be left out. Values can also span multiple lines, as long as\n        they are indented deeper than the first line of the value. Commands\n        are prefixed by ``#`` or ``;`` and may only appear on their own on\n        an otherwise empty line.\n\n        Both section and key names may contain dots (``.``) as namespace\n        separators. The actual configuration parameter name is constructed\n        by joining section name and key name together and converting to\n        lower case.\n\n        The special sections ``bottle`` and ``ROOT`` refer to the root\n        namespace and the ``DEFAULT`` section defines default values for all\n        other sections.\n\n        With Python 3, extended string interpolation is enabled.\n\n        :param filename: The path of a config file, or a list of paths.\n        :param options: All keyword parameters are passed to the underlying\n            :class:`python:configparser.ConfigParser` constructor call.\n\n        ';options.setdefault('allow_no_value',_B)
		if py3k:options.setdefault('interpolation',configparser.ExtendedInterpolation())
		conf=configparser.ConfigParser(**options);conf.read(filename)
		for section in conf.sections():
			for key in conf.options(section):
				value=conf.get(section,key)
				if section not in('bottle','ROOT'):key=section+_H+key
				self[key.lower()]=value
		return self
	def load_dict(self,source,namespace=''):
		"Load values from a dictionary structure. Nesting can be used to\n        represent namespaces.\n\n        >>> c = ConfigDict()\n        >>> c.load_dict({'some': {'namespace': {'key': 'value'} } })\n        {'some.namespace.key': 'value'}\n        "
		for (key,value) in source.items():
			if isinstance(key,basestring):
				nskey=(namespace+_H+key).strip(_H)
				if isinstance(value,dict):self.load_dict(value,namespace=nskey)
				else:self[nskey]=value
			else:raise TypeError(_AQ%type(key))
		return self
	def update(self,*a,**ka):
		"If the first parameter is a string, all keys are prefixed with this\n        namespace. Apart from that it works just as the usual dict.update().\n\n        >>> c = ConfigDict()\n        >>> c.update('some.namespace', key='value')\n        ";prefix=''
		if a and isinstance(a[0],basestring):prefix=a[0].strip(_H)+_H;a=a[1:]
		for (key,value) in dict(*(a),**ka).items():self[prefix+key]=value
	def setdefault(self,key,value):
		if key not in self:self[key]=value
		return self[key]
	def __setitem__(self,key,value):
		if not isinstance(key,basestring):raise TypeError(_AQ%type(key))
		self._virtual_keys.discard(key);value=self.meta_get(key,'filter',lambda x:x)(value)
		if key in self and self[key]is value:return
		self._on_change(key,value);dict.__setitem__(self,key,value)
		for overlay in self._iter_overlays():overlay._set_virtual(key,value)
	def __delitem__(self,key):
		if key not in self:raise KeyError(key)
		if key in self._virtual_keys:raise KeyError('Virtual keys cannot be deleted: %s'%key)
		if self._source and key in self._source:dict.__delitem__(self,key);self._set_virtual(key,self._source[key])
		else:
			self._on_change(key,_A);dict.__delitem__(self,key)
			for overlay in self._iter_overlays():overlay._delete_virtual(key)
	def _set_virtual(self,key,value):
		'Recursively set or update virtual keys. Do nothing if non-virtual\n        value is present.'
		if key in self and key not in self._virtual_keys:return
		self._virtual_keys.add(key)
		if key in self and self[key]is not value:self._on_change(key,value)
		dict.__setitem__(self,key,value)
		for overlay in self._iter_overlays():overlay._set_virtual(key,value)
	def _delete_virtual(self,key):
		'Recursively delete virtual entry. Do nothing if key is not virtual.'
		if key not in self._virtual_keys:return
		if key in self:self._on_change(key,_A)
		dict.__delitem__(self,key);self._virtual_keys.discard(key)
		for overlay in self._iter_overlays():overlay._delete_virtual(key)
	def _on_change(self,key,value):
		for cb in self._change_listener:
			if cb(self,key,value):return _B
	def _add_change_listener(self,func):self._change_listener.append(func);return func
	def meta_get(self,key,metafield,default=_A):'Return the value of a meta field for a key.';return self._meta.get(key,{}).get(metafield,default)
	def meta_set(self,key,metafield,value):'Set the meta field for a key to a new value.';self._meta.setdefault(key,{})[metafield]=value
	def meta_list(self,key):'Return an iterable of meta field names defined for a key.';return self._meta.get(key,{}).keys()
	def _define(self,key,default=_UNSET,help=_UNSET,validate=_UNSET):
		'(Unstable) Shortcut for plugins to define own config parameters.'
		if default is not _UNSET:self.setdefault(key,default)
		if help is not _UNSET:self.meta_set(key,'help',help)
		if validate is not _UNSET:self.meta_set(key,'validate',validate)
	def _iter_overlays(self):
		for ref in self._overlays:
			overlay=ref()
			if overlay is not _A:yield overlay
	def _make_overlay(self):
		"(Unstable) Create a new overlay that acts like a chained map: Values\n        missing in the overlay are copied from the source map. Both maps\n        share the same meta entries.\n\n        Entries that were copied from the source are called 'virtual'. You\n        can not delete virtual keys, but overwrite them, which turns them\n        into non-virtual entries. Setting keys on an overlay never affects\n        its source, but may affect any number of child overlays.\n\n        Other than collections.ChainMap or most other implementations, this\n        approach does not resolve missing keys on demand, but instead\n        actively copies all values from the source to the overlay and keeps\n        track of virtual and non-virtual keys internally. This removes any\n        lookup-overhead. Read-access is as fast as a build-in dict for both\n        virtual and non-virtual keys.\n\n        Changes are propagated recursively and depth-first. A failing\n        on-change handler in an overlay stops the propagation of virtual\n        values and may result in an partly updated tree. Take extra care\n        here and make sure that on-change handlers never fail.\n\n        Used by Route.config\n        ";self._overlays[:]=[ref for ref in self._overlays if ref()is not _A];overlay=ConfigDict();overlay._meta=self._meta;overlay._source=self;self._overlays.append(weakref.ref(overlay))
		for key in self:overlay._set_virtual(key,self[key])
		return overlay
class AppStack(list):
	'A stack-like list. Calling it returns the head of the stack.'
	def __call__(self):'Return the current default application.';return self.default
	def push(self,value=_A):
		'Add a new :class:`Bottle` instance to the stack'
		if not isinstance(value,Bottle):value=Bottle()
		self.append(value);return value
	new_app=push
	@property
	def default(self):
		try:return self[-1]
		except IndexError:return self.push()
class WSGIFileWrapper:
	def __init__(self,fp,buffer_size=1024*64):
		self.fp,self.buffer_size=fp,buffer_size
		for attr in ('fileno',_P,'read','readlines','tell','seek'):
			if hasattr(fp,attr):setattr(self,attr,getattr(fp,attr))
	def __iter__(self):
		buff,read=self.buffer_size,self.read;part=read(buff)
		while part:yield part;part=read(buff)
class _closeiter:
	'This only exists to be able to attach a .close method to iterators that\n    do not support attribute assignment (most of itertools).'
	def __init__(self,iterator,close=_A):self.iterator=iterator;self.close_callbacks=makelist(close)
	def __iter__(self):return iter(self.iterator)
	def close(self):
		for func in self.close_callbacks:func()
class ResourceManager:
	"This class manages a list of search paths and helps to find and open\n    application-bound resources (files).\n\n    :param base: default value for :meth:`add_path` calls.\n    :param opener: callable used to open resources.\n    :param cachemode: controls which lookups are cached. One of 'all',\n                     'found' or 'none'.\n    "
	def __init__(self,base='./',opener=open,cachemode='all'):self.opener=opener;self.base=base;self.cachemode=cachemode;self.path=[];self.cache={}
	def add_path(self,path,base=_A,index=_A,create=_C):
		"Add a new path to the list of search paths. Return False if the\n        path does not exist.\n\n        :param path: The new search path. Relative paths are turned into\n            an absolute and normalized form. If the path looks like a file\n            (not ending in `/`), the filename is stripped off.\n        :param base: Path used to absolutize relative search paths.\n            Defaults to :attr:`base` which defaults to ``os.getcwd()``.\n        :param index: Position within the list of search paths. Defaults\n            to last index (appends to the list).\n\n        The `base` parameter makes it easy to reference files installed\n        along with a python module or package::\n\n            res.add_path('./resources/', __file__)\n        ";base=os.path.abspath(os.path.dirname(base or self.base));path=os.path.abspath(os.path.join(base,os.path.dirname(path)));path+=os.sep
		if path in self.path:self.path.remove(path)
		if create and not os.path.isdir(path):os.makedirs(path)
		if index is _A:self.path.append(path)
		else:self.path.insert(index,path)
		self.cache.clear();return os.path.exists(path)
	def __iter__(self):
		'Iterate over all existing files in all registered paths.';search=self.path[:]
		while search:
			path=search.pop()
			if not os.path.isdir(path):continue
			for name in os.listdir(path):
				full=os.path.join(path,name)
				if os.path.isdir(full):search.append(full)
				else:yield full
	def lookup(self,name):
		'Search for a resource and return an absolute file path, or `None`.\n\n        The :attr:`path` list is searched in order. The first match is\n        returned. Symlinks are followed. The result is cached to speed up\n        future lookups.'
		if name not in self.cache or DEBUG:
			for path in self.path:
				fpath=os.path.join(path,name)
				if os.path.isfile(fpath):
					if self.cachemode in('all','found'):self.cache[name]=fpath
					return fpath
			if self.cachemode=='all':self.cache[name]=_A
		return self.cache[name]
	def open(self,name,mode='r',*args,**kwargs):
		'Find a resource and return a file object, or raise IOError.';fname=self.lookup(name)
		if not fname:raise OSError('Resource %r not found.'%name)
		return self.opener(fname,*(args),mode=mode,**kwargs)
class FileUpload:
	def __init__(self,fileobj,name,filename,headers=_A):'Wrapper for file uploads.';self.file=fileobj;self.name=name;self.raw_filename=filename;self.headers=HeaderDict(headers)if headers else HeaderDict()
	content_type=HeaderProperty(_N);content_length=HeaderProperty(_J,reader=int,default=-1)
	def get_header(self,name,default=_A):'Return the value of a header within the multipart part.';return self.headers.get(name,default)
	@cached_property
	def filename(self):
		"Name of the file on the client file system, but normalized to ensure\n        file system compatibility. An empty filename is returned as 'empty'.\n\n        Only ASCII letters, digits, dashes, underscores and dots are\n        allowed in the final filename. Accents are removed, if possible.\n        Whitespace is replaced by a single dash. Leading or tailing dots\n        or dashes are removed. The filename is limited to 255 characters.\n        ";A='ASCII';fname=self.raw_filename
		if not isinstance(fname,unicode):fname=fname.decode(_E,_A0)
		fname=normalize('NFKD',fname);fname=fname.encode(A,_A0).decode(A);fname=os.path.basename(fname.replace('\\',os.path.sep));fname=re.sub('[^a-zA-Z0-9-_.\\s]','',fname).strip();fname=re.sub('[-\\s]+','-',fname).strip('.-');return fname[:255]or'empty'
	def _copy_file(self,fp,chunk_size=2**16):
		read,write,offset=self.file.read,fp.write,self.file.tell()
		while 1:
			buf=read(chunk_size)
			if not buf:break
			write(buf)
		self.file.seek(offset)
	def save(self,destination,overwrite=_C,chunk_size=2**16):
		'Save file to disk or copy its content to an open file(-like) object.\n        If *destination* is a directory, :attr:`filename` is added to the\n        path. Existing files are not overwritten by default (IOError).\n\n        :param destination: File path, directory or file(-like) object.\n        :param overwrite: If True, replace existing files. (default: False)\n        :param chunk_size: Bytes to read at a time. (default: 64kb)\n        '
		if isinstance(destination,basestring):
			if os.path.isdir(destination):destination=os.path.join(destination,self.filename)
			if not overwrite and os.path.exists(destination):raise OSError('File exists.')
			with open(destination,'wb')as fp:self._copy_file(fp,chunk_size)
		else:self._copy_file(destination,chunk_size)
def abort(code=500,text='Unknown Error.'):'Aborts execution and causes a HTTP error.';raise HTTPError(code,text)
def redirect(url,code=_A):
	'Aborts execution and causes a 303 or 302 redirect, depending on\n    the HTTP protocol version.'
	if not code:code=303 if request.get('SERVER_PROTOCOL')=='HTTP/1.1'else 302
	res=response.copy(cls=HTTPResponse);res.status=code;res.body='';res.set_header('Location',urljoin(request.url,url));raise res
def _rangeiter(fp,offset,limit,bufsize=1024*1024):
	'Yield chunks from a range in a file.';fp.seek(offset)
	while limit>0:
		part=fp.read(min(limit,bufsize))
		if not part:break
		limit-=len(part);yield part
def static_file(filename,root,mimetype=_B,download=_C,charset='UTF-8',etag=_A,headers=_A):
	'Open a file in a safe way and return an instance of :exc:`HTTPResponse`\n    that can be sent back to the client.\n\n    :param filename: Name or path of the file to send, relative to ``root``.\n    :param root: Root path for file lookups. Should be an absolute directory\n        path.\n    :param mimetype: Provide the content-type header (default: guess from\n        file extension)\n    :param download: If True, ask the browser to open a `Save as...` dialog\n        instead of opening the file with the associated program. You can\n        specify a custom filename as a string. If not specified, the\n        original filename is used (default: False).\n    :param charset: The charset for files with a ``text/*`` mime-type.\n        (default: UTF-8)\n    :param etag: Provide a pre-computed ETag header. If set to ``False``,\n        ETag handling is disabled. (default: auto-generate ETag header)\n    :param headers: Additional headers dict to add to the response.\n\n    While checking user input is always a good idea, this function provides\n    additional protection against malicious ``filename`` parameters from\n    breaking out of the ``root`` directory and leaking sensitive information\n    to an attacker.\n\n    Read-protected files or files outside of the ``root`` directory are\n    answered with ``403 Access Denied``. Missing files result in a\n    ``404 Not Found`` response. Conditional requests (``If-Modified-Since``,\n    ``If-None-Match``) are answered with ``304 Not Modified`` whenever\n    possible. ``HEAD`` and ``Range`` requests (used by download managers to\n    check or continue partial downloads) are also handled automatically.\n\n    ';root=os.path.join(os.path.abspath(root),'');filename=os.path.abspath(os.path.join(root,filename.strip('/\\')));headers=headers.copy()if headers else{}
	if not filename.startswith(root):return HTTPError(403,'Access denied.')
	if not os.path.exists(filename)or not os.path.isfile(filename):return HTTPError(404,'File does not exist.')
	if not os.access(filename,os.R_OK):return HTTPError(403,'You do not have permission to access this file.')
	if mimetype:
		if(mimetype[:5]=='text/'or mimetype=='application/javascript')and charset and'charset'not in mimetype:mimetype+='; charset=%s'%charset
		headers[_N]=mimetype
	if download:download=os.path.basename(filename if download is _B else download);headers['Content-Disposition']='attachment; filename="%s"'%download
	stats=os.stat(filename);headers[_J]=clen=stats.st_size;headers[_AO]=email.utils.formatdate(stats.st_mtime,usegmt=_B);headers['Date']=email.utils.formatdate(time.time(),usegmt=_B);getenv=request.environ.get
	if etag is _A:etag='%d:%d:%d:%d:%s'%(stats.st_dev,stats.st_ino,stats.st_mtime,clen,filename);etag=hashlib.sha1(tob(etag)).hexdigest()
	if etag:
		headers['ETag']=etag;check=getenv('HTTP_IF_NONE_MATCH')
		if check and check==etag:return HTTPResponse(status=304,**headers)
	ims=getenv('HTTP_IF_MODIFIED_SINCE')
	if ims:
		ims=parse_date(ims.split(';')[0].strip())
		if ims is not _A and ims>=int(stats.st_mtime):return HTTPResponse(status=304,**headers)
	body=''if request.method==_h else open(filename,_m);headers['Accept-Ranges']='bytes';range_header=getenv('HTTP_RANGE')
	if range_header:
		ranges=list(parse_range_header(range_header,clen))
		if not ranges:return HTTPError(416,'Requested Range Not Satisfiable')
		offset,end=ranges[0];rlen=end-offset;headers[_AN]='bytes %d-%d/%d'%(offset,end-1,clen);headers[_J]=str(rlen)
		if body:body=_closeiter(_rangeiter(body,offset,rlen),body.close)
		return HTTPResponse(body,status=206,**headers)
	return HTTPResponse(body,**headers)
def debug(mode=_B):
	'Change the debug level.\n    There is only one debug level supported at the moment.';global DEBUG
	if mode:warnings.simplefilter(_f)
	DEBUG=bool(mode)
def http_date(value):
	if isinstance(value,basestring):return value
	if isinstance(value,datetime):value=value.utctimetuple()
	elif isinstance(value,datedate):value=value.timetuple()
	if not isinstance(value,(int,float)):value=calendar.timegm(value)
	return email.utils.formatdate(value,usegmt=_B)
def parse_date(ims):
	'Parse rfc1123, rfc850 and asctime timestamps and return UTC epoch.'
	try:ts=email.utils.parsedate_tz(ims);return calendar.timegm(ts[:8]+(0,))-(ts[9]or 0)
	except (TypeError,ValueError,IndexError,OverflowError):return _A
def parse_auth(header):
	'Parse rfc2617 HTTP authentication header string (basic) and return (user,pass) tuple or None'
	try:
		method,data=header.split(_A,1)
		if method.lower()=='basic':user,pwd=touni(base64.b64decode(tob(data))).split(_O,1);return user,pwd
	except (KeyError,ValueError):return _A
def parse_range_header(header,maxlen=0):
	'Yield (start, end) ranges parsed from a HTTP Range header. Skip\n    unsatisfiable ranges. The end index is non-inclusive.'
	if not header or header[:6]!='bytes=':return
	ranges=[r.split('-',1)for r in header[6:].split(',')if'-'in r]
	for (start,end) in ranges:
		try:
			if not start:start,end=max(0,maxlen-int(end)),maxlen
			elif not end:start,end=int(start),maxlen
			else:start,end=int(start),min(int(end)+1,maxlen)
			if 0<=start<end<=maxlen:yield(start,end)
		except ValueError:pass
_hsplit=re.compile('(?:(?:"((?:[^"\\\\]|\\\\.)*)")|([^;,=]+))([;,=]?)').findall
def _parse_http_header(h):
	'Parses a typical multi-valued and parametrised HTTP header (e.g. Accept headers) and returns a list of values\n        and parameters. For non-standard or broken input, this implementation may return partial results.\n    :param h: A header string (e.g. ``text/html,text/plain;q=0.9,*/*;q=0.8``)\n    :return: List of (value, params) tuples. The second element is a (possibly empty) dict.\n    ';values=[]
	if'"'not in h:
		for value in h.split(','):
			parts=value.split(';');values.append((parts[0].strip(),{}))
			for attr in parts[1:]:name,value=attr.split('=',1);values[-1][1][name.strip()]=value.strip()
	else:
		lop,key,attrs=',',_A,{}
		for (quoted,plain,tok) in _hsplit(h):
			value=plain.strip()if plain else quoted.replace('\\"','"')
			if lop==',':attrs={};values.append((value,attrs))
			elif lop==';':
				if tok=='=':key=value
				else:attrs[value]=''
			elif lop=='='and key:attrs[key]=value;key=_A
			lop=tok
	return values
def _parse_qsl(qs):
	r=[]
	for pair in qs.split('&'):
		if not pair:continue
		nv=pair.split('=',1)
		if len(nv)!=2:nv.append('')
		key=urlunquote(nv[0].replace('+',' '));value=urlunquote(nv[1].replace('+',' '));r.append((key,value))
	return r
def _lscmp(a,b):'Compares two strings in a cryptographically safe way:\n    Runtime is not affected by length of common prefix.';return not sum(0 if x==y else 1 for(x,y)in zip(a,b))and len(a)==len(b)
def cookie_encode(data,key,digestmod=_A):'Encode and sign a pickle-able object. Return a (byte) string';depr(0,13,'cookie_encode() will be removed soon.',_A7);digestmod=digestmod or hashlib.sha256;msg=base64.b64encode(pickle.dumps(data,-1));sig=base64.b64encode(hmac.new(tob(key),msg,digestmod=digestmod).digest());return tob('!')+sig+tob(_L)+msg
def cookie_decode(data,key,digestmod=_A):
	'Verify and decode an encoded string. Return an object or None.';depr(0,13,'cookie_decode() will be removed soon.',_A7);data=tob(data)
	if cookie_is_encoded(data):
		sig,msg=data.split(tob(_L),1);digestmod=digestmod or hashlib.sha256;hashed=hmac.new(tob(key),msg,digestmod=digestmod).digest()
		if _lscmp(sig[1:],base64.b64encode(hashed)):return pickle.loads(base64.b64decode(msg))
	return _A
def cookie_is_encoded(data):'Return True if the argument looks like a encoded cookie.';depr(0,13,'cookie_is_encoded() will be removed soon.',_A7);return bool(data.startswith(tob('!'))and tob(_L)in data)
def html_escape(string):'Escape HTML special characters ``&<>`` and quotes ``\'"``.';return string.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;').replace("'",'&#039;')
def html_quote(string):'Escape and quote a string to be used as an HTTP attribute.';return'"%s"'%html_escape(string).replace(_G,'&#10;').replace(_l,'&#13;').replace('\t','&#9;')
def yieldroutes(func):
	"Return a generator for routes that match the signature (name, args)\n    of the func parameter. This may yield more than one route if the function\n    takes optional keyword arguments. The output is best described by example::\n\n        a()         -> '/a'\n        b(x, y)     -> '/b/<x>/<y>'\n        c(x, y=5)   -> '/c/<x>' and '/c/<x>/<y>'\n        d(x=5, y=6) -> '/d' and '/d/<x>' and '/d/<x>/<y>'\n    ";A='/<%s>';path=_D+func.__name__.replace('__',_D).lstrip(_D);spec=getargspec(func);argc=len(spec[0])-len(spec[3]or[]);path+=A*argc%tuple(spec[0][:argc]);yield path
	for arg in spec[0][argc:]:path+=A%arg;yield path
def path_shift(script_name,path_info,shift=1):
	'Shift path fragments from PATH_INFO to SCRIPT_NAME and vice versa.\n\n    :return: The modified paths.\n    :param script_name: The SCRIPT_NAME path.\n    :param script_name: The PATH_INFO path.\n    :param shift: The number of path fragments to shift. May be negative to\n      change the shift direction. (default: 1)\n    '
	if shift==0:return script_name,path_info
	pathlist=path_info.strip(_D).split(_D);scriptlist=script_name.strip(_D).split(_D)
	if pathlist and pathlist[0]=='':pathlist=[]
	if scriptlist and scriptlist[0]=='':scriptlist=[]
	if 0<shift<=len(pathlist):moved=pathlist[:shift];scriptlist=scriptlist+moved;pathlist=pathlist[shift:]
	elif 0>shift>=-len(scriptlist):moved=scriptlist[shift:];pathlist=moved+pathlist;scriptlist=scriptlist[:shift]
	else:empty=_X if shift<0 else _M;raise AssertionError('Cannot shift. Nothing left from %s'%empty)
	new_script_name=_D+_D.join(scriptlist);new_path_info=_D+_D.join(pathlist)
	if path_info.endswith(_D)and pathlist:new_path_info+=_D
	return new_script_name,new_path_info
def auth_basic(check,realm='private',text='Access denied'):
	'Callback decorator to require HTTP auth (basic).\n    TODO: Add route(check_auth=...) parameter.'
	def decorator(func):
		@functools.wraps(func)
		def wrapper(*a,**ka):
			user,password=request.auth or(_A,_A)
			if user is _A or not check(user,password):err=HTTPError(401,text);err.add_header('WWW-Authenticate','Basic realm="%s"'%realm);return err
			return func(*(a),**ka)
		return wrapper
	return decorator
def make_default_app_wrapper(name):
	'Return a callable that relays calls to the current default app.'
	@functools.wraps(getattr(Bottle,name))
	def wrapper(*a,**ka):return getattr(app(),name)(*(a),**ka)
	return wrapper
route=make_default_app_wrapper('route')
get=make_default_app_wrapper('get')
post=make_default_app_wrapper('post')
put=make_default_app_wrapper('put')
delete=make_default_app_wrapper('delete')
patch=make_default_app_wrapper('patch')
error=make_default_app_wrapper(_n)
mount=make_default_app_wrapper('mount')
hook=make_default_app_wrapper('hook')
install=make_default_app_wrapper('install')
uninstall=make_default_app_wrapper('uninstall')
url=make_default_app_wrapper('get_url')
class ServerAdapter:
	quiet=_C
	def __init__(self,host=_A4,port=8080,**options):self.options=options;self.host=host;self.port=int(port)
	def run(self,handler):0
	def __repr__(self):args=', '.join(f"{k}={repr(v)}"for(k,v)in self.options.items());return f"{self.__class__.__name__}({args})"
class CGIServer(ServerAdapter):
	quiet=_B
	def run(self,handler):
		from wsgiref.handlers import CGIHandler
		def fixed_environ(environ,start_response):environ.setdefault(_M,'');return handler(environ,start_response)
		CGIHandler().run(fixed_environ)
class FlupFCGIServer(ServerAdapter):
	def run(self,handler):import flup.server.fcgi;self.options.setdefault('bindAddress',(self.host,self.port));flup.server.fcgi.WSGIServer(handler,**self.options).run()
class WSGIRefServer(ServerAdapter):
	def run(self,app):
		import socket;from wsgiref.simple_server import WSGIRequestHandler,WSGIServer,make_server
		class FixedHandler(WSGIRequestHandler):
			def address_string(self):return self.client_address[0]
			def log_request(*args,**kw):
				if not self.quiet:return WSGIRequestHandler.log_request(*(args),**kw)
		handler_cls=self.options.get('handler_class',FixedHandler);server_cls=self.options.get('server_class',WSGIServer)
		if _O in self.host:
			if getattr(server_cls,'address_family')==socket.AF_INET:
				class server_cls(server_cls):address_family=socket.AF_INET6
		self.srv=make_server(self.host,self.port,app,server_cls,handler_cls);self.port=self.srv.server_port
		try:self.srv.serve_forever()
		except KeyboardInterrupt:self.srv.server_close();raise
class CherryPyServer(ServerAdapter):
	def run(self,handler):
		depr(0,13,"The wsgi server part of cherrypy was split into a new project called 'cheroot'.","Use the 'cheroot' server adapter instead of cherrypy.");from cherrypy import wsgiserver;self.options[_AR]=self.host,self.port;self.options['wsgi_app']=handler;certfile=self.options.get(_A8)
		if certfile:del self.options[_A8]
		keyfile=self.options.get(_A9)
		if keyfile:del self.options[_A9]
		server=wsgiserver.CherryPyWSGIServer(**self.options)
		if certfile:server.ssl_certificate=certfile
		if keyfile:server.ssl_private_key=keyfile
		try:server.start()
		finally:server.stop()
class CherootServer(ServerAdapter):
	def run(self,handler):
		from cheroot import wsgi;from cheroot.ssl import builtin;self.options[_AR]=self.host,self.port;self.options['wsgi_app']=handler;certfile=self.options.pop(_A8,_A);keyfile=self.options.pop(_A9,_A);chainfile=self.options.pop('chainfile',_A);server=wsgi.Server(**self.options)
		if certfile and keyfile:server.ssl_adapter=builtin.BuiltinSSLAdapter(certfile,keyfile,chainfile)
		try:server.start()
		finally:server.stop()
class WaitressServer(ServerAdapter):
	def run(self,handler):from waitress import serve;serve(handler,host=self.host,port=self.port,_quiet=self.quiet,**self.options)
class PasteServer(ServerAdapter):
	def run(self,handler):from paste import httpserver;from paste.translogger import TransLogger;handler=TransLogger(handler,setup_console_handler=not self.quiet);httpserver.serve(handler,host=self.host,port=str(self.port),**self.options)
class MeinheldServer(ServerAdapter):
	def run(self,handler):from meinheld import server;server.listen((self.host,self.port));server.run(handler)
class FapwsServer(ServerAdapter):
	'Extremely fast webserver using libev. See https://github.com/william-os4y/fapws3'
	def run(self,handler):
		depr(0,13,'fapws3 is not maintained and support will be dropped.');import fapws._evwsgi as evwsgi;from fapws import base,config;port=self.port
		if float(config.SERVER_IDENT[-2:])>0.4:port=str(port)
		evwsgi.start(self.host,port)
		if _a in os.environ and not self.quiet:_stderr('WARNING: Auto-reloading does not work with Fapws3.');_stderr('         (Fapws3 breaks python thread support)')
		evwsgi.set_base_module(base)
		def app(environ,start_response):environ['wsgi.multiprocess']=_C;return handler(environ,start_response)
		evwsgi.wsgi_cb(('',app));evwsgi.run()
class TornadoServer(ServerAdapter):
	'The super hyped asynchronous server by facebook. Untested.'
	def run(self,handler):import tornado.httpserver,tornado.ioloop,tornado.wsgi;container=tornado.wsgi.WSGIContainer(handler);server=tornado.httpserver.HTTPServer(container);server.listen(port=self.port,address=self.host);tornado.ioloop.IOLoop.instance().start()
class AppEngineServer(ServerAdapter):
	'Adapter for Google App Engine.';quiet=_B
	def run(self,handler):
		depr(0,13,'AppEngineServer no longer required','Configure your application directly in your app.yaml');from google.appengine.ext.webapp import util;module=sys.modules.get(_U)
		if module and not hasattr(module,'main'):module.main=lambda:util.run_wsgi_app(handler)
		util.run_wsgi_app(handler)
class TwistedServer(ServerAdapter):
	'Untested.'
	def run(self,handler):
		from twisted.internet import reactor;from twisted.python.threadpool import ThreadPool;from twisted.web import server,wsgi;thread_pool=ThreadPool();thread_pool.start();reactor.addSystemEventTrigger('after','shutdown',thread_pool.stop);factory=server.Site(wsgi.WSGIResource(reactor,thread_pool,handler));reactor.listenTCP(self.port,factory,interface=self.host)
		if not reactor.running:reactor.run()
class DieselServer(ServerAdapter):
	'Untested.'
	def run(self,handler):depr(0,13,'Diesel is not tested or supported and will be removed.');from diesel.protocols.wsgi import WSGIApplication;app=WSGIApplication(handler,port=self.port);app.run()
class GeventServer(ServerAdapter):
	'Untested. Options:\n\n    * See gevent.wsgi.WSGIServer() documentation for more options.\n    '
	def run(self,handler):
		from gevent import local,pywsgi
		if not isinstance(threading.local(),local.local):msg='Bottle requires gevent.monkey.patch_all() (before import)';raise RuntimeError(msg)
		if self.quiet:self.options['log']=_A
		address=self.host,self.port;server=pywsgi.WSGIServer(address,handler,**self.options)
		if _a in os.environ:import signal;signal.signal(signal.SIGINT,lambda s,f:server.stop())
		server.serve_forever()
class GunicornServer(ServerAdapter):
	'Untested. See http://gunicorn.org/configure.html for options.'
	def run(self,handler):
		A='bind';from gunicorn.app.base import BaseApplication
		if self.host.startswith('unix:'):config={A:self.host}
		else:config={A:'%s:%d'%(self.host,self.port)}
		config.update(self.options)
		class GunicornApplication(BaseApplication):
			def load_config(self):
				for (key,value) in config.items():self.cfg.set(key,value)
			def load(self):return handler
		GunicornApplication().run()
class EventletServer(ServerAdapter):
	'Untested. Options:\n\n    * `backlog` adjust the eventlet backlog parameter which is the maximum\n      number of queued connections. Should be at least 1; the maximum\n      value is system-dependent.\n    * `family`: (default is 2) socket family, optional. See socket\n      documentation for available families.\n    '
	def run(self,handler):
		from eventlet import listen,patcher,wsgi
		if not patcher.is_monkey_patched(os):msg='Bottle requires eventlet.monkey_patch() (before import)';raise RuntimeError(msg)
		socket_args={}
		for arg in ('backlog','family'):
			try:socket_args[arg]=self.options.pop(arg)
			except KeyError:pass
		address=self.host,self.port
		try:wsgi.server(listen(address,**socket_args),handler,log_output=not self.quiet)
		except TypeError:wsgi.server(listen(address),handler)
class BjoernServer(ServerAdapter):
	'Fast server written in C: https://github.com/jonashaag/bjoern'
	def run(self,handler):from bjoern import run;run(handler,self.host,self.port,reuse_port=_B)
class AsyncioServerAdapter(ServerAdapter):
	'Extend ServerAdapter for adding custom event loop'
	def get_event_loop(self):0
class AiohttpServer(AsyncioServerAdapter):
	'Asynchronous HTTP client/server framework for asyncio\n    https://pypi.python.org/pypi/aiohttp/\n    https://pypi.org/project/aiohttp-wsgi/\n    '
	def get_event_loop(self):import asyncio;return asyncio.new_event_loop()
	def run(self,handler):
		import asyncio;from aiohttp_wsgi.wsgi import serve;self.loop=self.get_event_loop();asyncio.set_event_loop(self.loop)
		if _a in os.environ:import signal;signal.signal(signal.SIGINT,lambda s,f:self.loop.stop())
		serve(handler,host=self.host,port=self.port)
class AiohttpUVLoopServer(AiohttpServer):
	'uvloop\n    https://github.com/MagicStack/uvloop\n    '
	def get_event_loop(self):import uvloop;return uvloop.new_event_loop()
class AutoServer(ServerAdapter):
	'Untested.';adapters=[WaitressServer,PasteServer,TwistedServer,CherryPyServer,CherootServer,WSGIRefServer]
	def run(self,handler):
		for sa in self.adapters:
			try:return sa(self.host,self.port,**self.options).run(handler)
			except ImportError:pass
server_names={'cgi':CGIServer,'flup':FlupFCGIServer,_v:WSGIRefServer,'waitress':WaitressServer,'cherrypy':CherryPyServer,'cheroot':CherootServer,'paste':PasteServer,'fapws3':FapwsServer,'tornado':TornadoServer,'gae':AppEngineServer,'twisted':TwistedServer,'diesel':DieselServer,'meinheld':MeinheldServer,'gunicorn':GunicornServer,'eventlet':EventletServer,'gevent':GeventServer,'bjoern':BjoernServer,'aiohttp':AiohttpServer,'uvloop':AiohttpUVLoopServer,'auto':AutoServer}
def load(target,**namespace):
	"Import a module or fetch an object from a module.\n\n    * ``package.module`` returns `module` as a module object.\n    * ``pack.mod:name`` returns the module variable `name` from `pack.mod`.\n    * ``pack.mod:func()`` calls `pack.mod.func()` and returns the result.\n\n    The last form accepts not only function calls, but any type of\n    expression. Keyword arguments passed to this function are available as\n    local variables. Example: ``import_string('re:compile(x)', x='[a-z]')``\n    ";module,target=target.split(_O,1)if _O in target else(target,_A)
	if module not in sys.modules:__import__(module)
	if not target:return sys.modules[module]
	if target.isalnum():return getattr(sys.modules[module],target)
	package_name=module.split(_H)[0];namespace[package_name]=sys.modules[package_name];return eval(f"{module}.{target}",namespace)
def load_app(target):
	'Load a bottle application from a module and make sure that the import\n    does not affect the current default application, but returns a separate\n    application object. See :func:`load` for the target parameter.';global NORUN;NORUN,nr_old=_B,NORUN;tmp=default_app.push()
	try:rv=load(target);return rv if callable(rv)else tmp
	finally:default_app.remove(tmp);NORUN=nr_old
_debug=debug
def run(app=_A,server=_v,host=_A4,port=8080,interval=1,reloader=_C,quiet=_C,plugins=_A,debug=_A,config=_A,**kargs):
	'Start a server instance. This method blocks until the server terminates.\n\n    :param app: WSGI application or target string supported by\n           :func:`load_app`. (default: :func:`default_app`)\n    :param server: Server adapter to use. See :data:`server_names` keys\n           for valid names or pass a :class:`ServerAdapter` subclass.\n           (default: `wsgiref`)\n    :param host: Server address to bind to. Pass ``0.0.0.0`` to listens on\n           all interfaces including the external one. (default: 127.0.0.1)\n    :param port: Server port to bind to. Values below 1024 require root\n           privileges. (default: 8080)\n    :param reloader: Start auto-reloading server? (default: False)\n    :param interval: Auto-reloader interval in seconds (default: 1)\n    :param quiet: Suppress output to stdout and stderr? (default: False)\n    :param options: Options passed to the server adapter.\n    ';A='BOTTLE_LOCKFILE'
	if NORUN:return
	if reloader and not os.environ.get(_a):
		import subprocess;fd,lockfile=tempfile.mkstemp(prefix='bottle.',suffix='.lock');environ=os.environ.copy();environ[_a]='true';environ[A]=lockfile;args=[sys.executable]+sys.argv
		if getattr(sys.modules.get(_U),'__package__',_A):args[1:1]=['-m',sys.modules[_U].__package__]
		try:
			os.close(fd)
			while os.path.exists(lockfile):
				p=subprocess.Popen(args,env=environ)
				while p.poll()is _A:os.utime(lockfile,_A);time.sleep(interval)
				if p.returncode==3:continue
				sys.exit(p.returncode)
		except KeyboardInterrupt:pass
		finally:
			if os.path.exists(lockfile):os.unlink(lockfile)
		return
	try:
		if debug is not _A:_debug(debug)
		app=app or default_app()
		if isinstance(app,basestring):app=load_app(app)
		if not callable(app):raise ValueError('Application is not callable: %r'%app)
		for plugin in plugins or[]:
			if isinstance(plugin,basestring):plugin=load(plugin)
			app.install(plugin)
		if config:app.config.update(config)
		if server in server_names:server=server_names.get(server)
		if isinstance(server,basestring):server=load(server)
		if isinstance(server,type):server=server(host=host,port=port,**kargs)
		if not isinstance(server,ServerAdapter):raise ValueError('Unknown or unsupported server: %r'%server)
		server.quiet=server.quiet or quiet
		if not server.quiet:
			_stderr(f'Bottle (dbt v{__version__}) server starting up (using {repr(server)})...')
			if server.host.startswith('unix:'):_stderr('Listening on %s'%server.host)
			else:_stderr('Listening on http://%s:%d/'%(server.host,server.port))
			_stderr('Hit Ctrl-C to quit.\n')
		if reloader:
			lockfile=os.environ.get(A);bgcheck=FileCheckerThread(lockfile,interval)
			with bgcheck:server.run(app)
			if bgcheck.status=='reload':sys.exit(3)
		else:server.run(app)
	except KeyboardInterrupt:pass
	except (SystemExit,MemoryError):raise
	except:
		if not reloader:raise
		if not getattr(server,'quiet',quiet):print_exc()
		time.sleep(interval);sys.exit(3)
class FileCheckerThread(threading.Thread):
	'Interrupt main-thread as soon as a changed module file is detected,\n    the lockfile gets deleted or gets too old.'
	def __init__(self,lockfile,interval):threading.Thread.__init__(self);self.daemon=_B;self.lockfile,self.interval=lockfile,interval;self.status=_A
	def run(self):
		exists=os.path.exists;mtime=lambda p:os.stat(p).st_mtime;files=dict()
		for module in list(sys.modules.values()):
			path=getattr(module,'__file__','')or''
			if path[-4:]in('.pyo','.pyc'):path=path[:-1]
			if path and exists(path):files[path]=mtime(path)
		while not self.status:
			if not exists(self.lockfile)or mtime(self.lockfile)<time.time()-self.interval-5:self.status=_n;thread.interrupt_main()
			for (path,lmtime) in list(files.items()):
				if not exists(path)or mtime(path)>lmtime:self.status='reload';thread.interrupt_main();break
			time.sleep(self.interval)
	def __enter__(self):self.start()
	def __exit__(self,exc_type,*_):
		if not self.status:self.status='exit'
		self.join();return exc_type is not _A and issubclass(exc_type,KeyboardInterrupt)
class TemplateError(BottleException):0
class BaseTemplate:
	'Base class and minimal API for template adapters';extensions=['tpl','html','thtml','stpl'];settings={};defaults={}
	def __init__(self,source=_A,name=_A,lookup=_A,encoding=_E,**settings):
		'Create a new template.\n        If the source parameter (str or buffer) is missing, the name argument\n        is used to guess a template filename. Subclasses can assume that\n        self.source and/or self.filename are set. Both are strings.\n        The lookup, encoding and settings parameters are stored as instance\n        variables.\n        The lookup parameter stores a list containing directory paths.\n        The encoding parameter should be used to decode byte strings or files.\n        The settings parameter contains a dict for engine-specific settings.\n        ';self.name=name;self.source=source.read()if hasattr(source,'read')else source;self.filename=source.filename if hasattr(source,'filename')else _A;self.lookup=[os.path.abspath(x)for x in lookup]if lookup else[];self.encoding=encoding;self.settings=self.settings.copy();self.settings.update(settings)
		if not self.source and self.name:
			self.filename=self.search(self.name,self.lookup)
			if not self.filename:raise TemplateError('Template %s not found.'%repr(name))
		if not self.source and not self.filename:raise TemplateError('No template specified.')
		self.prepare(**self.settings)
	@classmethod
	def search(cls,name,lookup=_A):
		'Search name in all directories specified in lookup.\n        First without, then with common extensions. Return first hit.'
		if not lookup:raise depr(0,12,'Empty template lookup path.','Configure a template lookup path.')
		if os.path.isabs(name):raise depr(0,12,'Use of absolute path for template name.','Refer to templates with names or paths relative to the lookup path.')
		for spath in lookup:
			spath=os.path.abspath(spath)+os.sep;fname=os.path.abspath(os.path.join(spath,name))
			if not fname.startswith(spath):continue
			if os.path.isfile(fname):return fname
			for ext in cls.extensions:
				if os.path.isfile(f"{fname}.{ext}"):return f"{fname}.{ext}"
	@classmethod
	def global_config(cls,key,*args):
		'This reads or sets the global settings stored in class.settings.'
		if args:cls.settings=cls.settings.copy();cls.settings[key]=args[0]
		else:return cls.settings[key]
	def prepare(self,**options):'Run preparations (parsing, caching, ...).\n        It should be possible to call this again to refresh a template or to\n        update settings.\n        ';raise NotImplementedError
	def render(self,*args,**kwargs):'Render the template with the specified local variables and return\n        a single byte or unicode string. If it is a byte string, the encoding\n        must match self.encoding. This method must be thread-safe!\n        Local variables may be provided in dictionaries (args)\n        or directly, as keywords (kwargs).\n        ';raise NotImplementedError
class MakoTemplate(BaseTemplate):
	def prepare(self,**options):
		from mako.lookup import TemplateLookup;from mako.template import Template;options.update({'input_encoding':self.encoding});options.setdefault('format_exceptions',bool(DEBUG));lookup=TemplateLookup(directories=self.lookup,**options)
		if self.source:self.tpl=Template(self.source,lookup=lookup,**options)
		else:self.tpl=Template(uri=self.name,filename=self.filename,lookup=lookup,**options)
	def render(self,*args,**kwargs):
		for dictarg in args:kwargs.update(dictarg)
		_defaults=self.defaults.copy();_defaults.update(kwargs);return self.tpl.render(**_defaults)
class CheetahTemplate(BaseTemplate):
	def prepare(self,**options):
		from Cheetah.Template import Template;self.context=threading.local();self.context.vars={};options['searchList']=[self.context.vars]
		if self.source:self.tpl=Template(source=self.source,**options)
		else:self.tpl=Template(file=self.filename,**options)
	def render(self,*args,**kwargs):
		for dictarg in args:kwargs.update(dictarg)
		self.context.vars.update(self.defaults);self.context.vars.update(kwargs);out=str(self.tpl);self.context.vars.clear();return out
class Jinja2Template(BaseTemplate):
	def prepare(self,filters=_A,tests=_A,globals={},**kwargs):
		from jinja2 import Environment,FunctionLoader;self.env=Environment(loader=FunctionLoader(self.loader),**kwargs)
		if filters:self.env.filters.update(filters)
		if tests:self.env.tests.update(tests)
		if globals:self.env.globals.update(globals)
		if self.source:self.tpl=self.env.from_string(self.source)
		else:self.tpl=self.env.get_template(self.name)
	def render(self,*args,**kwargs):
		for dictarg in args:kwargs.update(dictarg)
		_defaults=self.defaults.copy();_defaults.update(kwargs);return self.tpl.render(**_defaults)
	def loader(self,name):
		if name==self.filename:fname=name
		else:fname=self.search(name,self.lookup)
		if not fname:return
		with open(fname,_m)as f:return f.read().decode(self.encoding),fname,lambda:_C
class SimpleTemplate(BaseTemplate):
	def prepare(self,escape_func=html_escape,noescape=_C,syntax=_A,**ka):
		self.cache={};enc=self.encoding;self._str=lambda x:touni(x,enc);self._escape=lambda x:escape_func(touni(x,enc));self.syntax=syntax
		if noescape:self._str,self._escape=self._escape,self._str
	@cached_property
	def co(self):return compile(self.code,self.filename or'<string>','exec')
	@cached_property
	def code(self):
		source=self.source
		if not source:
			with open(self.filename,_m)as f:source=f.read()
		try:source,encoding=touni(source),_E
		except UnicodeError:raise depr(0,11,'Unsupported template encodings.','Use utf-8 for templates.')
		parser=StplParser(source,encoding=encoding,syntax=self.syntax);code=parser.translate();self.encoding=parser.encoding;return code
	def _rebase(self,_env,_name=_A,**kwargs):_env[_o]=_name,kwargs
	def _include(self,_env,_name=_A,**kwargs):
		env=_env.copy();env.update(kwargs)
		if _name not in self.cache:self.cache[_name]=self.__class__(name=_name,lookup=self.lookup,syntax=self.syntax)
		return self.cache[_name].execute(env['_stdout'],env)
	def execute(self,_stdout,kwargs):
		env=self.defaults.copy();env.update(kwargs);env.update({'_stdout':_stdout,'_printlist':_stdout.extend,'include':functools.partial(self._include,env),'rebase':functools.partial(self._rebase,env),_o:_A,'_str':self._str,'_escape':self._escape,'get':env.get,'setdefault':env.setdefault,'defined':env.__contains__});exec(self.co,env)
		if env.get(_o):subtpl,rargs=env.pop(_o);rargs['base']=''.join(_stdout);del _stdout[:];return self._include(env,subtpl,**rargs)
		return env
	def render(self,*args,**kwargs):
		'Render the template using keyword arguments as local variables.';env={};stdout=[]
		for dictarg in args:env.update(dictarg)
		env.update(kwargs);self.execute(stdout,env);return ''.join(stdout)
class StplSyntaxError(TemplateError):0
class StplParser:
	'Parser for stpl templates.';_re_cache={};_re_tok='(\n        [urbURB]*\n        (?:  \'\'(?!\')\n            |""(?!")\n            |\'{6}\n            |"{6}\n            |\'(?:[^\\\\\']|\\\\.)+?\'\n            |"(?:[^\\\\"]|\\\\.)+?"\n            |\'{3}(?:[^\\\\]|\\\\.|\\n)+?\'{3}\n            |"{3}(?:[^\\\\]|\\\\.|\\n)+?"{3}\n        )\n    )';_re_inl=_re_tok.replace('|\\n','');_re_tok+="\n        # 2: Comments (until end of line, but not the newline itself)\n        |(\\#.*)\n\n        # 3: Open and close (4) grouping tokens\n        |([\\[\\{\\(])\n        |([\\]\\}\\)])\n\n        # 5,6: Keywords that start or continue a python block (only start of line)\n        |^([\\ \\t]*(?:if|for|while|with|try|def|class)\\b)\n        |^([\\ \\t]*(?:elif|else|except|finally)\\b)\n\n        # 7: Our special 'end' keyword (but only if it stands alone)\n        |((?:^|;)[\\ \\t]*end[\\ \\t]*(?=(?:%(block_close)s[\\ \\t]*)?\\r?$|;|\\#))\n\n        # 8: A customizable end-of-code-block template token (only end of line)\n        |(%(block_close)s[\\ \\t]*(?=\\r?$))\n\n        # 9: And finally, a single newline. The 10th token is 'everything else'\n        |(\\r?\\n)\n    ";_re_split='(?m)^[ \\t]*(\\\\?)((%(line_start)s)|(%(block_start)s))';_re_inl='%%(inline_start)s((?:%s|[^\'"\\n])*?)%%(inline_end)s'%_re_inl;_re_tok='(?mx)'+_re_tok;_re_inl='(?mx)'+_re_inl;default_syntax='<% %> % {{ }}'
	def __init__(self,source,syntax=_A,encoding=_E):self.source,self.encoding=touni(source,encoding),encoding;self.set_syntax(syntax or self.default_syntax);self.code_buffer,self.text_buffer=[],[];self.lineno,self.offset=1,0;self.indent,self.indent_mod=0,0;self.paren_depth=0
	def get_syntax(self):'Tokens as a space separated string (default: <% %> % {{ }})';return self._syntax
	def set_syntax(self,syntax):
		self._syntax=syntax;self._tokens=syntax.split()
		if syntax not in self._re_cache:names='block_start block_close line_start inline_start inline_end';etokens=map(re.escape,self._tokens);pattern_vars=dict(zip(names.split(),etokens));patterns=self._re_split,self._re_tok,self._re_inl;patterns=[re.compile(p%pattern_vars)for p in patterns];self._re_cache[syntax]=patterns
		self.re_split,self.re_tok,self.re_inl=self._re_cache[syntax]
	syntax=property(get_syntax,set_syntax)
	def translate(self):
		if self.offset:raise RuntimeError('Parser is a one time instance.')
		while _B:
			m=self.re_split.search(self.source,pos=self.offset)
			if m:
				text=self.source[self.offset:m.start()];self.text_buffer.append(text);self.offset=m.end()
				if m.group(1):line,sep,_=self.source[self.offset:].partition(_G);self.text_buffer.append(self.source[m.start():m.start(1)]+m.group(2)+line+sep);self.offset+=len(line+sep);continue
				self.flush_text();self.offset+=self.read_code(self.source[self.offset:],multiline=bool(m.group(4)))
			else:break
		self.text_buffer.append(self.source[self.offset:]);self.flush_text();return ''.join(self.code_buffer)
	def read_code(self,pysource,multiline):
		code_line,comment='','';offset=0
		while _B:
			m=self.re_tok.search(pysource,pos=offset)
			if not m:code_line+=pysource[offset:];offset=len(pysource);self.write_code(code_line.strip(),comment);break
			code_line+=pysource[offset:m.start()];offset=m.end();_str,_com,_po,_pc,_blk1,_blk2,_end,_cend,_nl=m.groups()
			if self.paren_depth>0 and(_blk1 or _blk2):code_line+=_blk1 or _blk2;continue
			if _str:code_line+=_str
			elif _com:
				comment=_com
				if multiline and _com.strip().endswith(self._tokens[1]):multiline=_C
			elif _po:self.paren_depth+=1;code_line+=_po
			elif _pc:
				if self.paren_depth>0:self.paren_depth-=1
				code_line+=_pc
			elif _blk1:code_line=_blk1;self.indent+=1;self.indent_mod-=1
			elif _blk2:code_line=_blk2;self.indent_mod-=1
			elif _cend:
				if multiline:multiline=_C
				else:code_line+=_cend
			elif _end:self.indent-=1;self.indent_mod+=1
			else:
				self.write_code(code_line.strip(),comment);self.lineno+=1;code_line,comment,self.indent_mod='','',0
				if not multiline:break
		return offset
	def flush_text(self):
		text=''.join(self.text_buffer);del self.text_buffer[:]
		if not text:return
		parts,pos,nl=[],0,'\\\n'+'  '*self.indent
		for m in self.re_inl.finditer(text):
			prefix,pos=text[pos:m.start()],m.end()
			if prefix:parts.append(nl.join(map(repr,prefix.splitlines(_B))))
			if prefix.endswith(_G):parts[-1]+=nl
			parts.append(self.process_inline(m.group(1).strip()))
		if pos<len(text):
			prefix=text[pos:];lines=prefix.splitlines(_B)
			if lines[-1].endswith('\\\\\n'):lines[-1]=lines[-1][:-3]
			elif lines[-1].endswith('\\\\\r\n'):lines[-1]=lines[-1][:-4]
			parts.append(nl.join(map(repr,lines)))
		code='_printlist((%s,))'%', '.join(parts);self.lineno+=code.count(_G)+1;self.write_code(code)
	@staticmethod
	def process_inline(chunk):
		if chunk[0]=='!':return'_str(%s)'%chunk[1:]
		return'_escape(%s)'%chunk
	def write_code(self,line,comment=''):code='  '*(self.indent+self.indent_mod);code+=line.lstrip()+comment+_G;self.code_buffer.append(code)
def template(*args,**kwargs):
	'\n    Get a rendered template as a string iterator.\n    You can use a name, a filename or a template string as first parameter.\n    Template rendering arguments can be passed as dictionaries\n    or directly (as keyword arguments).\n    ';tpl=args[0]if args else _A
	for dictarg in args[1:]:kwargs.update(dictarg)
	adapter=kwargs.pop('template_adapter',SimpleTemplate);lookup=kwargs.pop('template_lookup',TEMPLATE_PATH);tplid=id(lookup),tpl
	if tplid not in TEMPLATES or DEBUG:
		settings=kwargs.pop('template_settings',{})
		if isinstance(tpl,adapter):
			TEMPLATES[tplid]=tpl
			if settings:TEMPLATES[tplid].prepare(**settings)
		elif _G in tpl or'{'in tpl or'%'in tpl or'$'in tpl:TEMPLATES[tplid]=adapter(source=tpl,lookup=lookup,**settings)
		else:TEMPLATES[tplid]=adapter(name=tpl,lookup=lookup,**settings)
	if not TEMPLATES[tplid]:abort(500,'Template (%s) not found'%tpl)
	return TEMPLATES[tplid].render(kwargs)
mako_template=functools.partial(template,template_adapter=MakoTemplate)
cheetah_template=functools.partial(template,template_adapter=CheetahTemplate)
jinja2_template=functools.partial(template,template_adapter=Jinja2Template)
def view(tpl_name,**defaults):
	'Decorator: renders a template for a handler.\n    The handler can control its behavior like that:\n\n      - return a dict of template vars to fill out the template\n      - return something other than a dict and the view decorator will not\n        process the template, but return the handler result as is.\n        This includes returning a HTTPResponse(dict) to get,\n        for instance, JSON with autojson or other castfilters.\n    '
	def decorator(func):
		@functools.wraps(func)
		def wrapper(*args,**kwargs):
			result=func(*(args),**kwargs)
			if isinstance(result,(dict,DictMixin)):tplvars=defaults.copy();tplvars.update(result);return template(tpl_name,**tplvars)
			elif result is _A:return template(tpl_name,**defaults)
			return result
		return wrapper
	return decorator
mako_view=functools.partial(view,template_adapter=MakoTemplate)
cheetah_view=functools.partial(view,template_adapter=CheetahTemplate)
jinja2_view=functools.partial(view,template_adapter=Jinja2Template)
TEMPLATE_PATH=['./','./views/']
TEMPLATES={}
DEBUG=_C
NORUN=_C
HTTP_CODES=httplib.responses.copy()
HTTP_CODES[418]="I'm a teapot"
HTTP_CODES[428]='Precondition Required'
HTTP_CODES[429]='Too Many Requests'
HTTP_CODES[431]='Request Header Fields Too Large'
HTTP_CODES[451]='Unavailable For Legal Reasons'
HTTP_CODES[511]='Network Authentication Required'
_HTTP_STATUS_LINES={k:'%d %s'%(k,v)for(k,v)in HTTP_CODES.items()}
ERROR_PAGE_TEMPLATE='\n%%try:\n    %%from %s import DEBUG, request\n    <!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">\n    <html>\n        <head>\n            <title>Error: {{e.status}}</title>\n            <style type="text/css">\n              html {background-color: #eee; font-family: sans-serif;}\n              body {background-color: #fff; border: 1px solid #ddd;\n                    padding: 15px; margin: 15px;}\n              pre {background-color: #eee; border: 1px solid #ddd; padding: 5px;}\n            </style>\n        </head>\n        <body>\n            <h1>Error: {{e.status}}</h1>\n            <p>Sorry, the requested URL <tt>{{repr(request.url)}}</tt>\n               caused an error:</p>\n            <pre>{{e.body}}</pre>\n            %%if DEBUG and e.exception:\n              <h2>Exception:</h2>\n              %%try:\n                %%exc = repr(e.exception)\n              %%except:\n                %%exc = \'<unprintable %%s object>\' %% type(e.exception).__name__\n              %%end\n              <pre>{{exc}}</pre>\n            %%end\n            %%if DEBUG and e.traceback:\n              <h2>Traceback:</h2>\n              <pre>{{e.traceback}}</pre>\n            %%end\n        </body>\n    </html>\n%%except ImportError:\n    <b>ImportError:</b> Could not generate the error page. Please add bottle to\n    the import path.\n%%end\n'%__name__
request=LocalRequest()
response=LocalResponse()
local=threading.local()
apps=app=default_app=AppStack()
ext=_ImportRedirect('bottle.ext'if __name__==_U else __name__+'.ext','bottle_%s').module
def _main(argv):
	args,parser=_cli_parse(argv)
	def _cli_error(cli_msg):parser.print_help();_stderr('\nError: %s\n'%cli_msg);sys.exit(1)
	if args.version:print('Bottle %s'%__version__);sys.exit(0)
	if not args.app:_cli_error('No application entry point specified.')
	sys.path.insert(0,_H);sys.modules.setdefault('bottle',sys.modules[_U]);host,port=args.bind or _AA,8080
	if _O in host and host.rfind(']')<host.rfind(_O):host,port=host.rsplit(_O,1)
	host=host.strip('[]');config=ConfigDict()
	for cfile in args.conf or[]:
		try:
			if cfile.endswith('.json'):
				with open(cfile,_m)as fp:config.load_dict(json_loads(fp.read()))
			else:config.load_config(cfile)
		except configparser.Error as parse_error:_cli_error(parse_error)
		except OSError:_cli_error('Unable to read config file %r'%cfile)
		except (UnicodeError,TypeError,ValueError)as error:_cli_error(f"Unable to parse config file {cfile!r}: {error}")
	for cval in args.param or[]:
		if'='in cval:config.update((cval.split('=',1),))
		else:config[cval]=_B
	run(args.app,host=host,port=int(port),server=args.server,reloader=args.reload,plugins=args.plugin,debug=args.debug,config=config)
SERVER_MUTEX=threading.Lock()
@dataclass
class ServerRunResult:'The result of running a query.';column_names:List[str];rows:List[List[Any]];raw_code:str;executed_code:str
@dataclass
class ServerCompileResult:'The result of compiling a project.';result:str
@dataclass
class ServerResetResult:'The result of resetting the Server database.';result:str
@dataclass
class ServerRegisterResult:'The result of registering a project.';added:str;projects:List[str]
@dataclass
class ServerUnregisterResult:'The result of unregistering a project.';removed:str;projects:List[str]
class ServerErrorCode(Enum):'The error codes that can be returned by the Server API.';FailedToReachServer=-1;CompileSqlFailure=1;ExecuteSqlFailure=2;ProjectParseFailure=3;ProjectNotRegistered=4;ProjectHeaderNotSupplied=5
@dataclass
class ServerError:'An error that can be serialized to JSON.';code:ServerErrorCode;message:str;data:Dict[str,Any]
@dataclass
class ServerErrorContainer:'A container for an ServerError that can be serialized to JSON.';error:ServerError
def server_serializer(o):
	'Encode JSON. Handles server-specific types.'
	if isinstance(o,decimal.Decimal):return float(o)
	if isinstance(o,ServerErrorCode):return o.value
	return str(o)
def remove_comments(string):
	'Remove comments from a string.';pattern='(\\".*?\\"|\\\'.*?\\\')|(/\\*.*?\\*/|//[^\\r\\n]*$)';regex=re.compile(pattern,re.MULTILINE|re.DOTALL)
	def _replacer(match):
		if match.group(2)is not _A:return''
		else:return match.group(1)
	multiline_comments_removed=regex.sub(_replacer,string)+_G;output=''
	for line in multiline_comments_removed.splitlines(keepends=_B):
		if line.strip().startswith('--'):continue
		s_quote_c=0;d_quote_c=0;cmt_dash=0;split_ix=-1
		for (i,c) in enumerate(line):
			if cmt_dash>=2:split_ix=i-cmt_dash;break
			if c=='"':d_quote_c+=1
			elif c=="'":s_quote_c+=1
			elif c=='-'and d_quote_c%2==0 and s_quote_c%2==0:cmt_dash+=1;continue
			cmt_dash=0
		if split_ix>0:output+=line[:split_ix]+_G
		else:output+=line
	return ''.join(output)
class DbtInterfaceServerPlugin:
	'Used to inject the dbt-core-interface runner into the request context.';name=_AS;api=2
	def __init__(self,runner=_A):
		'Initialize the plugin with the runner to inject into the request context.';self.runners=DbtProjectContainer()
		if runner:self.runners.add_parsed_project(runner)
	def apply(self,callback,route):
		'Apply the plugin to the route callback.'
		def wrapper(*args,**kwargs):start=time.time();body=callback(*(args),**kwargs,runners=self.runners);end=time.time();response.headers['X-dbt-Exec-Time']=str(end-start);return body
		return wrapper
@route('/run',method=_T)
def run_sql(runners):
	'Run SQL against a dbt project.';project_runner=runners.get_project(request.get_header(_Q))or runners.get_default_project()
	if not project_runner:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectNotRegistered,message=_b,data={_c:runners.registered_projects()})))
	query=remove_comments(request.body.read().decode(_V));limit=request.query.get('limit',200);query_with_limit=f"select * from ({query}) as __server_query limit {limit}"
	try:result=project_runner.execute_code(query_with_limit)
	except Exception as execution_err:return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ExecuteSqlFailure,message=str(execution_err),data=execution_err.__dict__)))
	compiled_query=re.search('select \\* from \\(([\\w\\W]+)\\) as __server_query',result.compiled_code).groups()[0];return asdict(ServerRunResult(rows=[list(row)for row in result.table.rows],column_names=result.table.column_names,executed_code=compiled_query.strip(),raw_code=query))
@route('/compile',method=_T)
def compile_sql(runners):
	'Compiles a SQL query.';project_runner=runners.get_project(request.get_header(_Q))or runners.get_default_project()
	if not project_runner:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectNotRegistered,message=_b,data={_c:runners.registered_projects()})))
	query=request.body.read().decode(_V).strip()
	if has_jinja(query):
		try:compiled_query=project_runner.compile_code(query).compiled_code
		except Exception as compile_err:return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.CompileSqlFailure,message=str(compile_err),data=compile_err.__dict__)))
	else:compiled_query=query
	return asdict(ServerCompileResult(result=compiled_query))
@route(['/parse','/reset'])
def reset(runners):
	'Reset the runner and clear the cache.';C='Currently reparsing project';B='Mutex is locked, reparse in progress';A='Mutex locked';project_runner=runners.get_project(request.get_header(_Q))or runners.get_default_project()
	if not project_runner:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectNotRegistered,message=_b,data={_c:runners.registered_projects()})))
	reset=str(request.query.get('reset','false')).lower()=='true';old_target=getattr(project_runner.base_config,_i,project_runner.config.target_name);new_target=request.query.get(_i,old_target)
	if not reset and old_target==new_target:
		if SERVER_MUTEX.acquire(blocking=_C):LOGGER.debug(A);parse_job=threading.Thread(target=_reset,args=(project_runner,reset,old_target,new_target));parse_job.start();return asdict(ServerResetResult(result='Initializing project parsing'))
		else:LOGGER.debug(B);return asdict(ServerResetResult(result=C))
	elif SERVER_MUTEX.acquire(blocking=old_target!=new_target):LOGGER.debug(A);return asdict(_reset(project_runner,reset,old_target,new_target))
	else:LOGGER.debug(B);return asdict(ServerResetResult(result=C))
def _reset(runner,reset,old_target,new_target):
	'Reset the project runner.\n\n    Can be called asynchronously or synchronously.\n    ';target_did_change=old_target!=new_target
	try:runner.base_config.target=new_target;LOGGER.debug('Starting reparse');runner.safe_parse_project(reinit=reset or target_did_change)
	except Exception as reparse_err:LOGGER.debug('Reparse error');runner.base_config.target=old_target;rv=ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectParseFailure,message=str(reparse_err),data=reparse_err.__dict__))
	else:LOGGER.debug('Reparse success');rv=ServerResetResult(result=f"Profile target changed from {old_target} to {new_target}!"if target_did_change else f"Reparsed project with profile {old_target}!")
	finally:LOGGER.debug('Unlocking mutex');SERVER_MUTEX.release()
	return rv
@route('/register',method=_T)
def register(runners):
	'Register a new project runner.';project=request.get_header(_Q)
	if not project:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectHeaderNotSupplied,message=_AT,data=dict(request.headers))))
	if project in runners:return asdict(ServerRegisterResult(added=project,projects=runners.registered_projects()))
	project_dir=request.json['project_dir'];profiles_dir=request.json['profiles_dir'];target=request.json.get(_i)
	try:new_runner=DbtProject(project_dir=project_dir,profiles_dir=profiles_dir,target=target)
	except Exception as init_err:return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectParseFailure,message=str(init_err),data=init_err.__dict__)))
	runners[project]=new_runner;runners.add_parsed_project;return asdict(ServerRegisterResult(added=project,projects=runners.registered_projects()))
@route('/unregister',method=_T)
def unregister(runners):
	'Unregister a project runner from the server.';project=request.get_header(_Q)
	if not project:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectHeaderNotSupplied,message=_AT,data=dict(request.headers))))
	if project not in runners:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectNotRegistered,message=_b,data={_c:runners.registered_projects()})))
	runners.drop_project(project);return asdict(ServerUnregisterResult(removed=project,projects=runners.registered_projects()))
@route('/v1/info',methods=_S)
def trino_info(runners):'Trino info endpoint.';return{'coordinator':{},'workers':[],'memory':{},'jvm':{},'system':{}}
@route('/v1/statement',method=_T)
def trino_statement(runners):
	'Trino statement endpoint.\n\n    This endpoint is used to execute queries and return the results.\n    It is very minimal right now. The only purpose is to proxy SELECT queries\n    to dbt from a JDBC and return the results to the JDBC.\n    ';L='interval day to second';K='date';J='boolean';I='bigint';H='LONG_LITERAL';G='value';F='kind';E='varchar';D='arguments';C='rawType';B='typeSignature';A='type';from agate import data_types;_user=request.headers.get('X-Presto-User',_f);project_runner=runners.get_project(request.get_header(_Q))or runners.get_default_project()
	if not project_runner:return{'errorName':'ProjectNotRegistered','errorType':'USER_ERROR','errorLocation':{'lineNumber':1,'columnNumber':1},_n:'Project is not registered. Make a POST request to the /register endpoint'}
	query=request.body.read().decode(_V);res=project_runner.execute_code(query);columns=[]
	for column in res.table.columns:
		if isinstance(column.data_type,data_types.Text):columns+=[{_I:column.name,A:E,B:{C:E,D:[{F:H,G:255}]}}]
		elif isinstance(column.data_type,data_types.Number):columns+=[{_I:column.name,A:I,B:{C:I,D:[]}}]
		elif isinstance(column.data_type,data_types.Boolean):columns+=[{_I:column.name,A:J,B:{C:J,D:[]}}]
		elif isinstance(column.data_type,data_types.Date):columns+=[{_I:column.name,A:K,B:{C:K,D:[]}}]
		elif isinstance(column.data_type,data_types.DateTime):columns+=[{_I:column.name,A:_AB,B:{C:_AB,D:[]}}]
		elif isinstance(column.data_type,data_types.TimeDelta):columns+=[{_I:column.name,A:L,B:{C:L,D:[]}}]
		else:columns+=[{_I:column.name,A:E,B:{C:E,D:[{F:H,G:255}]}}]
	return{'id':'someId','infoUri':'http://localhost:8581/v1/info','columns':columns,'data':[list(row)for row in res.table.rows],'stats':{'state':'FINISHED','nodes':1}}
@route(['/health','/api/health'],methods=_S)
def health_check(runners):
	'Health check endpoint.';project_runner=runners.get_project(request.get_header(_Q))or runners.get_default_project()
	if not project_runner:response.status=400;return asdict(ServerErrorContainer(error=ServerError(code=ServerErrorCode.ProjectNotRegistered,message=_b,data={_c:runners.registered_projects()})))
	return{'result':{'status':'ready','project_name':project_runner.config.project_name,'target_name':project_runner.config.target_name,'profile_name':project_runner.config.project_name,'logs':project_runner.config.log_path,_AB:str(datetime.utcnow()),_n:_A},'id':str(uuid.uuid4()),_AS:__name__}
@route(["/heartbeat", "/api/heartbeat"], methods=_S)
def heart_beat(runners):
    """Heart beat endpoint.""";return {"result": {"status":"ready"}}
ServerPlugin=DbtInterfaceServerPlugin()
install(ServerPlugin)
install(JSONPlugin(json_dumps=lambda body:json.dumps(body,default=server_serializer)))
def run_server(runner=_A,host=_AA,port=8581):
	'Run the dbt core interface server.\n\n    See supported servers below. By default, the server will run with the\n    `WSGIRefServer` which is a pure Python server. If you want to use a different server,\n    you will need to install the dependencies for that server.\n\n    (CGIServer, FlupFCGIServer, WSGIRefServer, WaitressServer,\n    CherryPyServer, CherootServer, PasteServer, FapwsServer,\n    TornadoServer, AppEngineServer, TwistedServer, DieselServer,\n    MeinheldServer, GunicornServer, EventletServer, GeventServer,\n    BjoernServer, AiohttpServer, AiohttpUVLoopServer, AutoServer)\n    '
	if runner:ServerPlugin.runners.add_parsed_project(runner)
	run(host=host,port=port)
try:from git import Repo
except ImportError:pass
else:
	from pathlib import Path
	def build_diff_queries(model,runner):'Leverage git to build two temporary tables for diffing the results of a query throughout a change.';node=runner.get_ref_node(model);dbt_path=Path(node.root_path);repo=Repo(dbt_path,search_parent_directories=_B);t=next(Path(repo.working_dir).rglob(node.original_file_path)).relative_to(repo.working_dir);sha=repo.head.object.hexsha;target=repo.head.object.tree[str(t)];git_node_name='z_'+sha[-7:];original_node=runner.get_server_node(target.data_stream.read().decode(_V),git_node_name);changed_node=node;original_node=runner.compile_node(original_node);changed_node=runner.compile_node(changed_node);return original_node.compiled_sql,changed_node.compiled_sql
	def build_diff_tables(model,runner):
		'Leverage git to build two temporary tables for diffing the results of a query throughout a change.';C='dbt-osmosis';B='Creating new relation for %s';A='dbt_diff';node=runner.get_ref_node(model);dbt_path=Path(node.root_path);repo=Repo(dbt_path,search_parent_directories=_B);t=next(Path(repo.working_dir).rglob(node.original_file_path)).relative_to(repo.working_dir);sha=repo.head.object.hexsha;target=repo.head.object.tree[str(t)];git_node_name='z_'+sha[-7:];original_node=runner.get_server_node(target.data_stream.read().decode(_V),git_node_name);changed_node=node;original_node=runner.compile_node(original_node).node;changed_node=runner.compile_node(changed_node).node;git_node_parts=original_node.database,A,git_node_name;ref_a,did_exist=runner.get_or_create_relation(*(git_node_parts))
		if not did_exist:
			LOGGER.info(B,ref_a)
			with runner.adapter.connection_named(C):runner.execute_macro(_r,kwargs={_R:ref_a});runner.execute_macro(_s,kwargs={_t:original_node.compiled_sql,_R:ref_a,_u:_B},run_compiled_sql=_B)
		temp_node_name='z_'+hashlib.md5(changed_node.compiled_sql.encode(_V),usedforsecurity=_C).hexdigest()[-7:];git_node_parts=original_node.database,A,temp_node_name;ref_b,did_exist=runner.get_or_create_relation(*(git_node_parts))
		if not did_exist:
			ref_b=runner.adapter.Relation.create(*(git_node_parts));LOGGER.info(B,ref_b)
			with runner.adapter.connection_named(C):runner.execute_macro(_r,kwargs={_R:ref_b});runner.execute_macro(_s,kwargs={_t:original_node.compiled_sql,_R:ref_b,_u:_B},run_compiled_sql=_B)
		return ref_a,ref_b
	def diff_tables(ref_a,ref_b,pk,runner,aggregate=_B):'Given two relations, compare the results and return a table of the differences.';LOGGER.info(_AU);_,table=runner.adapter_execute(runner.execute_macro('_dbt_osmosis_compare_relations_agg'if aggregate else'_dbt_osmosis_compare_relations',kwargs={'a_relation':ref_a,'b_relation':ref_b,_AV:pk}),auto_begin=_B,fetch=_B);return table
	def diff_queries(sql_a,sql_b,pk,runner,aggregate=_B):'Given two queries, compare the results and return a table of the differences.';LOGGER.info(_AU);_,table=runner.adapter_execute(runner.execute_macro('_dbt_osmosis_compare_queries_agg'if aggregate else'_dbt_osmosis_compare_queries',kwargs={'a_query':sql_a,'b_query':sql_b,_AV:pk}),auto_begin=_B,fetch=_B);return table
	def diff_and_print_to_console(model,pk,runner,make_temp_tables=_C,agg=_B,output='table'):
		'Compare two tables and print the results to the console.';A='in_original, in_changed';import agate
		if make_temp_tables:table=diff_tables(*build_diff_tables(model,runner),pk,runner,agg)
		else:table=diff_queries(*build_diff_queries(model,runner),pk,runner,agg)
		print('');output=output.lower()
		if output=='table':table.print_table()
		elif output in('chart','bar'):
			if not agg:LOGGER.warn('Cannot render output format chart with --no-agg option, defaulting to table');table.print_table()
			else:_table=table.compute([(A,agate.Formula(agate.Text(),lambda r:'%(in_a)s, %(in_b)s'%r))]);_table.print_bars(label_column_name=A,value_column_name='count')
		elif output=='csv':table.to_csv('dbt-osmosis-diff.csv')
		else:LOGGER.warn('No such output format %s, defaulting to table',output);table.print_table()
if __name__==_U:import argparse;parser=argparse.ArgumentParser(description='Run the dbt interface server. Defaults to the WSGIRefServer');parser.add_argument('--host',default=_AA,help='The host to run the server on. Defaults to localhost');parser.add_argument('--port',default=8581,help='The port to run the server on. Defaults to 8581');args=parser.parse_args();run_server(host=args.host,port=args.port)
