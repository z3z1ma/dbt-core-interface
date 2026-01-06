"""Tests for the server module."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from dbt_core_interface.server import (
    DISABLE_AUTOLOAD,
    STATE_FILE,
    ServerErrorCode,
    ServerError,
    ServerErrorContainer,
    ServerRunResult,
    ServerCompileResult,
    ServerRegisterResult,
    ServerUnregisterResult,
    ServerResetResult,
    _get_state_file_path,
    _format_run_result,
    _create_error_response,
    _get_container,
    _load_saved_state,
    _save_state,
    app,
)
from dbt_core_interface.project import ExecutionResult
from dbt_core_interface.quality import QualityCheckType, Severity


@pytest.fixture
def mock_runner():
    """Create a mock DbtProject runner."""
    runner = MagicMock()
    runner.project_name = "test_project"
    runner.runtime_config.target_name = "dev"
    runner.log_path = Path("/tmp/logs")
    runner.project_root = Path("/tmp/project")
    runner.args = MagicMock()
    runner.args.profiles_dir = None
    runner.args.threads = 1
    runner.args.vars = {}
    return runner


@pytest.fixture
def mock_container(mock_runner):
    """Create a mock DbtProjectContainer."""
    container = MagicMock()
    container.registered_projects.return_value = [Path("/tmp/project")]
    container.__iter__ = Mock(return_value=iter([mock_runner]))
    container.get_project.return_value = mock_runner
    container.get_default_project.return_value = mock_runner
    container.find_project_in_tree.return_value = mock_runner
    return container


@pytest.fixture
def client(mock_container):
    """Create a test client with mocked dependencies."""
    app.dependency_overrides[_get_container] = lambda: mock_container
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


class TestServerErrorCode:
    """Tests for ServerErrorCode enum."""

    def test_error_code_values(self):
        """Test that error codes have expected values."""
        assert ServerErrorCode.FailedToReachServer == -1
        assert ServerErrorCode.CompileSqlFailure == 1
        assert ServerErrorCode.ExecuteSqlFailure == 2
        assert ServerErrorCode.ProjectParseFailure == 3
        assert ServerErrorCode.ProjectNotRegistered == 4
        assert ServerErrorCode.ProjectHeaderNotSupplied == 5
        assert ServerErrorCode.MissingRequiredParams == 6
        assert ServerErrorCode.StateInjectionFailure == 7


class TestServerError:
    """Tests for ServerError model."""

    def test_server_error_creation(self):
        """Test creating a ServerError."""
        error = ServerError(
            code=ServerErrorCode.CompileSqlFailure,
            message="Compilation failed",
            data={"line": 10}
        )
        assert error.code == ServerErrorCode.CompileSqlFailure
        assert error.message == "Compilation failed"
        assert error.data == {"line": 10}


class TestServerErrorContainer:
    """Tests for ServerErrorContainer model."""

    def test_server_error_container_creation(self):
        """Test creating a ServerErrorContainer."""
        error = ServerError(
            code=ServerErrorCode.ExecuteSqlFailure,
            message="Execution failed",
            data={}
        )
        container = ServerErrorContainer(error=error)
        assert container.error.code == ServerErrorCode.ExecuteSqlFailure
        assert container.error.message == "Execution failed"


class TestServerResultModels:
    """Tests for server result models."""

    def test_server_run_result(self):
        """Test ServerRunResult model."""
        result = ServerRunResult(
            column_names=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]],
            raw_code="select * from users",
            executed_code="select id, name from public.users"
        )
        assert result.column_names == ["id", "name"]
        assert len(result.rows) == 2
        assert result.raw_code == "select * from users"

    def test_server_compile_result(self):
        """Test ServerCompileResult model."""
        result = ServerCompileResult(result="SELECT 1")
        assert result.result == "SELECT 1"

    def test_server_register_result(self):
        """Test ServerRegisterResult model."""
        result = ServerRegisterResult(
            added="my_project",
            projects=["/path/to/project"]
        )
        assert result.added == "my_project"
        assert len(result.projects) == 1

    def test_server_unregister_result(self):
        """Test ServerUnregisterResult model."""
        result = ServerUnregisterResult(
            removed="my_project",
            projects=[]
        )
        assert result.removed == "my_project"
        assert result.projects == []

    def test_server_reset_result(self):
        """Test ServerResetResult model."""
        result = ServerResetResult(result="Success")
        assert result.result == "Success"


class TestGetStateFilePath:
    """Tests for _get_state_file_path function."""

    def test_default_state_file_path(self):
        """Test default state file path."""
        with patch.dict(os.environ, {}, clear=True):
            path = _get_state_file_path()
            assert path == Path.home() / ".dbt_core_interface_state.json"

    def test_custom_state_file_from_env(self):
        """Test custom state file path from environment variable."""
        custom_path = "/custom/state.json"
        with patch.dict(os.environ, {STATE_FILE: custom_path}):
            path = _get_state_file_path()
            assert path == Path(custom_path).expanduser().resolve()

    def test_state_file_path_expands_tilde(self):
        """Test that ~ is expanded in state file path."""
        with patch.dict(os.environ, {STATE_FILE: "~/custom/state.json"}):
            path = _get_state_file_path()
            assert path == Path("~/custom/state.json").expanduser().resolve()


class TestCreateErrorResponse:
    """Tests for _create_error_response function."""

    def test_create_error_response_basic(self):
        """Test creating a basic error response."""
        response = _create_error_response(
            ServerErrorCode.CompileSqlFailure,
            "SQL compilation failed"
        )
        assert isinstance(response, ServerErrorContainer)
        assert response.error.code == ServerErrorCode.CompileSqlFailure
        assert response.error.message == "SQL compilation failed"

    def test_create_error_response_with_data(self):
        """Test creating an error response with additional data."""
        data = {"line": 42, "column": 10}
        response = _create_error_response(
            ServerErrorCode.ExecuteSqlFailure,
            "Query execution failed",
            data
        )
        assert response.error.data == data

    def test_create_error_response_empty_data(self):
        """Test creating an error response with no data defaults to empty dict."""
        response = _create_error_response(
            ServerErrorCode.ProjectParseFailure,
            "Failed to parse project"
        )
        assert response.error.data == {}


class TestFormatRunResult:
    """Tests for _format_run_result function."""

    def test_format_run_result_basic(self):
        """Test formatting a basic execution result."""
        mock_table = MagicMock()
        mock_table.column_names = ["id", "name"]
        mock_table.rows = [[1, "Alice"], [2, "Bob"]]

        exec_result = MagicMock(spec=ExecutionResult)
        exec_result.table = mock_table
        exec_result.raw_code = "select * from users"
        exec_result.compiled_code = "select id, name from users"

        result = _format_run_result(exec_result)

        assert isinstance(result, ServerRunResult)
        assert result.column_names == ["id", "name"]
        assert result.rows == [[1, "Alice"], [2, "Bob"]]
        assert result.raw_code == "select * from users"
        assert result.executed_code == "select id, name from users"

    def test_format_run_result_empty_table(self):
        """Test formatting an empty execution result."""
        mock_table = MagicMock()
        mock_table.column_names = []
        mock_table.rows = []

        exec_result = MagicMock(spec=ExecutionResult)
        exec_result.table = mock_table
        exec_result.raw_code = "select 1 where 1=0"
        exec_result.compiled_code = "select 1 where 1=0"

        result = _format_run_result(exec_result)

        assert result.column_names == []
        assert result.rows == []


class TestLoadSavedState:
    """Tests for _load_saved_state function."""

    def test_load_state_disabled(self):
        """Test that loading is disabled when env var is set."""
        with patch.dict(os.environ, {DISABLE_AUTOLOAD: "true"}):
            container = MagicMock()
            _load_saved_state(container)
            container.create_project.assert_not_called()

    def test_load_state_disabled_lowercase(self):
        """Test that loading is disabled with lowercase 'yes'."""
        with patch.dict(os.environ, {DISABLE_AUTOLOAD: "yes"}):
            container = MagicMock()
            _load_saved_state(container)
            container.create_project.assert_not_called()

    def test_load_state_file_not_exists(self):
        """Test loading when state file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "nonexistent.json"
            with patch.dict(os.environ, {STATE_FILE: str(state_path)}, clear=False):
                container = MagicMock()
                _load_saved_state(container)
                container.create_project.assert_not_called()

    def test_load_state_invalid_content(self):
        """Test loading with invalid state file content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("not a list")

            with patch.dict(os.environ, {STATE_FILE: str(state_path)}, clear=False):
                container = MagicMock()
                _load_saved_state(container)
                container.create_project.assert_not_called()

    def test_load_state_success(self):
        """Test successful state loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_data = [
                {
                    "project": "test_project",
                    "project_dir": tmpdir,
                    "profiles_dir": None,
                    "target": "dev",
                    "threads": 4,
                    "vars": {"key": "value"}
                }
            ]
            state_path.write_text(json.dumps(state_data))

            with patch.dict(os.environ, {STATE_FILE: str(state_path)}, clear=False):
                container = MagicMock()
                container.create_project.return_value = MagicMock()
                _load_saved_state(container)
                container.create_project.assert_called_once()


class TestSaveState:
    """Tests for _save_state function."""

    def test_save_state_basic(self, mock_runner):
        """Test basic state saving."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            with patch.dict(os.environ, {STATE_FILE: str(state_path)}, clear=False):
                container = MagicMock()
                container.registered_projects.return_value = [Path(tmpdir)]
                container.get_project.return_value = mock_runner
                mock_runner.project_name = "test_project"
                mock_runner.args = MagicMock()
                mock_runner.args.project_dir = tmpdir
                mock_runner.args.profiles_dir = None
                mock_runner.args.target = "dev"
                mock_runner.args.threads = 1
                mock_runner.args.vars = {}

                _save_state(container)

                assert state_path.exists()
                content = json.loads(state_path.read_text())
                assert isinstance(content, list)
                assert len(content) == 1
                assert content[0]["project"] == "test_project"

    def test_save_state_creates_directory(self, mock_runner):
        """Test that save creates parent directory if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "nested" / "dir" / "state.json"
            with patch.dict(os.environ, {STATE_FILE: str(state_path)}, clear=False):
                container = MagicMock()
                container.registered_projects.return_value = [Path(tmpdir)]
                container.get_project.return_value = mock_runner
                mock_runner.project_name = "test_project"
                mock_runner.args = MagicMock()
                mock_runner.args.project_dir = tmpdir
                mock_runner.args.profiles_dir = None
                mock_runner.args.target = None
                mock_runner.args.threads = 1
                mock_runner.args.vars = {}

                _save_state(container)

                assert state_path.exists()
                assert state_path.parent.exists()


class TestStatusEndpoints:
    """Tests for status and health check endpoints."""

    def test_heartbeat_endpoint(self, client):
        """Test the heartbeat endpoint."""
        response = client.get("/api/v1/heartbeat")
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["result"]["status"] == "ready"

    def test_status_endpoint(self, client, mock_runner):
        """Test the status endpoint."""
        response = client.get("/api/v1/status")
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["result"]["status"] == "ready"
        assert data["result"]["project_name"] == "test_project"
        assert "timestamp" in data["result"]
        assert "id" in data

    def test_legacy_health_endpoint(self, client, mock_runner):
        """Test the legacy /health endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["result"]["status"] == "ready"


class TestListProjects:
    """Tests for list projects endpoint."""

    def test_list_projects_empty(self, client, mock_container):
        """Test listing projects when none registered."""
        mock_container.__iter__ = Mock(return_value=iter([]))
        response = client.get("/api/v1/projects")
        assert response.status_code == 200
        data = response.json()
        assert "projects" in data
        assert data["projects"] == []

    def test_list_projects_with_projects(self, client, mock_runner):
        """Test listing registered projects."""
        response = client.get("/api/v1/projects")
        assert response.status_code == 200
        data = response.json()
        assert "projects" in data
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "test_project"
        assert data["projects"][0]["target"] == "dev"


class TestMiddleware:
    """Tests for middleware."""

    def test_execution_time_header(self, client):
        """Test that execution time is added to response headers."""
        response = client.get("/api/v1/heartbeat")
        assert "X-dbt-Exec-Time" in response.headers
        # Should be a float representation of seconds
        exec_time = float(response.headers["X-dbt-Exec-Time"])
        assert exec_time >= 0


class TestLifespan:
    """Tests for lifespan context manager."""

    def test_lifespan_is_callable(self):
        """Test that lifespan is a callable context manager."""
        from dbt_core_interface.server import lifespan
        assert callable(lifespan)


class TestQualityCheckConfigModels:
    """Tests for quality check config models."""

    def test_quality_check_config_defaults(self):
        """Test QualityCheckConfig default values."""
        from dbt_core_interface.server import QualityCheckConfig

        config = QualityCheckConfig(
            name="test_check",
            check_type=QualityCheckType.ROW_COUNT,
            model_name="my_model"
        )
        assert config.description == ""
        assert config.severity == Severity.WARNING
        assert config.enabled is True
        assert config.config == {}

    def test_quality_check_config_with_values(self):
        """Test QualityCheckConfig with custom values."""
        from dbt_core_interface.server import QualityCheckConfig

        config = QualityCheckConfig(
            name="test_check",
            check_type=QualityCheckType.NULL_PERCENTAGE,
            model_name="my_model",
            description="Check for nulls",
            severity=Severity.ERROR,
            enabled=False,
            config={"column_name": "id", "max_null_percentage": 0.1}
        )
        assert config.description == "Check for nulls"
        assert config.severity == Severity.ERROR
        assert config.enabled is False
        assert config.config["column_name"] == "id"


class TestAlertChannelConfig:
    """Tests for AlertChannelConfig model."""

    def test_alert_channel_config_defaults(self):
        """Test AlertChannelConfig default values."""
        from dbt_core_interface.server import AlertChannelConfig

        config = AlertChannelConfig(channel_type="webhook")
        assert config.config == {}

    def test_alert_channel_config_with_config(self):
        """Test AlertChannelConfig with custom config."""
        from dbt_core_interface.server import AlertChannelConfig

        config = AlertChannelConfig(
            channel_type="webhook",
            config={"url": "https://example.com", "timeout": 10}
        )
        assert config.config["url"] == "https://example.com"
        assert config.config["timeout"] == 10


class TestTestSuggestionModels:
    """Tests for test suggestion result models."""

    def test_test_suggestion_result(self):
        """Test TestSuggestionResult model."""
        from dbt_core_interface.server import TestSuggestionResult

        result = TestSuggestionResult(
            model="my_model",
            unique_id="model.project.my_model",
            path="models/my_model.sql",
            suggestions=[
                {"test": "unique", "column": "id"},
                {"test": "not_null", "column": "id"}
            ]
        )
        assert result.model == "my_model"
        assert len(result.suggestions) == 2

    def test_server_test_suggestion_result(self):
        """Test ServerTestSuggestionResult model."""
        from dbt_core_interface.server import (
            ServerTestSuggestionResult,
            TestSuggestionResult
        )

        result = ServerTestSuggestionResult(result=[
            TestSuggestionResult(
                model="my_model",
                unique_id="model.project.my_model",
                path="models/my_model.sql",
                suggestions=[]
            )
        ])
        assert len(result.result) == 1

    def test_server_test_yml_result(self):
        """Test ServerTestYmlResult model."""
        from dbt_core_interface.server import ServerTestYmlResult

        result = ServerTestYmlResult(result="version: 2\nmodels:\n  - name: test")
        assert result.result.startswith("version:")


class TestGetContainer:
    """Tests for _get_container function."""

    def test_get_container_returns_instance(self):
        """Test that _get_container returns a DbtProjectContainer instance."""
        from dbt_core_interface.container import DbtProjectContainer

        container = _get_container()
        assert isinstance(container, DbtProjectContainer)


class TestAppConfiguration:
    """Tests for FastAPI app configuration."""

    def test_app_metadata(self):
        """Test that app has correct metadata."""
        assert app.title == "dbt-core-interface API"
        assert app.version == "1.0"

    def test_app_has_openapi(self):
        """Test that OpenAPI schema is available."""
        assert app.openapi_schema is not None or app.openapi() is not None
