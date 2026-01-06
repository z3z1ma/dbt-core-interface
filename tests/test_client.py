"""Tests for the client module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from dbt_core_interface.client import DbtInterfaceClient, ServerErrorException
from dbt_core_interface.server import (
    ServerErrorCode,
    ServerError,
    ServerErrorContainer,
    ServerRegisterResult,
    ServerUnregisterResult,
    ServerRunResult,
    ServerCompileResult,
    ServerLintResult,
    ServerFormatResult,
    ServerResetResult,
)


class TestServerErrorException:
    """Tests for ServerErrorException."""

    def test_exception_creation(self):
        """Test creating ServerErrorException."""
        error = ServerError(
            code=ServerErrorCode.CompileSqlFailure,
            message="Compilation failed",
            data={"line": 42}
        )
        exc = ServerErrorException(error)
        assert exc.code == ServerErrorCode.CompileSqlFailure
        assert exc.message == "Compilation failed"
        assert exc.data == {"line": 42}

    def test_exception_str_representation(self):
        """Test string representation of exception."""
        error = ServerError(
            code=ServerErrorCode.ExecuteSqlFailure,
            message="Query failed",
            data={}
        )
        exc = ServerErrorException(error)
        # The actual representation includes the enum class name
        assert "ExecuteSqlFailure" in str(exc)
        assert "Query failed" in str(exc)

    def test_exception_is_base_exception(self):
        """Test that ServerErrorException is an Exception."""
        error = ServerError(
            code=ServerErrorCode.ProjectParseFailure,
            message="Parse error",
            data={}
        )
        exc = ServerErrorException(error)
        assert isinstance(exc, Exception)


class TestDbtInterfaceClientRequest:
    """Tests for _request method."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_request_adds_project_dir_param(self, mock_session_cls):
        """Test that request adds project_dir parameter."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/my/project")
        mock_session.request.reset_mock()  # Reset after init
        mock_response.json.return_value = {"result": "ok"}
        mock_session.request.return_value = mock_response

        client._request("GET", "/api/v1/status", params={"custom": "value"})

        call_kwargs = mock_session.request.call_args.kwargs
        params = call_kwargs.get("params", {})
        assert "project_dir" in params
        assert "custom" in params


class TestDbtInterfaceClientProperties:
    """Tests for client properties and attributes."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_client_properties(self, mock_session_cls):
        """Test basic client properties."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(
            project_dir="/path/to/project",
            profiles_dir="/path/to/profiles",
            target="production",
            base_url="http://remote:8581",
            timeout=30.0,
            unregister_on_close=False
        )

        assert client.project_dir == Path("/path/to/project").resolve()
        assert client.base_url == "http://remote:8581"
        assert client.timeout == 30.0
        assert client.unregister_on_close is False

    @patch("dbt_core_interface.client.requests.Session")
    def test_client_base_url_trailing_slash(self, mock_session_cls):
        """Test that trailing slash is removed from base_url."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(
            project_dir="/path",
            base_url="http://localhost:8581/"
        )

        assert client.base_url == "http://localhost:8581"

    @patch("dbt_core_interface.client.requests.Session")
    def test_init_timeout_tuple(self, mock_session_cls):
        """Test client initialization with tuple timeout."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(
            project_dir="/path",
            timeout=(5.0, 30.0)
        )

        assert client.timeout == (5.0, 30.0)


class TestDbtInterfaceClientClose:
    """Tests for DbtInterfaceClient.close method."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_close_with_unregister_on_close_false(self, mock_session_cls):
        """Test that close doesn't unregister when flag is False."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path", unregister_on_close=False)
        mock_session.reset_mock()
        client.close()

        # Should not make request to unregister
        assert not mock_session.request.called

    @patch("dbt_core_interface.client.requests.Session")
    def test_close_handles_unregistration_error(self, mock_session_cls):
        """Test that close handles unregistration errors gracefully."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path")
        mock_session.request.side_effect = Exception("Connection lost")
        # Should not raise exception
        client.close()


class TestDbtInterfaceClientContextManager:
    """Tests for DbtInterfaceClient context manager."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_context_manager_enter_returns_self(self, mock_session_cls):
        """Test that __enter__ returns self."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path")
        with client as ctx:
            assert ctx is client

    @patch("dbt_core_interface.client.requests.Session")
    def test_context_manager_exit_closes_session(self, mock_session_cls):
        """Test that __exit__ closes the session."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "added": "test",
            "projects": ["/path"],
            "removed": "test",
            "projects": []
        }
        mock_session.request.return_value = mock_response

        with DbtInterfaceClient(project_dir="/path") as client:
            pass

        mock_session.close.assert_called_once()


class TestDbtInterfaceClientTimeout:
    """Tests for timeout handling."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_request_uses_timeout(self, mock_session_cls):
        """Test that request uses configured timeout."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path", timeout=5.0)

        call_kwargs = mock_session.request.call_args.kwargs
        assert call_kwargs.get("timeout") == 5.0

    @patch("dbt_core_interface.client.requests.Session")
    def test_request_uses_tuple_timeout(self, mock_session_cls):
        """Test that request uses tuple timeout."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path", timeout=(3.0, 10.0))

        call_kwargs = mock_session.request.call_args.kwargs
        assert call_kwargs.get("timeout") == (3.0, 10.0)


class TestDbtInterfaceClientPartialMethods:
    """Tests for partial method attributes."""

    @patch("dbt_core_interface.client.requests.Session")
    def test_partial_methods_exist(self, mock_session_cls):
        """Test that partial methods exist for common dbt commands."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"added": "test", "projects": ["/path"]}
        mock_session.request.return_value = mock_response

        client = DbtInterfaceClient(project_dir="/path")

        # These should all be callable methods
        assert callable(client.run)
        assert callable(client.test)
        assert callable(client.build)
        assert callable(client.compile)
        assert callable(client.clean)
        assert callable(client.debug)
        assert callable(client.deps)
        assert callable(client.docs_generate)
        assert callable(client.list)
        assert callable(client.parse)
        assert callable(client.seed)
        assert callable(client.show)
        assert callable(client.snapshot)
        assert callable(client.source_freshness)
        assert callable(client.run_operation)
