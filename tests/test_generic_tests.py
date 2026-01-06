#!/usr/bin/env python
# pyright: reportPrivateImportUsage=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Tests for the Generic Test Library module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dbt_core_interface.generic_tests import (
    GenericTestConfig,
    GenericTestDefinition,
    GenericTestLibrary,
    GenericTestType,
    TestSeverity,
)
from dbt_core_interface.project import DbtProject


@pytest.fixture
def mock_project():
    """Create a mock DbtProject for testing."""
    project = MagicMock(spec=DbtProject)
    project.project_name = "test_project"
    return project


@pytest.fixture
def test_library(mock_project):
    """Create a GenericTestLibrary instance for testing."""
    return GenericTestLibrary(mock_project)


class TestGenericTestConfig:
    """Tests for GenericTestConfig."""

    def test_to_yaml_with_column(self):
        """Test YAML generation with column name."""
        config = GenericTestConfig(
            test_type=GenericTestType.UNIQUE,
            column_name="id",
        )
        yaml_output = config.to_yaml("users")
        assert "unique:" in yaml_output
        assert "users.id" in yaml_output

    def test_to_yaml_with_severity_warn(self):
        """Test YAML generation with warn severity."""
        config = GenericTestConfig(
            test_type=GenericTestType.NOT_NULL,
            column_name="email",
            severity=TestSeverity.WARN,
        )
        yaml_output = config.to_yaml("users")
        assert "not_null:" in yaml_output
        assert "severity: warn" in yaml_output

    def test_to_yaml_with_config_dict(self):
        """Test YAML generation with config dict."""
        config = GenericTestConfig(
            test_type=GenericTestType.ACCEPTED_VALUES,
            column_name="status",
            config={"values": ["active", "inactive"]},
        )
        yaml_output = config.to_yaml("users")
        assert "accepted_values:" in yaml_output
        assert "'active', 'inactive'" in yaml_output


class TestGenericTestDefinition:
    """Tests for GenericTestDefinition."""

    def test_render_config(self):
        """Test rendering a config from definition."""
        definition = GenericTestDefinition(
            name="test_not_null",
            description="Test description",
            test_type=GenericTestType.NOT_NULL,
            sql_template="SELECT * FROM {{ this }} WHERE {{ column_name }} IS NULL",
            required_args=["column_name"],
        )
        config = definition.render_config(column_name="email")
        assert config.test_type == GenericTestType.NOT_NULL
        assert config.config["column_name"] == "email"

    def test_render_config_with_none_values(self):
        """Test that None values are excluded from config."""
        definition = GenericTestDefinition(
            name="test_unique",
            description="Test description",
            test_type=GenericTestType.UNIQUE,
            sql_template="SELECT {{ column_name }} FROM {{ this }}",
            required_args=["column_name"],
        )
        config = definition.render_config(column_name="id", severity=None)
        assert "severity" not in config.config


class TestGenericTestLibrary:
    """Tests for GenericTestLibrary."""

    def test_initialization(self, mock_project):
        """Test library initialization."""
        library = GenericTestLibrary(mock_project)
        assert library.project == mock_project
        assert len(library.list_tests()) > 0

    def test_default_tests_registered(self, test_library):
        """Test that default tests are registered."""
        tests = test_library.list_tests()
        test_names = {t.name for t in tests}

        # Check for core tests
        assert "unique" in test_names
        assert "not_null" in test_names
        assert "relationships" in test_names
        assert "accepted_values" in test_names
        assert "recency" in test_names
        assert "cardinality_equals" in test_names

    def test_get_existing_test(self, test_library):
        """Test getting an existing test."""
        test = test_library.get("unique")
        assert test is not None
        assert test.name == "unique"
        assert test.test_type == GenericTestType.UNIQUE

    def test_get_nonexistent_test(self, test_library):
        """Test getting a non-existent test."""
        test = test_library.get("nonexistent_test")
        assert test is None

    def test_register_custom_test(self, test_library):
        """Test registering a custom test."""
        custom_test = GenericTestDefinition(
            name="custom_test",
            description="A custom test",
            test_type=GenericTestType.CUSTOM_SQL,
            sql_template="SELECT 1",
        )
        test_library.register(custom_test)

        retrieved = test_library.get("custom_test")
        assert retrieved is not None
        assert retrieved.name == "custom_test"

    def test_suggest_tests_for_primary_key(self, test_library):
        """Test suggestions for primary key column."""
        suggestions = test_library.suggest_tests_for_column("id")

        test_types = {s.test_type for s in suggestions}
        assert GenericTestType.UNIQUE in test_types
        assert GenericTestType.NOT_NULL in test_types

    def test_suggest_tests_for_foreign_key(self, test_library):
        """Test suggestions for foreign key column."""
        suggestions = test_library.suggest_tests_for_column("user_id")

        test_types = {s.test_type for s in suggestions}
        assert GenericTestType.NOT_NULL in test_types

    def test_suggest_tests_for_email(self, test_library):
        """Test suggestions for email column."""
        suggestions = test_library.suggest_tests_for_column("email")

        test_types = {s.test_type for s in suggestions}
        assert GenericTestType.NOT_NULL in test_types

    def test_suggest_tests_for_status(self, test_library):
        """Test suggestions for status column."""
        suggestions = test_library.suggest_tests_for_column("status")

        test_types = {s.test_type for s in suggestions}
        assert GenericTestType.ACCEPTED_VALUES in test_types

    def test_suggest_tests_for_timestamp(self, test_library):
        """Test suggestions for timestamp column."""
        suggestions = test_library.suggest_tests_for_column("created_at")

        test_types = {s.test_type for s in suggestions}
        assert GenericTestType.NOT_NULL in test_types
        assert GenericTestType.RECENCY in test_types

    def test_generate_schema_yml(self, test_library):
        """Test generating schema YAML."""
        columns = {
            "id": {"description": "Primary key"},
            "email": {"description": "Email address"},
        }

        schema_yml = test_library.generate_schema_yml("users", columns)

        assert "version: 2" in schema_yml
        assert "name: users" in schema_yml
        assert "name: id" in schema_yml
        assert "name: email" in schema_yml

    def test_generate_schema_yml_includes_tests(self, test_library):
        """Test that generated schema includes suggested tests."""
        columns = {
            "id": {},
            "user_id": {},
        }

        schema_yml = test_library.generate_schema_yml("orders", columns)

        # Should include tests for these column types
        assert "tests:" in schema_yml

    def test_column_case_insensitive_matching(self, test_library):
        """Test that column matching is case-insensitive."""
        suggestions_upper = test_library.suggest_tests_for_column("ID")
        suggestions_lower = test_library.suggest_tests_for_column("id")

        # Should return same number of suggestions
        assert len(suggestions_upper) == len(suggestions_lower)

    def test_suffix_matching_for_foreign_keys(self, test_library):
        """Test suffix matching for foreign key patterns."""
        # Various foreign key patterns
        for col_name in ["user_id", "customer_uuid", "order_guid"]:
            suggestions = test_library.suggest_tests_for_column(col_name)
            test_types = {s.test_type for s in suggestions}
            assert GenericTestType.NOT_NULL in test_types


class TestGenericTestType:
    """Tests for GenericTestType enum."""

    def test_test_type_values(self):
        """Test that test type values match expected strings."""
        assert GenericTestType.UNIQUE.value == "unique"
        assert GenericTestType.NOT_NULL.value == "not_null"
        assert GenericTestType.RELATIONSHIPS.value == "relationships"
        assert GenericTestType.ACCEPTED_VALUES.value == "accepted_values"
        assert GenericTestType.RECENCY.value == "recency"
        assert GenericTestType.CARDINALITY_EQUALS.value == "cardinality_equals"


class TestTestSeverity:
    """Tests for TestSeverity enum."""

    def test_severity_values(self):
        """Test that severity values match expected strings."""
        assert TestSeverity.ERROR.value == "error"
        assert TestSeverity.WARN.value == "warn"


@pytest.mark.parametrize(
    "column_name,expected_tests",
    [
        ("id", ["unique", "not_null"]),
        ("user_id", ["not_null"]),
        ("email", ["not_null"]),
        ("status", ["accepted_values"]),
        ("created_at", ["not_null", "recency"]),
    ],
)
def test_column_patterns(test_library, column_name, expected_tests):
    """Parametrized test for column pattern matching."""
    suggestions = test_library.suggest_tests_for_column(column_name)
    suggested_types = [s.test_type.value for s in suggestions]

    for expected_test in expected_tests:
        assert expected_test in suggested_types
