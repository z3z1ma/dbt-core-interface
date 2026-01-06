"""Test cases for the test_suggester module."""

import pytest

from dbt_core_interface.test_suggester import (
    ColumnPattern,
    DEFAULT_PATTERNS,
    ProjectTestPatterns,
    TestSuggester,
    TestSuggestion,
    TestType,
)


class TestColumnPattern:
    """Tests for ColumnPattern."""

    def test_primary_key_pattern(self) -> None:
        """Test primary key pattern matching."""
        pattern = ColumnPattern(
            name="primary_key",
            regex=r"^(?:.*_)?(?:id|uuid|guid|pk)$",
            description="Primary key column",
            suggested_tests=[TestType.UNIQUE, TestType.NOT_NULL],
        )

        assert pattern.matches("id")
        assert pattern.matches("user_id")
        assert pattern.matches("uuid")
        assert pattern.matches("order_uuid")
        assert not pattern.matches("name")
        assert not pattern.matches("status")

    def test_foreign_key_pattern(self) -> None:
        """Test foreign key pattern matching."""
        pattern = ColumnPattern(
            name="foreign_key",
            regex=r"^(?:.*_)?(?:.*_id|.*_uuid|.*_guid|.*_fk)$",
            description="Foreign key column",
            suggested_tests=[TestType.NOT_NULL, TestType.RELATIONSHIPS],
        )

        assert pattern.matches("user_id")
        assert pattern.matches("order_uuid")
        assert pattern.matches("customer_guid")
        assert not pattern.matches("id")
        assert not pattern.matches("name")

    def test_timestamp_pattern(self) -> None:
        """Test timestamp pattern matching."""
        pattern = ColumnPattern(
            name="timestamp",
            regex=r"^(?:.*_)?(?:created_at|updated_at|deleted_at|timestamp|date|time)$",
            description="Timestamp column",
            suggested_tests=[TestType.NOT_NULL],
        )

        assert pattern.matches("created_at")
        assert pattern.matches("updated_at")
        assert pattern.matches("deleted_at")
        assert pattern.matches("timestamp")
        assert not pattern.matches("name")

    def test_pattern_case_insensitive(self) -> None:
        """Test that patterns are case-insensitive."""
        pattern = ColumnPattern(
            name="primary_key",
            regex=r"^(?:.*_)?(?:id|uuid|guid|pk)$",
            description="Primary key column",
            suggested_tests=[TestType.UNIQUE, TestType.NOT_NULL],
        )

        assert pattern.matches("ID")
        assert pattern.matches("User_Id")
        assert pattern.matches("UUID")


class TestProjectTestPatterns:
    """Tests for ProjectTestPatterns."""

    def test_add_and_get_column_pattern(self) -> None:
        """Test adding and retrieving column patterns."""
        patterns = ProjectTestPatterns()
        patterns.add_column_pattern("user_id", [TestType.UNIQUE, TestType.NOT_NULL])

        tests = patterns.get_tests_for_column("user_id")
        # Use set comparison since order may vary
        assert set(tests) == {TestType.UNIQUE, TestType.NOT_NULL}

    def test_column_pattern_normalized(self) -> None:
        """Test that column names are normalized to lowercase."""
        patterns = ProjectTestPatterns()
        patterns.add_column_pattern("User_ID", [TestType.UNIQUE])
        patterns.add_column_pattern("user_id", [TestType.NOT_NULL])

        tests = patterns.get_tests_for_column("USER_ID")
        assert TestType.UNIQUE in tests
        assert TestType.NOT_NULL in tests

    def test_suffix_matching(self) -> None:
        """Test suffix matching for learned patterns."""
        patterns = ProjectTestPatterns()
        patterns.add_column_pattern("_id", [TestType.NOT_NULL, TestType.RELATIONSHIPS])

        tests = patterns.get_tests_for_column("user_id")
        assert TestType.NOT_NULL in tests
        assert TestType.RELATIONSHIPS in tests

    def test_no_match_returns_empty(self) -> None:
        """Test that non-matching columns return empty list."""
        patterns = ProjectTestPatterns()
        patterns.add_column_pattern("_id", [TestType.NOT_NULL])

        tests = patterns.get_tests_for_column("name")
        assert tests == []

    def test_deduplication(self) -> None:
        """Test that duplicate test types are deduplicated."""
        patterns = ProjectTestPatterns()
        patterns.add_column_pattern("user_id", [TestType.UNIQUE, TestType.NOT_NULL])
        patterns.add_column_pattern("user_id", [TestType.UNIQUE, TestType.RELATIONSHIPS])

        tests = patterns.get_tests_for_column("user_id")
        assert len(tests) == 3
        assert TestType.UNIQUE in tests
        assert TestType.NOT_NULL in tests
        assert TestType.RELATIONSHIPS in tests


class TestTestSuggestion:
    """Tests for TestSuggestion."""

    def test_to_yaml_with_column(self) -> None:
        """Test YAML generation with column name."""
        suggestion = TestSuggestion(
            test_type=TestType.UNIQUE,
            column_name="user_id",
            reason="Primary key column",
        )

        yaml = suggestion.to_yaml("users")
        assert "- unique:" in yaml
        assert "users.user_id" in yaml

    def test_to_yaml_with_config(self) -> None:
        """Test YAML generation with config."""
        suggestion = TestSuggestion(
            test_type=TestType.RELATIONSHIPS,
            column_name="user_id",
            reason="Foreign key to users table",
            config={"to": "ref('users')", "field": "id"},
        )

        yaml = suggestion.to_yaml("orders")
        assert "- relationships:" in yaml
        assert "orders.user_id" in yaml
        assert "to: ref('users')" in yaml


class MockNode:
    """Mock ManifestNode for testing."""

    def __init__(
        self,
        name: str,
        unique_id: str,
        path: str,
        columns: dict[str, dict[str, str]],
        resource_type: str = "model",
    ) -> None:
        self.name = name
        self.unique_id = unique_id
        self.original_file_path = path
        self.columns = columns
        self.resource_type = resource_type
        self.description = ""


class TestTestSuggester:
    """Tests for TestSuggester."""

    def test_suggest_for_primary_key(self) -> None:
        """Test suggesting tests for primary key column."""
        suggester = TestSuggester()
        node = MockNode(
            name="users",
            unique_id="model.project.users",
            path="models/users.sql",
            columns={"id": {"name": "id", "description": "User ID"}},
        )

        suggestions = suggester.suggest_tests_for_model(node)

        test_types = [s.test_type for s in suggestions]
        assert TestType.UNIQUE in test_types
        assert TestType.NOT_NULL in test_types

        # Check that at least one suggestion is for the id column
        id_suggestions = [s for s in suggestions if s.column_name == "id"]
        assert len(id_suggestions) > 0

    def test_suggest_for_foreign_key(self) -> None:
        """Test suggesting tests for foreign key column."""
        suggester = TestSuggester()
        node = MockNode(
            name="orders",
            unique_id="model.project.orders",
            path="models/orders.sql",
            columns={
                "user_id": {"name": "user_id", "description": "Foreign key to users"},
            },
        )

        suggestions = suggester.suggest_tests_for_model(node)

        test_types = [s.test_type for s in suggestions]
        assert TestType.NOT_NULL in test_types
        assert TestType.RELATIONSHIPS in test_types

    def test_suggest_for_timestamps(self) -> None:
        """Test suggesting tests for timestamp columns."""
        suggester = TestSuggester()
        node = MockNode(
            name="users",
            unique_id="model.project.users",
            path="models/users.sql",
            columns={
                "created_at": {"name": "created_at", "description": "Creation time"},
                "updated_at": {"name": "updated_at", "description": "Update time"},
            },
        )

        suggestions = suggester.suggest_tests_for_model(node)

        # Should have not_null suggestions for timestamps
        timestamp_suggestions = [
            s for s in suggestions if s.column_name in ("created_at", "updated_at")
        ]
        assert len(timestamp_suggestions) >= 2

        for s in timestamp_suggestions:
            assert s.test_type == TestType.NOT_NULL

    def test_suggest_with_learned_patterns(self) -> None:
        """Test that learned patterns take precedence."""
        learned = ProjectTestPatterns()
        # Learn that status columns should have accepted_values test
        learned.add_column_pattern("status", [TestType.ACCEPTED_VALUES])

        suggester = TestSuggester(learned_patterns=learned)
        node = MockNode(
            name="orders",
            unique_id="model.project.orders",
            path="models/orders.sql",
            columns={"status": {"name": "status", "description": "Order status"}},
        )

        suggestions = suggester.suggest_tests_for_model(node)

        # Should have the learned test
        status_suggestions = [s for s in suggestions if s.column_name == "status"]
        assert len(status_suggestions) > 0
        assert any(s.test_type == TestType.ACCEPTED_VALUES for s in status_suggestions)

    def test_fact_table_detection(self) -> None:
        """Test fact table detection."""
        suggester = TestSuggester()
        node = MockNode(
            name="orders",
            unique_id="model.project.orders",
            path="models/orders.sql",
            columns={
                "id": {"name": "id"},
                "user_id": {"name": "user_id"},
                "product_id": {"name": "product_id"},
                "store_id": {"name": "store_id"},
                "amount": {"name": "amount"},
            },
        )

        # Should detect as fact table (many _id columns)
        assert suggester._looks_like_fact_table(node)

    def test_dim_table_not_fact(self) -> None:
        """Test that dimension tables are not detected as fact tables."""
        suggester = TestSuggester()
        node = MockNode(
            name="users",
            unique_id="model.project.users",
            path="models/users.sql",
            columns={
                "id": {"name": "id"},
                "name": {"name": "name"},
                "email": {"name": "email"},
                "created_at": {"name": "created_at"},
            },
        )

        # Should not detect as fact table (few _id columns)
        assert not suggester._looks_like_fact_table(node)

    def test_generate_test_yml(self) -> None:
        """Test YAML generation for a model."""
        suggester = TestSuggester()
        node = MockNode(
            name="users",
            unique_id="model.project.users",
            path="models/users.sql",
            columns={
                "id": {"name": "id", "description": "User ID"},
                "email": {"name": "email", "description": "Email address"},
            },
        )

        yml = suggester.generate_test_yml(node)

        assert "version: 2" in yml
        assert "models:" in yml
        assert "- name: users" in yml
        assert "columns:" in yml
        assert "- name: id" in yml
        assert "- name: email" in yml

    def test_custom_patterns(self) -> None:
        """Test custom pattern addition."""
        custom = [
            ColumnPattern(
                name="custom_id",
                regex=r"^custom_.*_id$",
                description="Custom foreign key",
                suggested_tests=[TestType.NOT_NULL],
            )
        ]
        suggester = TestSuggester(custom_patterns=custom)
        node = MockNode(
            name="test",
            unique_id="model.project.test",
            path="models/test.sql",
            columns={"custom_user_id": {"name": "custom_user_id"}},
        )

        suggestions = suggester.suggest_tests_for_model(node)
        custom_suggestions = [s for s in suggestions if s.column_name == "custom_user_id"]

        assert len(custom_suggestions) > 0
        assert TestType.NOT_NULL in [s.test_type for s in custom_suggestions]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
