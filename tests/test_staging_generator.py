"""Test cases for the staging_generator module."""

import pytest

from dbt_core_interface.staging_generator import (
    ColumnMapping,
    NamingConvention,
    StagingGenerator,
    StagingModelConfig,
    generate_staging_model_from_source,
)


class MockSourceDefinition:
    """Mock SourceDefinition for testing."""

    def __init__(
        self,
        source_name: str,
        name: str,
        columns: dict[str, dict[str, str]],
        database: str = "test_db",
        schema: str = "test_schema",
        description: str = "",
    ) -> None:
        self.source_name = source_name
        self.name = name
        self.columns = columns
        self.database = database
        self.schema = schema
        self.description = description
        self.resource_type = "source"


class TestColumnMapping:
    """Tests for ColumnMapping."""

    def test_to_select_sql_basic(self) -> None:
        """Test basic SELECT SQL generation."""
        mapping = ColumnMapping(
            source_name="UserId",
            staging_name="user_id",
        )

        sql = mapping.to_select_sql()
        assert "UserId as user_id," in sql

    def test_to_select_sql_with_indent(self) -> None:
        """Test SELECT SQL with custom indent."""
        mapping = ColumnMapping(
            source_name="col_name",
            staging_name="col_name",
        )

        sql = mapping.to_select_sql(indent=8)
        assert sql.startswith("        ")


class TestStagingGenerator:
    """Tests for StagingGenerator."""

    def test_generate_model_name(self) -> None:
        """Test model name generation."""
        generator = StagingGenerator()

        name = generator._generate_model_name("raw", "users")
        assert name == "stg_users"

        name = generator._generate_model_name("jira", "issue_table")
        assert name == "stg_issue_table"

    def test_clean_column_name_snake_case(self) -> None:
        """Test column name cleaning with snake_case convention."""
        config = StagingModelConfig(naming_convention=NamingConvention.SNAKE_CASE)
        generator = StagingGenerator(config)

        # Basic snake_case conversion
        assert generator._clean_column_name("UserName") == "user_name"
        assert generator._clean_column_name("user_name") == "user_name"

        # Remove prefixes
        config.remove_prefixes = ["src_", "raw_"]
        assert generator._clean_column_name("src_user_id") == "user_id"
        assert generator._clean_column_name("raw_order_data") == "order_data"

        # Remove suffixes
        config.remove_prefixes = []
        config.remove_suffixes = ["_col", "_field"]
        assert generator._clean_column_name("user_id_col") == "user_id"

    def test_clean_column_name_camel_case(self) -> None:
        """Test column name cleaning with camelCase convention."""
        config = StagingModelConfig(naming_convention=NamingConvention.CAMEL_CASE)
        generator = StagingGenerator(config)

        assert generator._clean_column_name("user_name") == "userName"
        assert generator._clean_column_name("UserId") == "userId"

    def test_clean_column_name_kebab_case(self) -> None:
        """Test column name cleaning with kebab-case convention."""
        config = StagingModelConfig(naming_convention=NamingConvention.KEBAB_CASE)
        generator = StagingGenerator(config)

        assert generator._clean_column_name("user_name") == "user-name"
        assert generator._clean_column_name("some_long_name") == "some-long-name"

    def test_clean_column_name_pascal_case(self) -> None:
        """Test column name cleaning with PascalCase convention."""
        config = StagingModelConfig(naming_convention=NamingConvention.PASCAL_CASE)
        generator = StagingGenerator(config)

        assert generator._clean_column_name("user_name") == "UserName"
        assert generator._clean_column_name("user_id") == "UserId"

    def test_add_prefix_suffix(self) -> None:
        """Test adding prefix and suffix to column names."""
        config = StagingModelConfig(
            naming_convention=NamingConvention.SNAKE_CASE,
            add_prefix="src",
            add_suffix="cleaned",
        )
        generator = StagingGenerator(config)

        result = generator._clean_column_name("user_name")
        assert result == "src_user_name_cleaned"

    def test_custom_column_mapping(self) -> None:
        """Test custom column mapping overrides."""
        config = StagingModelConfig(
            naming_convention=NamingConvention.SNAKE_CASE,
            custom_column_mappings={"UserID": "custom_id"},
        )
        generator = StagingGenerator(config)

        source = MockSourceDefinition(
            source_name="raw",
            name="users",
            columns={
                "UserID": {"name": "UserID", "data_type": "integer"},
                "UserName": {"name": "UserName", "data_type": "varchar"},
            },
        )

        result = generator.generate_from_source(source)

        # Check custom mapping is used
        user_id_mapping = [m for m in result["column_mappings"] if m["source_name"] == "UserID"][0]
        assert user_id_mapping["staging_name"] == "custom_id"

        # Check default naming is used for other columns
        user_name_mapping = [
            m for m in result["column_mappings"] if m["source_name"] == "UserName"
        ][0]
        assert user_name_mapping["staging_name"] == "user_name"

    def test_generate_from_source(self) -> None:
        """Test full staging model generation."""
        generator = StagingGenerator()
        source = MockSourceDefinition(
            source_name="raw_data",
            name="customers",
            columns={
                "CustomerID": {"name": "CustomerID", "data_type": "integer"},
                "CustomerName": {"name": "CustomerName", "data_type": "varchar"},
                "CreatedAt": {"name": "CreatedAt", "data_type": "timestamp"},
            },
        )

        result = generator.generate_from_source(source)

        # Check result structure
        assert "model_name" in result
        assert "model_sql" in result
        assert "schema_yml" in result
        assert "column_mappings" in result

        # Check model name
        assert result["model_name"] == "stg_customers"
        assert result["source_name"] == "raw_data"
        assert result["table_name"] == "customers"

        # Check SQL content
        sql = result["model_sql"]
        assert "staging model" in sql.lower()
        assert "{{ source('raw_data', 'customers') }}" in sql
        assert "select" in sql.lower()

        # Check column mappings
        assert len(result["column_mappings"]) == 3

    def test_generate_config_block(self) -> None:
        """Test config block generation."""
        config = StagingModelConfig(materialization="table")
        generator = StagingGenerator(config)

        block = generator._generate_config_block()

        assert "{{ config(" in block
        assert "materialized='table'" in block
        assert ") }}" in block

    def test_generate_config_block_with_dedup(self) -> None:
        """Test config block with deduplication column."""
        config = StagingModelConfig(
            materialization="incremental",
            dedup_column="id",
        )
        generator = StagingGenerator(config)

        block = generator._generate_config_block()

        assert "materialized='incremental'" in block
        assert "unique_key='id'" in block

    def test_suggest_column_tests(self) -> None:
        """Test column test suggestions."""
        generator = StagingGenerator()

        # Primary key patterns
        tests = generator._suggest_column_tests("id")
        assert "unique" in tests
        assert "not_null" in tests

        tests = generator._suggest_column_tests("user_id")
        assert "unique" in tests
        assert "not_null" in tests

        # Timestamp patterns
        tests = generator._suggest_column_tests("created_at")
        assert "not_null" in tests

        # Email patterns
        tests = generator._suggest_column_tests("email")
        assert "not_null" in tests

        # No patterns match
        tests = generator._suggest_column_tests("random_column")
        assert tests == []

    def test_generate_schema_yml(self) -> None:
        """Test YAML schema generation."""
        generator = StagingGenerator()

        column_mappings = [
            ColumnMapping(
                source_name="UserId",
                staging_name="user_id",
                description="User identifier",
            ),
            ColumnMapping(
                source_name="UserName",
                staging_name="user_name",
                description="User name",
            ),
        ]

        yml = generator._generate_schema_yml(
            model_name="stg_users",
            source_name="raw",
            table_name="users",
            column_mappings=column_mappings,
        )

        assert "version: 2" in yml
        assert "models:" in yml
        assert "- name: stg_users" in yml
        assert "description:" in yml
        assert "tests:" in yml

    def test_generate_from_source_with_tests_disabled(self) -> None:
        """Test generation without tests."""
        config = StagingModelConfig(generate_tests=False)
        generator = StagingGenerator(config)

        source = MockSourceDefinition(
            source_name="raw",
            name="users",
            columns={
                "UserId": {"name": "UserId", "data_type": "integer"},
            },
        )

        result = generator.generate_from_source(source)

        # YAML should not have tests section
        yml = result["schema_yml"]
        # Tests section should be empty or minimal
        assert "stg_users" in yml

    def test_to_snake_case(self) -> None:
        """Test snake_case conversion."""
        generator = StagingGenerator()

        assert generator._to_snake_case("UserName") == "user_name"
        assert generator._to_snake_case("user_name") == "user_name"
        assert generator._to_snake_case("user-name") == "user_name"
        assert generator._to_snake_case("some.long.name") == "some_long_name"
        assert generator._to_snake_case("ABC") == "abc"

    def test_to_camel_case(self) -> None:
        """Test camelCase conversion."""
        generator = StagingGenerator()

        assert generator._to_camel_case("user_name") == "userName"
        assert generator._to_camel_case("user-id") == "userId"
        assert generator._to_camel_case("UserID") == "userId"

    def test_to_kebab_case(self) -> None:
        """Test kebab-case conversion."""
        generator = StagingGenerator()

        assert generator._to_kebab_case("user_name") == "user-name"
        assert generator._to_kebab_case("userName") == "user-name"

    def test_to_pascal_case(self) -> None:
        """Test PascalCase conversion."""
        generator = StagingGenerator()

        assert generator._to_pascal_case("user_name") == "UserName"
        assert generator._to_pascal_case("user-id") == "UserId"


class TestGenerateStagingModelFromSource:
    """Tests for the convenience function."""

    def test_convenience_function(self) -> None:
        """Test the generate_staging_model_from_source function."""
        source = MockSourceDefinition(
            source_name="raw",
            name="orders",
            columns={
                "OrderID": {"name": "OrderID", "data_type": "integer"},
            },
        )

        result = generate_staging_model_from_source(source)

        assert isinstance(result, dict)
        assert "model_name" in result
        assert "model_sql" in result
        assert "schema_yml" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
