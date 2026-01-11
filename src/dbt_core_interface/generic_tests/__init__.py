#!/usr/bin/env python
# pyright: reportPrivateImportUsage=false
"""Generic dbt test library - reusable and customizable data quality tests.

This module provides a comprehensive library of generic dbt tests that can be
easily configured via YAML schema files. These tests cover common data quality
scenarios like null checks, uniqueness, relationships, and more.

Example usage in schema.yml:
```yaml
version: 2

models:
  - name: users
    columns:
      - name: id
        tests:
          - unique
          - not_null

      - name: email
        tests:
          - not_null:
              severity: warn

      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
```

"""

from __future__ import annotations

import logging
import typing as t
from dataclasses import dataclass, field
from enum import Enum

if t.TYPE_CHECKING:
    pass

from dbt_core_interface.project import DbtProject

logger = logging.getLogger(__name__)


class GenericTestType(Enum):
    """Types of generic dbt tests."""

    UNIQUE = "unique"
    NOT_NULL = "not_null"
    RELATIONSHIPS = "relationships"
    ACCEPTED_VALUES = "accepted_values"
    RELATIONSHIP_WHERE = "relationship_where"
    VALUES_AT = "values_at"
    WHERE = "where"
    EQUALITY = "equality"
    RECENCY = "recency"
    CARDINALITY_EQUALS = "cardinality_equals"
    MUTUALLY_EXCLUSIVE_RANGES = "mutually_exclusive_ranges"
    NOT_CONSTANT = "not_constant"
    RELATIONSHIPS_WHERE = "relationships_where"
    AT_LEAST_ONE = "at_least_one"
    GROUP_BY = "group_by"
    CUSTOM_SQL = "custom_sql"


class TestSeverity(str, Enum):
    """Severity levels for test failures."""

    ERROR = "error"
    WARN = "warn"


@dataclass(frozen=True)
class GenericTestConfig:
    """Configuration for a generic test."""

    test_type: GenericTestType
    column_name: str | None = None
    severity: TestSeverity = TestSeverity.ERROR
    config: dict[str, t.Any] = field(default_factory=dict)
    enabled: bool = True

    def to_yaml(self, model_name: str, indent: int = 4) -> str:
        """Convert test configuration to YAML format."""
        indent_str = " " * indent
        test_indent = " " * (indent + 2)

        if self.column_name:
            lines = [f"{indent_str}- {self.test_type.value}:"]
            lines.append(f"{test_indent}{model_name}.{self.column_name}")
        else:
            lines = [f"{indent_str}- {self.test_type.value}:"]

        # Add configuration options
        if self.severity != TestSeverity.ERROR:
            lines.append(f"{test_indent}severity: {self.severity.value}")

        for key, value in self.config.items():
            if isinstance(value, str):
                lines.append(f"{test_indent}{key}: '{value}'")
            elif isinstance(value, list):
                formatted_list = ", ".join(f"'{v}'" for v in value)
                lines.append(f"{test_indent}{key}: [{formatted_list}]")
            elif isinstance(value, bool):
                lines.append(f"{test_indent}{key}: {str(value).lower()}")
            else:
                lines.append(f"{test_indent}{key}: {value}")

        return "\n".join(lines)


@dataclass
class GenericTestDefinition:
    """Definition of a generic test with SQL template."""

    name: str
    description: str
    test_type: GenericTestType
    sql_template: str
    required_args: list[str] = field(default_factory=list)
    optional_args: dict[str, t.Any] = field(default_factory=dict)
    examples: list[dict[str, t.Any]] = field(default_factory=list)

    def render_config(self, **kwargs: t.Any) -> GenericTestConfig:
        """Render a test configuration from this definition."""
        config = GenericTestConfig(
            test_type=self.test_type,
            config={k: v for k, v in kwargs.items() if v is not None},
        )
        return config


class GenericTestLibrary:
    """Library of reusable generic dbt tests."""

    def __init__(self, project: DbtProject) -> None:
        """Initialize the generic test library.

        Args:
            project: The dbt project instance

        """
        self.project = project
        self._definitions: dict[str, GenericTestDefinition] = {}
        self._register_default_tests()

    def _register_default_tests(self) -> None:
        """Register all default generic tests."""
        self.register(
            GenericTestDefinition(
                name="unique",
                description="Ensures that a column has unique values (no duplicates).",
                test_type=GenericTestType.UNIQUE,
                sql_template="""
                    {% set column_name = kwargs.get('column_name') %}
                    {% set severity = kwargs.get('severity', 'error') %}

                    SELECT
                        {{ column_name }},
                        COUNT(*) AS count
                    FROM {{ this }}
                    GROUP BY {{ column_name }}
                    HAVING COUNT(*) > 1
                """,
                required_args=["column_name"],
                examples=[
                    {
                        "description": "Check that user IDs are unique",
                        "config": {"column_name": "id"},
                    }
                ],
            )
        )

        self.register(
            GenericTestDefinition(
                name="not_null",
                description="Ensures that a column has no null values.",
                test_type=GenericTestType.NOT_NULL,
                sql_template="""
                    {% set column_name = kwargs.get('column_name') %}

                    SELECT *
                    FROM {{ this }}
                    WHERE {{ column_name }} IS NULL
                """,
                required_args=["column_name"],
                examples=[
                    {
                        "description": "Check that email addresses are never null",
                        "config": {"column_name": "email"},
                    }
                ],
            )
        )

        self.register(
            GenericTestDefinition(
                name="relationships",
                description="Ensures that a column's values exist as a primary key in another table.",
                test_type=GenericTestType.RELATIONSHIPS,
                sql_template="""
                    {% set column_name = kwargs.get('column_name') %}
                    {% set to = kwargs.get('to', 'ref("' + kwargs.get('to_table') + '")') %}
                    {% set from_field = kwargs.get('field', column_name) %}
                    {% set to_field = kwargs.get('to_field', from_field) %}

                    SELECT
                        {{ from_field }}
                    FROM {{ this }}
                    WHERE {{ from_field }} IS NOT NULL
                      AND {{ from_field }} NOT IN (
                          SELECT {{ to_field }}
                          FROM {{ to }}
                      )
                """,
                required_args=["column_name", "to"],
                optional_args={
                    "field": "Column name in the from table (default: same as column_name)",
                    "to_field": "Column name in the to table (default: same as field)",
                },
                examples=[
                    {
                        "description": "Ensure order customer_id references users.id",
                        "config": {
                            "column_name": "customer_id",
                            "to": "ref('users')",
                            "to_field": "id",
                        },
                    }
                ],
            )
        )

        self.register(
            GenericTestDefinition(
                name="accepted_values",
                description="Ensures that a column only contains values from a specified list.",
                test_type=GenericTestType.ACCEPTED_VALUES,
                sql_template="""
                    {% set column_name = kwargs.get('column_name') %}
                    {% set values = kwargs.get('values', []) %}
                    {% set quote = kwargs.get('quote', True) %}

                    SELECT *
                    FROM {{ this }}
                    WHERE {{ column_name }} NOT IN (
                        {% for value in values %}
                            {% if quote %}'{{ value }}'{% else %}{{ value }}{% endif %}
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                """,
                required_args=["column_name", "values"],
                optional_args={"quote": "Whether to quote values (default: true)"},
                examples=[
                    {
                        "description": "Ensure status only contains specific values",
                        "config": {
                            "column_name": "status",
                            "values": ["active", "inactive", "pending"],
                        },
                    }
                ],
            )
        )

        self.register(
            GenericTestDefinition(
                name="recency",
                description="Checks that data has been updated within a specified time window.",
                test_type=GenericTestType.RECENCY,
                sql_template="""
                    {% set column_name = kwargs.get('column_name', 'updated_at') %}
                    {% set interval = kwargs.get('interval', '24 hour') %}
                    {% set time_column = kwargs.get('datepart', 'timestamp') %}

                    SELECT COUNT(*) as stale_count
                    FROM {{ this }}
                    WHERE {{ column_name }} < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ interval }})
                """,
                required_args=["column_name"],
                optional_args={"interval": "Time window for recency check (default: '24 hour')"},
                examples=[
                    {
                        "description": "Ensure orders have been updated within 24 hours",
                        "config": {"column_name": "updated_at", "interval": "24 hour"},
                    }
                ],
            )
        )

        self.register(
            GenericTestDefinition(
                name="cardinality_equals",
                description="Ensures that the cardinality (distinct count) of a column matches another table.",
                test_type=GenericTestType.CARDINALITY_EQUALS,
                sql_template="""
                    {% set column_name = kwargs.get('column_name') %}
                    {% set to = kwargs.get('to') %}
                    {% set to_field = kwargs.get('to_field', column_name) %}

                    WITH this_count AS (
                        SELECT COUNT(DISTINCT {{ column_name }}) AS count
                        FROM {{ this }}
                    ),
                    that_count AS (
                        SELECT COUNT(DISTINCT {{ to_field }}) AS count
                        FROM {{ to }}
                    )
                    SELECT
                        this_count.count AS this_count,
                        that_count.count AS that_count
                    FROM this_count, that_count
                    HAVING this_count.count != that_count.count
                """,
                required_args=["column_name", "to"],
                optional_args={
                    "to_field": "Column name in the to table (default: same as column_name)"
                },
                examples=[
                    {
                        "description": "Ensure distinct customer count matches source",
                        "config": {
                            "column_name": "customer_id",
                            "to": "ref('stg_customers')",
                        },
                    }
                ],
            )
        )

    def register(self, definition: GenericTestDefinition) -> None:
        """Register a new generic test definition.

        Args:
            definition: The test definition to register

        """
        self._definitions[definition.name] = definition
        logger.debug(f"Registered generic test: {definition.name}")

    def get(self, test_name: str) -> GenericTestDefinition | None:
        """Get a test definition by name.

        Args:
            test_name: The name of the test

        Returns:
            The test definition or None if not found

        """
        return self._definitions.get(test_name)

    def list_tests(self) -> list[GenericTestDefinition]:
        """List all registered test definitions.

        Returns:
            List of all test definitions

        """
        return list(self._definitions.values())

    def suggest_tests_for_column(
        self, column_name: str, column_info: dict[str, t.Any] | None = None
    ) -> list[GenericTestConfig]:
        """Suggest appropriate tests for a column based on its name and metadata.

        Args:
            column_name: The name of the column
            column_info: Optional metadata about the column

        Returns:
            List of suggested test configurations

        """
        suggestions: list[GenericTestConfig] = []
        col_lower = column_name.lower()

        # Primary key patterns
        if (
            col_lower in ("id", "uuid", "guid", "pk")
            or col_lower.endswith("_id")
            and col_lower != "id"
        ):
            if col_lower in ("id", "uuid", "guid") or col_lower.endswith("_pk"):
                suggestions.append(
                    GenericTestConfig(
                        test_type=GenericTestType.UNIQUE,
                        column_name=column_name,
                    )
                )
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.NOT_NULL,
                    column_name=column_name,
                )
            )

        # Foreign key patterns
        elif any(col_lower.endswith(suffix) for suffix in ("_id", "_fk", "_uuid", "_guid")):
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.NOT_NULL,
                    column_name=column_name,
                )
            )

        # Email patterns
        elif "email" in col_lower:
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.NOT_NULL,
                    column_name=column_name,
                )
            )

        # Status/state patterns
        elif any(keyword in col_lower for keyword in ("status", "state", "type", "category")):
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.ACCEPTED_VALUES,
                    column_name=column_name,
                    config={"values": []},  # User should populate
                )
            )

        # Timestamp patterns
        elif any(keyword in col_lower for keyword in ("created_at", "updated_at", "deleted_at")):
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.NOT_NULL,
                    column_name=column_name,
                )
            )
            suggestions.append(
                GenericTestConfig(
                    test_type=GenericTestType.RECENCY,
                    column_name=column_name,
                    config={"interval": "24 hour"},
                )
            )

        return suggestions

    def generate_schema_yml(self, model_name: str, columns: dict[str, dict[str, t.Any]]) -> str:
        """Generate a schema.yml file with suggested tests for a model.

        Args:
            model_name: The name of the model
            columns: Dictionary of column_name -> column_info

        Returns:
            YAML string with test definitions

        """
        lines = ["version: 2", "", "models:", f"  - name: {model_name}", "    columns:"]

        for col_name, col_info in columns.items():
            description = col_info.get("description", "")
            lines.append(f"      - name: {col_name}")
            if description:
                lines.append(f"        description: {description}")

            # Get suggested tests
            suggestions = self.suggest_tests_for_column(col_name, col_info)

            if suggestions:
                lines.append("        tests:")
                for suggestion in suggestions:
                    lines.append(suggestion.to_yaml(model_name, indent=10))

        return "\n".join(lines)


__all__ = [
    "GenericTestType",
    "GenericTestConfig",
    "GenericTestDefinition",
    "GenericTestLibrary",
    "TestSeverity",
]
