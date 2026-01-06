#!/usr/bin/env python
# pyright: reportPrivateImportUsage=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""AI-powered test suggestion and generation for dbt models.

Analyzes model patterns and automatically suggests appropriate tests (unique, not_null,
relationships, expressions). Learns from project patterns to suggest tests that match
team conventions.
"""

from __future__ import annotations

import logging
import re
import typing as t
from dataclasses import dataclass, field
from enum import Enum

if t.TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ManifestNode

logger = logging.getLogger(__name__)


class TestType(Enum):
    """Types of dbt tests."""

    UNIQUE = "unique"
    NOT_NULL = "not_null"
    RELATIONSHIPS = "relationships"
    ACCEPTED_VALUES = "accepted_values"
    RELATIONSHIP_WHERE = "relationship_where"


@dataclass(frozen=True)
class ColumnPattern:
    """A pattern that matches certain column names and suggests tests."""

    name: str
    regex: str
    description: str
    suggested_tests: list[TestType] = field(default_factory=list)
    test_params: dict[str, t.Any] = field(default_factory=dict)

    def matches(self, column_name: str) -> re.Match[str] | None:
        """Check if this pattern matches the column name."""
        return re.match(self.regex, column_name, re.IGNORECASE)


@dataclass(frozen=True)
class TestSuggestion:
    """A suggested test for a model."""

    test_type: TestType
    column_name: str | None = None
    reason: str = ""
    config: dict[str, t.Any] = field(default_factory=dict)

    def to_yaml(self, model_name: str, indent: int = 2) -> str:
        """Convert suggestion to dbt test YAML format."""
        indent_str = " " * indent
        inner_indent = " " * (indent + 2)
        if self.column_name:
            lines = [f"{indent_str}- {self.test_type.value}:"]
            lines.append(f"{inner_indent}{model_name}.{self.column_name}")
            for key, value in self.config.items():
                lines.append(f"{inner_indent}{key}: {value}")
        else:
            lines = [f"{indent_str}- {self.test_type.value}:"]
            for key, value in self.config.items():
                lines.append(f"{inner_indent}{key}: {value}")
        return "\n".join(lines)


# Default patterns for detecting column types
DEFAULT_PATTERNS: list[ColumnPattern] = [
    ColumnPattern(
        name="primary_key",
        regex=r"^(?:.*_)?(?:id|uuid|guid|pk)$",
        description="Primary key column",
        suggested_tests=[TestType.UNIQUE, TestType.NOT_NULL],
    ),
    ColumnPattern(
        name="foreign_key",
        regex=r"^(?:.*_)?(?:.*_id|.*_uuid|.*_guid|.*_fk)$",
        description="Foreign key column",
        suggested_tests=[TestType.NOT_NULL, TestType.RELATIONSHIPS],
    ),
    ColumnPattern(
        name="timestamp",
        regex=r"^(?:.*_)?(?:created_at|updated_at|deleted_at|timestamp|date|time)$",
        description="Timestamp column",
        suggested_tests=[TestType.NOT_NULL],
    ),
    ColumnPattern(
        name="email",
        regex=r"^(?:.*_)?(?:email|email_address)$",
        description="Email column",
        suggested_tests=[TestType.NOT_NULL],
    ),
    ColumnPattern(
        name="status",
        regex=r"^(?:.*_)?(?:status|state|type|category)$",
        description="Status/state column",
        suggested_tests=[TestType.ACCEPTED_VALUES],
    ),
    ColumnPattern(
        name="required_field",
        regex=r"^(?:.*_)?(?:name|title|description|username|user_name|first_name|last_name)$",
        description="Commonly required field",
        suggested_tests=[TestType.NOT_NULL],
    ),
]


@dataclass
class ProjectTestPatterns:
    """Learned test patterns from the project."""

    column_tests: dict[str, list[TestType]] = field(default_factory=dict)
    test_configs: dict[str, dict[str, t.Any]] = field(default_factory=dict)

    def add_column_pattern(self, column_name: str, test_types: list[TestType]) -> None:
        """Add a learned column pattern."""
        normalized = column_name.lower()
        if normalized not in self.column_tests:
            self.column_tests[normalized] = []
        self.column_tests[normalized].extend(test_types)
        # Deduplicate
        self.column_tests[normalized] = list(set(self.column_tests[normalized]))

    def get_tests_for_column(self, column_name: str) -> list[TestType]:
        """Get learned tests for a column name."""
        # Try exact match first
        normalized = column_name.lower()
        if normalized in self.column_tests:
            return self.column_tests[normalized]

        # Try suffix matching (e.g., "_id" suffixes)
        for pattern, tests in self.column_tests.items():
            if pattern.startswith("_") and column_name.lower().endswith(pattern):
                return tests

        return []

    def learn_from_manifest(self, manifest: t.Any) -> None:
        """Learn test patterns from existing tests in the manifest."""
        for node in manifest.nodes.values():
            if node.resource_type == "test":
                self._learn_from_test_node(node)

    def _learn_from_test_node(self, node: ManifestNode) -> None:
        """Learn from a single test node."""
        test_name = node.name
        test_params = {}

        # Extract test parameters from the node
        if hasattr(node, "test_metadata"):
            metadata = node.test_metadata  # pyright: ignore[reportAttributeAccessIssue]
            if metadata:
                kwargs = metadata.kwargs or {}
                column_name = kwargs.get("column_name")
                test_params = {k: v for k, v in kwargs.items() if k != "column_name"}

        # Determine test type
        test_type = self._parse_test_type(test_name)
        if test_type and column_name:
            self.add_column_pattern(column_name, [test_type])
            if test_params:
                key = f"{column_name}_{test_type.value}"
                self.test_configs[key] = test_params

    def _parse_test_type(self, test_name: str) -> TestType | None:
        """Parse test type from test name."""
        for test_type in TestType:
            if test_type.value in test_name.lower():
                return test_type
        return None


class TestSuggester:
    """AI-powered test suggestion engine for dbt models."""

    def __init__(
        self,
        custom_patterns: list[ColumnPattern] | None = None,
        learned_patterns: ProjectTestPatterns | None = None,
    ) -> None:
        """Initialize the test suggester.

        Args:
            custom_patterns: Additional patterns to use for detection
            learned_patterns: Previously learned patterns from the project

        """
        self.patterns = DEFAULT_PATTERNS + (custom_patterns or [])
        self.learned = learned_patterns or ProjectTestPatterns()

    def suggest_tests_for_model(
        self, model: ManifestNode, manifest: t.Any | None = None
    ) -> list[TestSuggestion]:
        """Suggest tests for a dbt model.

        Args:
            model: The model node to analyze
            manifest: The full manifest (for relationship resolution)

        Returns:
            List of test suggestions

        """
        suggestions: list[TestSuggestion] = []

        # Get model columns
        columns = self._extract_columns(model)
        logger.debug(f"Analyzing {len(columns)} columns for model {model.name}")

        for column_name, _column_info in columns.items():
            # Check learned patterns first
            learned_tests = self.learned.get_tests_for_column(column_name)
            if learned_tests:
                for test_type in learned_tests:
                    key = f"{column_name}_{test_type.value}"
                    config = self.learned.test_configs.get(key, {})
                    suggestions.append(
                        TestSuggestion(
                            test_type=test_type,
                            column_name=column_name,
                            reason="Learned from existing project tests",
                            config=config,
                        )
                    )
                continue

            # Apply default patterns
            for pattern in self.patterns:
                match = pattern.matches(column_name)
                if match:
                    for test_type in pattern.suggested_tests:
                        suggestions.append(
                            TestSuggestion(
                                test_type=test_type,
                                column_name=column_name,
                                reason=pattern.description,
                            )
                        )
                    break

        # Add model-level suggestions
        suggestions.extend(self._suggest_model_level_tests(model, manifest))

        return suggestions

    def _extract_columns(self, model: ManifestNode) -> dict[str, t.Any]:
        """Extract column information from a model."""
        columns: dict[str, t.Any] = {}

        if hasattr(model, "columns"):
            for col_name, col_info in model.columns.items():
                columns[col_name] = col_info

        return columns

    def _suggest_model_level_tests(
        self, model: ManifestNode, manifest: t.Any | None = None
    ) -> list[TestSuggestion]:
        """Suggest model-level tests."""
        suggestions: list[TestSuggestion] = []

        # Suggest tests for models that look like fact/dimension tables
        if self._looks_like_fact_table(model):
            suggestions.append(
                TestSuggestion(
                    test_type=TestType.RELATIONSHIPS,
                    reason="Fact table should have foreign key relationships",
                )
            )

        return suggestions

    def _looks_like_fact_table(self, model: ManifestNode) -> bool:
        """Check if model looks like a fact table."""
        # Fact tables typically have many _id columns (foreign keys)
        # Exclude pure "id" which is likely a primary key, not a foreign key
        fk_id_count = 0
        total_count = 0

        if hasattr(model, "columns"):
            for col_name in model.columns.keys():
                total_count += 1
                # Count columns ending in _id but not exactly "id"
                if col_name.endswith("_id") and col_name != "id":
                    fk_id_count += 1

        # Heuristic: more than 20% of columns are foreign key IDs
        return total_count > 0 and (fk_id_count / total_count) > 0.2

    def generate_test_yml(
        self, model: ManifestNode, manifest: t.Any | None = None
    ) -> str:
        """Generate YAML schema file with suggested tests.

        Args:
            model: The model node
            manifest: The full manifest

        Returns:
            YAML string with test definitions

        """
        suggestions = self.suggest_tests_for_model(model, manifest)

        lines = [
            "version: 2",
            "models:",
            f"  - name: {model.name}",
            f"    description: {model.description or 'Add description'}",
            "    columns:",
        ]

        # Group suggestions by column
        by_column: dict[str, list[TestSuggestion]] = {}
        model_level: list[TestSuggestion] = []

        for suggestion in suggestions:
            if suggestion.column_name:
                if suggestion.column_name not in by_column:
                    by_column[suggestion.column_name] = []
                by_column[suggestion.column_name].append(suggestion)
            else:
                model_level.append(suggestion)

        # Generate column-level tests
        for col_name, col_suggestions in by_column.items():
            col_info = ""
            if hasattr(model, "columns") and col_name in model.columns:
                col_info_obj = model.columns[col_name]
                col_info = f" description: {col_info_obj.get('description', '')}"

            lines.append(f"      - name: {col_name}{col_info}")
            lines.append("        tests:")

            for suggestion in col_suggestions:
                test_line = f"          - {suggestion.test_type.value}"
                if suggestion.config:
                    # Add config parameters
                    config_str = ", ".join(f"{k}={v}" for k, v in suggestion.config.items())
                    test_line += f": {{{config_str}}}"
                lines.append(test_line)

        # Generate model-level tests
        if model_level:
            lines.append("    tests:")
            for suggestion in model_level:
                lines.append(f"      - {suggestion.test_type.value}")

        return "\n".join(lines)

    def learn_from_project(self, manifest: t.Any) -> None:
        """Learn test patterns from the existing project.

        Args:
            manifest: The dbt manifest containing existing tests

        """
        self.learned.learn_from_manifest(manifest)
        logger.info(f"Learned patterns from {len(self.learned.column_tests)} columns")


__all__ = [
    "TestType",
    "ColumnPattern",
    "TestSuggestion",
    "ProjectTestPatterns",
    "TestSuggester",
    "DEFAULT_PATTERNS",
]
