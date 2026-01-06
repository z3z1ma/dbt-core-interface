#!/usr/bin/env python
# pyright: reportPrivateImportUsage=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Documentation completeness checker for dbt models.

Analyzes dbt project models for missing descriptions, undocumented columns,
and incomplete metadata. Generates reports showing documentation coverage
and gaps.
"""

from __future__ import annotations

import dataclasses
import logging
import typing as t
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from rich.console import Console
from rich.table import Table

if t.TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ManifestNode

logger = logging.getLogger(__name__)
console = Console()


class GapSeverity(str, Enum):
    """Severity levels for documentation gaps."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class GapType(str, Enum):
    """Types of documentation gaps."""

    MISSING_MODEL_DESCRIPTION = "missing_model_description"
    MISSING_COLUMN_DESCRIPTION = "missing_column_description"
    MISSING_MODEL_META = "missing_model_meta"
    MISSING_COLUMN_META = "missing_column_meta"
    INCOMPLETE_DESCRIPTION = "incomplete_description"
    NO_SCHEMA_FILE = "no_schema_file"


@dataclass(frozen=True)
class DocumentationGap:
    """Represents a documentation gap found in a model."""

    gap_type: GapType
    model_name: str
    model_path: str
    severity: GapSeverity
    message: str
    column_name: str | None = None
    suggestion: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, t.Any]:
        """Convert gap to dictionary."""
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class ModelDocumentationStatus:
    """Documentation status for a single model."""

    model_name: str
    model_path: str
    has_description: bool
    description_length: int
    total_columns: int
    documented_columns: int
    has_meta: bool
    gaps: list[DocumentationGap]

    @property
    def column_coverage_percentage(self) -> float:
        """Calculate column documentation coverage percentage."""
        if self.total_columns == 0:
            return 100.0
        return (self.documented_columns / self.total_columns) * 100

    def to_dict(self) -> dict[str, t.Any]:
        """Convert status to dictionary."""
        return {
            "model_name": self.model_name,
            "model_path": self.model_path,
            "has_description": self.has_description,
            "description_length": self.description_length,
            "total_columns": self.total_columns,
            "documented_columns": self.documented_columns,
            "column_coverage_percentage": round(self.column_coverage_percentage, 2),
            "has_meta": self.has_meta,
            "gaps": [gap.to_dict() for gap in self.gaps],
        }


@dataclass(frozen=True)
class DocumentationReport:
    """Complete documentation report for the project."""

    project_name: str
    timestamp: str
    total_models: int
    models_with_descriptions: int
    models_without_descriptions: int
    total_columns: int
    documented_columns: int
    undocumented_columns: int
    model_statuses: list[ModelDocumentationStatus]
    all_gaps: list[DocumentationGap]

    @property
    def overall_model_coverage_percentage(self) -> float:
        """Calculate overall model description coverage percentage."""
        if self.total_models == 0:
            return 100.0
        return (self.models_with_descriptions / self.total_models) * 100

    @property
    def overall_column_coverage_percentage(self) -> float:
        """Calculate overall column description coverage percentage."""
        if self.total_columns == 0:
            return 100.0
        return (self.documented_columns / self.total_columns) * 100

    @property
    def critical_gaps(self) -> list[DocumentationGap]:
        """Get all critical severity gaps."""
        return [g for g in self.all_gaps if g.severity == GapSeverity.CRITICAL]

    @property
    def high_gaps(self) -> list[DocumentationGap]:
        """Get all high severity gaps."""
        return [g for g in self.all_gaps if g.severity == GapSeverity.HIGH]

    def to_dict(self) -> dict[str, t.Any]:
        """Convert report to dictionary."""
        return {
            "project_name": self.project_name,
            "timestamp": self.timestamp,
            "summary": {
                "total_models": self.total_models,
                "models_with_descriptions": self.models_with_descriptions,
                "models_without_descriptions": self.models_without_descriptions,
                "model_coverage_percentage": round(self.overall_model_coverage_percentage, 2),
                "total_columns": self.total_columns,
                "documented_columns": self.documented_columns,
                "undocumented_columns": self.undocumented_columns,
                "column_coverage_percentage": round(self.overall_column_coverage_percentage, 2),
                "critical_gap_count": len(self.critical_gaps),
                "high_gap_count": len(self.high_gaps),
                "total_gap_count": len(self.all_gaps),
            },
            "model_statuses": [status.to_dict() for status in self.model_statuses],
            "all_gaps": [gap.to_dict() for gap in self.all_gaps],
        }

    def print_summary(self) -> None:
        """Print a formatted summary of the report to console."""
        # Summary table
        summary_table = Table(title=f"Documentation Coverage Report: {self.project_name}")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="magenta")

        summary_table.add_row("Models analyzed", str(self.total_models))
        summary_table.add_row(
            "Models with descriptions",
            f"{self.models_with_descriptions} ({self.overall_model_coverage_percentage:.1f}%)",
        )
        summary_table.add_row(
            "Models without descriptions",
            f"{self.models_without_descriptions} ({100 - self.overall_model_coverage_percentage:.1f}%)",
        )
        summary_table.add_row("", "")  # spacer
        summary_table.add_row("Total columns", str(self.total_columns))
        summary_table.add_row(
            "Documented columns",
            f"{self.documented_columns} ({self.overall_column_coverage_percentage:.1f}%)",
        )
        summary_table.add_row(
            "Undocumented columns",
            f"{self.undocumented_columns} ({100 - self.overall_column_coverage_percentage:.1f}%)",
        )
        summary_table.add_row("", "")  # spacer
        summary_table.add_row("Critical gaps", str(len(self.critical_gaps)))
        summary_table.add_row("High gaps", str(len(self.high_gaps)))
        summary_table.add_row("Total gaps", str(len(self.all_gaps)))

        console.print(summary_table)

        # Top undocumented models table
        undocumented_models = [
            (s.model_name, s.total_columns - s.documented_columns)
            for s in self.model_statuses
            if s.total_columns > s.documented_columns
        ]
        undocumented_models.sort(key=lambda x: x[1], reverse=True)

        if undocumented_models:
            gaps_table = Table(title="Models with Most Undocumented Columns")
            gaps_table.add_column("Model", style="cyan")
            gaps_table.add_column("Undocumented Columns", style="red")

            for model_name, count in undocumented_models[:10]:
                gaps_table.add_row(model_name, str(count))

            console.print(gaps_table)

    def print_model_details(self, model_name: str | None = None) -> None:
        """Print detailed information for a specific model or all models."""
        statuses = self.model_statuses
        if model_name:
            statuses = [s for s in statuses if s.model_name == model_name]
            if not statuses:
                console.print(f"[yellow]Model '{model_name}' not found in report.[/yellow]")
                return

        for status in statuses:
            model_table = Table(title=f"Model: {status.model_name}")
            model_table.add_column("Property", style="cyan")
            model_table.add_column("Value", style="white")

            model_table.add_row("Path", status.model_path)
            model_table.add_row(
                "Has description", "[green]Yes[/green]" if status.has_description else "[red]No[/red]"
            )
            if status.has_description:
                model_table.add_row("Description length", str(status.description_length))
            model_table.add_row(
                "Column coverage",
                f"{status.documented_columns}/{status.total_columns} ({status.column_coverage_percentage:.1f}%)",
            )
            model_table.add_row(
                "Has meta", "[green]Yes[/green]" if status.has_meta else "[red]No[/red]"
            )

            console.print(model_table)

            if status.gaps:
                gaps_table = Table(title="Documentation Gaps")
                gaps_table.add_column("Severity", style="red")
                gaps_table.add_column("Type", style="yellow")
                gaps_table.add_column("Message", style="white")

                for gap in status.gaps:
                    severity_color = {
                        GapSeverity.CRITICAL: "red",
                        GapSeverity.HIGH: "red",
                        GapSeverity.MEDIUM: "yellow",
                        GapSeverity.LOW: "blue",
                    }.get(gap.severity, "white")

                    gaps_table.add_row(
                        f"[{severity_color}]{gap.severity.value}[/{severity_color}]",
                        gap.gap_type.value,
                        gap.message,
                    )

                console.print(gaps_table)


class DocumentationChecker:
    """Checker for analyzing documentation completeness in dbt projects."""

    # Minimum description lengths to be considered "complete"
    MIN_MODEL_DESCRIPTION_LENGTH = 10
    MIN_COLUMN_DESCRIPTION_LENGTH = 5

    def __init__(
        self,
        min_model_description_length: int = MIN_MODEL_DESCRIPTION_LENGTH,
        min_column_description_length: int = MIN_COLUMN_DESCRIPTION_LENGTH,
    ) -> None:
        """Initialize the documentation checker.

        Args:
            min_model_description_length: Minimum characters for model description
            min_column_description_length: Minimum characters for column description

        """
        self.min_model_description_length = min_model_description_length
        self.min_column_description_length = min_column_description_length

    def check_model(self, model: ManifestNode) -> ModelDocumentationStatus:
        """Check documentation completeness for a single model.

        Args:
            model: The manifest node to check

        Returns:
            ModelDocumentationStatus with gaps and coverage info

        """
        gaps: list[DocumentationGap] = []

        # Check model description
        has_description = bool(model.description)
        description_length = len(model.description) if model.description else 0

        if not has_description:
            gaps.append(
                DocumentationGap(
                    gap_type=GapType.MISSING_MODEL_DESCRIPTION,
                    model_name=model.name,
                    model_path=model.original_file_path,
                    severity=GapSeverity.CRITICAL,
                    message="Model has no description",
                    suggestion=f"Add a description for model '{model.name}' explaining its purpose and contents",
                )
            )
        elif description_length < self.min_model_description_length:
            gaps.append(
                DocumentationGap(
                    gap_type=GapType.INCOMPLETE_DESCRIPTION,
                    model_name=model.name,
                    model_path=model.original_file_path,
                    severity=GapSeverity.MEDIUM,
                    message=f"Model description is too short ({description_length} chars)",
                    suggestion=f"Expand the description to at least {self.min_model_description_length} characters",
                )
            )

        # Check for meta key
        has_meta = bool(hasattr(model, "meta") and model.meta)

        if not has_meta:
            gaps.append(
                DocumentationGap(
                    gap_type=GapType.MISSING_MODEL_META,
                    model_name=model.name,
                    model_path=model.original_file_path,
                    severity=GapSeverity.LOW,
                    message="Model has no metadata",
                    suggestion="Consider adding metadata like owner, team, or tags",
                )
            )

        # Check columns
        total_columns = 0
        documented_columns = 0

        if hasattr(model, "columns"):
            for col_name, col_info in model.columns.items():
                total_columns += 1
                col_desc = col_info.get("description", "") if isinstance(col_info, dict) else ""

                if col_desc and len(col_desc) >= self.min_column_description_length:
                    documented_columns += 1
                else:
                    # Check if column is documented but too short
                    if col_desc and len(col_desc) < self.min_column_description_length:
                        gaps.append(
                            DocumentationGap(
                                gap_type=GapType.INCOMPLETE_DESCRIPTION,
                                model_name=model.name,
                                model_path=model.original_file_path,
                                severity=GapSeverity.LOW,
                                column_name=col_name,
                                message=f"Column '{col_name}' description is too short ({len(col_desc)} chars)",
                                suggestion=f"Expand column description to at least {self.min_column_description_length} characters",
                            )
                        )
                        documented_columns += 1  # Still counts as documented
                    else:
                        gaps.append(
                            DocumentationGap(
                                gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
                                model_name=model.name,
                                model_path=model.original_file_path,
                                severity=GapSeverity.HIGH,
                                column_name=col_name,
                                message=f"Column '{col_name}' has no description",
                                suggestion=f"Add description for column '{col_name}' explaining its purpose",
                            )
                        )

                # Check column meta
                col_meta = col_info.get("meta") if isinstance(col_info, dict) else None
                if not col_meta:
                    gaps.append(
                        DocumentationGap(
                            gap_type=GapType.MISSING_COLUMN_META,
                            model_name=model.name,
                            model_path=model.original_file_path,
                            severity=GapSeverity.LOW,
                            column_name=col_name,
                            message=f"Column '{col_name}' has no metadata",
                            suggestion="Consider adding metadata like data_type, format, or pii flags",
                        )
                    )

        return ModelDocumentationStatus(
            model_name=model.name,
            model_path=model.original_file_path,
            has_description=has_description,
            description_length=description_length,
            total_columns=total_columns,
            documented_columns=documented_columns,
            has_meta=has_meta,
            gaps=gaps,
        )

    def check_project(
        self,
        manifest: t.Any,
        project_name: str,
        model_name_filter: str | None = None,
    ) -> DocumentationReport:
        """Check documentation completeness for all models in the project.

        Args:
            manifest: The dbt manifest
            project_name: Name of the project
            model_name_filter: Optional filter to check only specific model

        Returns:
            DocumentationReport with complete coverage analysis

        """
        model_statuses: list[ModelDocumentationStatus] = []
        all_gaps: list[DocumentationGap] = []

        models_to_check = []
        for node in manifest.nodes.values():
            if node.resource_type == "model":
                if model_name_filter is None or node.name == model_name_filter:
                    models_to_check.append(node)

        for model in models_to_check:
            status = self.check_model(model)
            model_statuses.append(status)
            all_gaps.extend(status.gaps)

        # Calculate summary statistics
        total_models = len(model_statuses)
        models_with_descriptions = sum(1 for s in model_statuses if s.has_description)
        models_without_descriptions = total_models - models_with_descriptions

        total_columns = sum(s.total_columns for s in model_statuses)
        documented_columns = sum(s.documented_columns for s in model_statuses)
        undocumented_columns = total_columns - documented_columns

        return DocumentationReport(
            project_name=project_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            total_models=total_models,
            models_with_descriptions=models_with_descriptions,
            models_without_descriptions=models_without_descriptions,
            total_columns=total_columns,
            documented_columns=documented_columns,
            undocumented_columns=undocumented_columns,
            model_statuses=model_statuses,
            all_gaps=all_gaps,
        )

    def get_gaps_by_severity(
        self, report: DocumentationReport, severity: GapSeverity
    ) -> list[DocumentationGap]:
        """Get all gaps of a specific severity level."""
        return [g for g in report.all_gaps if g.severity == severity]

    def get_gaps_by_type(
        self, report: DocumentationReport, gap_type: GapType
    ) -> list[DocumentationGap]:
        """Get all gaps of a specific type."""
        return [g for g in report.all_gaps if g.gap_type == gap_type]

    def get_gaps_by_model(self, report: DocumentationReport, model_name: str) -> list[DocumentationGap]:
        """Get all gaps for a specific model."""
        return [g for g in report.all_gaps if g.model_name == model_name]


__all__ = [
    "GapSeverity",
    "GapType",
    "DocumentationGap",
    "ModelDocumentationStatus",
    "DocumentationReport",
    "DocumentationChecker",
]
