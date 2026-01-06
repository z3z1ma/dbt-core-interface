#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Tests for the documentation checker module."""

from unittest.mock import MagicMock

import pytest

from dbt_core_interface.doc_checker import (
    DocumentationChecker,
    DocumentationGap,
    DocumentationReport,
    GapSeverity,
    GapType,
    ModelDocumentationStatus,
)


def test_documentation_gap_creation() -> None:
    """Test DocumentationGap dataclass creation."""
    gap = DocumentationGap(
        gap_type=GapType.MISSING_MODEL_DESCRIPTION,
        model_name="test_model",
        model_path="models/test_model.sql",
        severity=GapSeverity.CRITICAL,
        message="Model has no description",
        suggestion="Add a description",
    )

    assert gap.gap_type == GapType.MISSING_MODEL_DESCRIPTION
    assert gap.model_name == "test_model"
    assert gap.severity == GapSeverity.CRITICAL
    assert gap.column_name is None


def test_documentation_gap_to_dict() -> None:
    """Test DocumentationGap serialization."""
    gap = DocumentationGap(
        gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
        model_name="test_model",
        model_path="models/test_model.sql",
        severity=GapSeverity.HIGH,
        message="Column 'user_id' has no description",
        column_name="user_id",
        suggestion="Add column description",
    )

    gap_dict = gap.to_dict()
    assert gap_dict["gap_type"] == "missing_column_description"
    assert gap_dict["model_name"] == "test_model"
    assert gap_dict["column_name"] == "user_id"


def test_model_documentation_status() -> None:
    """Test ModelDocumentationStatus calculation."""
    status = ModelDocumentationStatus(
        model_name="test_model",
        model_path="models/test_model.sql",
        has_description=True,
        description_length=50,
        total_columns=10,
        documented_columns=7,
        has_meta=True,
        gaps=[],
    )

    assert status.model_name == "test_model"
    assert status.has_description is True
    assert status.column_coverage_percentage == 70.0


def test_model_documentation_status_no_columns() -> None:
    """Test ModelDocumentationStatus with zero columns."""
    status = ModelDocumentationStatus(
        model_name="empty_model",
        model_path="models/empty.sql",
        has_description=True,
        description_length=20,
        total_columns=0,
        documented_columns=0,
        has_meta=False,
        gaps=[],
    )

    # Should be 100% when there are no columns
    assert status.column_coverage_percentage == 100.0


def test_model_documentation_status_to_dict() -> None:
    """Test ModelDocumentationStatus serialization."""
    status = ModelDocumentationStatus(
        model_name="test_model",
        model_path="models/test.sql",
        has_description=True,
        description_length=30,
        total_columns=5,
        documented_columns=3,
        has_meta=False,
        gaps=[],
    )

    status_dict = status.to_dict()
    assert status_dict["model_name"] == "test_model"
    assert status_dict["column_coverage_percentage"] == 60.0


def test_documentation_report_summary() -> None:
    """Test DocumentationReport summary calculations."""
    report = DocumentationReport(
        project_name="test_project",
        timestamp="2024-01-01T00:00:00Z",
        total_models=10,
        models_with_descriptions=7,
        models_without_descriptions=3,
        total_columns=100,
        documented_columns=80,
        undocumented_columns=20,
        model_statuses=[],
        all_gaps=[],
    )

    assert report.project_name == "test_project"
    assert report.overall_model_coverage_percentage == 70.0
    assert report.overall_column_coverage_percentage == 80.0
    assert len(report.critical_gaps) == 0
    assert len(report.high_gaps) == 0


def test_documentation_report_to_dict() -> None:
    """Test DocumentationReport serialization."""
    gap = DocumentationGap(
        gap_type=GapType.MISSING_MODEL_DESCRIPTION,
        model_name="test",
        model_path="models/test.sql",
        severity=GapSeverity.CRITICAL,
        message="No description",
    )

    report = DocumentationReport(
        project_name="test_project",
        timestamp="2024-01-01T00:00:00Z",
        total_models=1,
        models_with_descriptions=0,
        models_without_descriptions=1,
        total_columns=0,
        documented_columns=0,
        undocumented_columns=0,
        model_statuses=[],
        all_gaps=[gap],
    )

    report_dict = report.to_dict()
    assert report_dict["summary"]["total_models"] == 1
    assert report_dict["summary"]["model_coverage_percentage"] == 0.0
    assert report_dict["summary"]["critical_gap_count"] == 1
    assert len(report_dict["all_gaps"]) == 1


def test_gap_severity_enum() -> None:
    """Test GapSeverity enum values."""
    assert GapSeverity.CRITICAL.value == "critical"
    assert GapSeverity.HIGH.value == "high"
    assert GapSeverity.MEDIUM.value == "medium"
    assert GapSeverity.LOW.value == "low"


def test_gap_type_enum() -> None:
    """Test GapType enum values."""
    assert GapType.MISSING_MODEL_DESCRIPTION.value == "missing_model_description"
    assert GapType.MISSING_COLUMN_DESCRIPTION.value == "missing_column_description"
    assert GapType.MISSING_MODEL_META.value == "missing_model_meta"
    assert GapType.INCOMPLETE_DESCRIPTION.value == "incomplete_description"


def test_documentation_checker_initialization() -> None:
    """Test DocumentationChecker initialization with defaults."""
    checker = DocumentationChecker()
    assert checker.min_model_description_length == 10
    assert checker.min_column_description_length == 5


def test_documentation_checker_custom_thresholds() -> None:
    """Test DocumentationChecker with custom thresholds."""
    checker = DocumentationChecker(
        min_model_description_length=50,
        min_column_description_length=20,
    )
    assert checker.min_model_description_length == 50
    assert checker.min_column_description_length == 20


def test_documentation_checker_check_model_no_description() -> None:
    """Test checker detects missing model description."""
    checker = DocumentationChecker()

    # Create mock model node without description
    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.original_file_path = "models/test_model.sql"
    mock_model.description = None
    mock_model.meta = None

    # Mock columns
    mock_model.columns = {}
    mock_model.__contains__ = lambda self, key: key == "columns"

    status = checker.check_model(mock_model)

    assert status.model_name == "test_model"
    assert status.has_description is False
    assert len(status.gaps) > 0
    # Should have a gap for missing model description
    gap_types = [g.gap_type for g in status.gaps]
    assert GapType.MISSING_MODEL_DESCRIPTION in gap_types


def test_documentation_checker_check_model_with_description() -> None:
    """Test checker with model having proper description."""
    checker = DocumentationChecker(min_model_description_length=10)

    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.original_file_path = "models/test_model.sql"
    mock_model.description = "This is a comprehensive description of the model"
    mock_model.meta = {"owner": "data-team"}
    mock_model.columns = {}
    mock_model.__contains__ = lambda self, key: key == "columns"

    status = checker.check_model(mock_model)

    assert status.has_description is True
    assert status.description_length == 48  # "This is a comprehensive description of the model"
    # Should not have missing description gap
    gap_types = [g.gap_type for g in status.gaps]
    assert GapType.MISSING_MODEL_DESCRIPTION not in gap_types


def test_documentation_checker_undocumented_columns() -> None:
    """Test checker detects undocumented columns."""
    checker = DocumentationChecker()

    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.original_file_path = "models/test.sql"
    mock_model.description = "A test model"
    mock_model.meta = None

    # Mock columns with mixed documentation
    mock_col1 = {"description": "User ID"}
    mock_col2 = {"description": ""}
    mock_col3 = {"description": "Created at timestamp"}
    mock_model.columns = {"id": mock_col1, "name": mock_col2, "created_at": mock_col3}
    mock_model.__contains__ = lambda self, key: key == "columns"

    status = checker.check_model(mock_model)

    assert status.total_columns == 3
    assert status.documented_columns == 2  # id and created_at
    assert status.total_columns - status.documented_columns == 1

    # Should have a gap for the undocumented column
    column_gaps = [g for g in status.gaps if g.column_name == "name"]
    assert len(column_gaps) > 0


def test_documentation_checker_short_description() -> None:
    """Test checker detects descriptions that are too short."""
    checker = DocumentationChecker(min_model_description_length=50)

    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.original_file_path = "models/test.sql"
    mock_model.description = "Too short"
    mock_model.meta = None
    mock_model.columns = {}
    mock_model.__contains__ = lambda self, key: key == "columns"

    status = checker.check_model(mock_model)

    # Should have an incomplete description gap
    gap_types = [g.gap_type for g in status.gaps]
    assert GapType.INCOMPLETE_DESCRIPTION in gap_types

    incomplete_gaps = [g for g in status.gaps if g.gap_type == GapType.INCOMPLETE_DESCRIPTION]
    assert len(incomplete_gaps) > 0


def test_documentation_checker_get_gaps_by_severity() -> None:
    """Test filtering gaps by severity."""
    checker = DocumentationChecker()

    gaps = [
        DocumentationGap(
            gap_type=GapType.MISSING_MODEL_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.CRITICAL,
            message="Critical gap",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.HIGH,
            message="High gap",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_MODEL_META,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.LOW,
            message="Low gap",
        ),
    ]

    # Create a mock report
    mock_report = MagicMock(spec=DocumentationReport)
    mock_report.all_gaps = gaps

    critical_gaps = checker.get_gaps_by_severity(mock_report, GapSeverity.CRITICAL)
    assert len(critical_gaps) == 1
    assert critical_gaps[0].severity == GapSeverity.CRITICAL


def test_documentation_checker_get_gaps_by_type() -> None:
    """Test filtering gaps by type."""
    checker = DocumentationChecker()

    gaps = [
        DocumentationGap(
            gap_type=GapType.MISSING_MODEL_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.CRITICAL,
            message="No model desc",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.HIGH,
            message="No column desc",
            column_name="id",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.HIGH,
            message="No column desc",
            column_name="name",
        ),
    ]

    mock_report = MagicMock(spec=DocumentationReport)
    mock_report.all_gaps = gaps

    column_gaps = checker.get_gaps_by_type(mock_report, GapType.MISSING_COLUMN_DESCRIPTION)
    assert len(column_gaps) == 2


def test_documentation_checker_get_gaps_by_model() -> None:
    """Test filtering gaps by model name."""
    checker = DocumentationChecker()

    gaps = [
        DocumentationGap(
            gap_type=GapType.MISSING_MODEL_DESCRIPTION,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.CRITICAL,
            message="Gap 1",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_COLUMN_DESCRIPTION,
            model_name="model2",
            model_path="models/model2.sql",
            severity=GapSeverity.HIGH,
            message="Gap 2",
        ),
        DocumentationGap(
            gap_type=GapType.MISSING_MODEL_META,
            model_name="model1",
            model_path="models/model1.sql",
            severity=GapSeverity.LOW,
            message="Gap 3",
        ),
    ]

    mock_report = MagicMock(spec=DocumentationReport)
    mock_report.all_gaps = gaps

    model1_gaps = checker.get_gaps_by_model(mock_report, "model1")
    assert len(model1_gaps) == 2
    assert all(g.model_name == "model1" for g in model1_gaps)


def test_documentation_report_print_summary() -> None:
    """Test DocumentationReport print_summary doesn't crash."""
    report = DocumentationReport(
        project_name="test_project",
        timestamp="2024-01-01T00:00:00Z",
        total_models=5,
        models_with_descriptions=3,
        models_without_descriptions=2,
        total_columns=50,
        documented_columns=40,
        undocumented_columns=10,
        model_statuses=[],
        all_gaps=[],
    )

    # Should not raise any exceptions
    try:
        report.print_summary()
    except Exception as e:
        pytest.fail(f"print_summary raised an exception: {e}")


def test_documentation_report_print_model_details() -> None:
    """Test DocumentationReport print_model_details doesn't crash."""
    status = ModelDocumentationStatus(
        model_name="test_model",
        model_path="models/test.sql",
        has_description=True,
        description_length=50,
        total_columns=5,
        documented_columns=3,
        has_meta=False,
        gaps=[],
    )

    report = DocumentationReport(
        project_name="test_project",
        timestamp="2024-01-01T00:00:00Z",
        total_models=1,
        models_with_descriptions=1,
        models_without_descriptions=0,
        total_columns=5,
        documented_columns=3,
        undocumented_columns=2,
        model_statuses=[status],
        all_gaps=[],
    )

    # Should not raise any exceptions
    try:
        report.print_model_details()
        report.print_model_details("test_model")
    except Exception as e:
        pytest.fail(f"print_model_details raised an exception: {e}")

    # Test with non-existent model
    try:
        report.print_model_details("non_existent")
    except Exception as e:
        pytest.fail(f"print_model_details with non-existent model raised an exception: {e}")
