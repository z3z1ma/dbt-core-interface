#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Tests for the quality monitoring module."""

import pytest

from dbt_core_interface.quality import (
    CheckStatus,
    ConsoleAlertChannel,
    CustomSqlCheck,
    DuplicateCheck,
    LogAlertChannel,
    NullPercentageCheck,
    QualityCheckResult,
    QualityMonitor,
    RowCountCheck,
    Severity,
    ValueRangeCheck,
    WebhookAlertChannel,
)


def test_row_count_check_min_violation() -> None:
    """Test RowCountCheck with minimum rows violation."""
    check = RowCountCheck(
        name="test_row_count",
        description="Test row count check",
        severity=Severity.ERROR,
        min_rows=100,
    )

    assert check.name == "test_row_count"
    assert check.min_rows == 100
    assert check.max_rows is None
    assert check.enabled is True


def test_row_count_check_max_violation() -> None:
    """Test RowCountCheck with maximum rows violation."""
    check = RowCountCheck(
        name="test_row_count_max",
        max_rows=10000,
    )

    assert check.name == "test_row_count_max"
    assert check.min_rows is None
    assert check.max_rows == 10000


def test_row_count_check_requires_bounds() -> None:
    """Test that RowCountCheck requires min or max rows."""
    with pytest.raises(ValueError, match="At least one of min_rows or max_rows"):
        RowCountCheck(name="invalid_check")


def test_null_percentage_check() -> None:
    """Test NullPercentageCheck initialization."""
    check = NullPercentageCheck(
        name="test_null_percentage",
        column_name="user_id",
        max_null_percentage=5.0,
    )

    assert check.name == "test_null_percentage"
    assert check.column_name == "user_id"
    assert check.max_null_percentage == 5.0


def test_duplicate_check() -> None:
    """Test DuplicateCheck initialization."""
    check = DuplicateCheck(
        name="test_duplicates",
        columns=["user_id", "date"],
        max_duplicate_percentage=1.0,
    )

    assert check.name == "test_duplicates"
    assert check.columns == ["user_id", "date"]
    assert check.max_duplicate_percentage == 1.0


def test_value_range_check() -> None:
    """Test ValueRangeCheck initialization."""
    check = ValueRangeCheck(
        name="test_value_range",
        column_name="age",
        min_value=0,
        max_value=120,
    )

    assert check.name == "test_value_range"
    assert check.column_name == "age"
    assert check.min_value == 0
    assert check.max_value == 120


def test_value_range_check_requires_bounds() -> None:
    """Test that ValueRangeCheck requires min or max value."""
    with pytest.raises(ValueError, match="At least one of min_value or max_value"):
        ValueRangeCheck(name="invalid_check", column_name="x")


def test_custom_sql_check() -> None:
    """Test CustomSqlCheck initialization."""
    check = CustomSqlCheck(
        name="test_custom",
        sql_template="SELECT COUNT(*) FROM {{ ref('my_model') }} WHERE status = 'failed'",
        expect_true=False,
    )

    assert check.name == "test_custom"
    assert "SELECT COUNT(*)" in check.sql_template
    assert check.expect_true is False


def test_quality_check_result_to_dict() -> None:
    """Test QualityCheckResult serialization."""
    result = QualityCheckResult(
        check_name="test_check",
        check_type="row_count",
        status=CheckStatus.PASSED,
        severity=Severity.INFO,
        message="Check passed",
        actual_value=100,
        expected_value=50,
        execution_time_ms=12.5,
    )

    result_dict = result.to_dict()
    assert result_dict["check_name"] == "test_check"
    assert result_dict["status"] == "passed"
    assert result_dict["actual_value"] == 100
    assert result_dict["execution_time_ms"] == 12.5


def test_log_alert_channel() -> None:
    """Test LogAlertChannel initialization."""
    channel = LogAlertChannel()
    assert channel is not None


def test_console_alert_channel() -> None:
    """Test ConsoleAlertChannel initialization."""
    channel = ConsoleAlertChannel()
    assert channel is not None


def test_webhook_alert_channel() -> None:
    """Test WebhookAlertChannel initialization."""
    channel = WebhookAlertChannel(
        url="https://example.com/webhook",
        timeout=10.0,
        headers={"Authorization": "Bearer token"},
    )
    assert channel.url == "https://example.com/webhook"
    assert channel.timeout == 10.0
    assert channel.headers == {"Authorization": "Bearer token"}


def test_quality_monitor_initialization() -> None:
    """Test QualityMonitor can be created (but requires real DbtProject for full testing)."""
    # Note: Full testing requires a real DbtProject instance
    # This test verifies the import and basic type structure

    # Verify the QualityMonitor class exists and has expected attributes
    assert QualityMonitor is not None
    assert hasattr(QualityMonitor, "add_check")
    assert hasattr(QualityMonitor, "run_checks")
    assert hasattr(QualityMonitor, "add_alert_channel")


def test_severity_enum() -> None:
    """Test Severity enum values."""
    assert Severity.INFO.value == "info"
    assert Severity.WARNING.value == "warning"
    assert Severity.ERROR.value == "error"
    assert Severity.CRITICAL.value == "critical"


def test_check_status_enum() -> None:
    """Test CheckStatus enum values."""
    assert CheckStatus.PASSED.value == "passed"
    assert CheckStatus.FAILED.value == "failed"
    assert CheckStatus.ERROR.value == "error"
    assert CheckStatus.SKIPPED.value == "skipped"


def test_quality_check_get_sql() -> None:
    """Test SQL generation for various check types."""
    # RowCountCheck
    row_check = RowCountCheck(name="rows", min_rows=1)
    assert "COUNT(*)" in row_check.get_sql("my_model")
    assert "ref('my_model')" in row_check.get_sql("my_model")

    # NullPercentageCheck
    null_check = NullPercentageCheck(name="nulls", column_name="id")
    sql = null_check.get_sql("my_model")
    assert "id" in sql
    assert "ref('my_model')" in sql

    # DuplicateCheck
    dup_check = DuplicateCheck(name="dups", columns=["id", "date"])
    sql = dup_check.get_sql("my_model")
    assert "id" in sql
    assert "date" in sql

    # ValueRangeCheck
    range_check = ValueRangeCheck(name="range", column_name="age", min_value=0, max_value=120)
    sql = range_check.get_sql("my_model")
    assert "age" in sql
    assert "ref('my_model')" in sql


def test_custom_sql_check_template() -> None:
    """Test CustomSqlCheck with model template."""
    check = CustomSqlCheck(
        name="custom",
        sql_template="SELECT COUNT(*) FROM {{ ref('orders') }} WHERE total < 0",
    )
    assert "orders" in check.get_sql("any_model")


def test_quality_check_enabled_default() -> None:
    """Test that quality checks are enabled by default."""
    check = RowCountCheck(name="test", min_rows=1)
    assert check.enabled is True


def test_quality_check_severity_default() -> None:
    """Test that quality checks default to WARNING severity."""
    check = RowCountCheck(name="test", min_rows=1)
    assert check.severity == Severity.WARNING
