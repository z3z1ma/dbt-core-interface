#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Data quality monitoring and alerting for dbt-core-interface.

This module provides automated data quality checks and alerting capabilities
for dbt projects, supporting various check types and alerting mechanisms.
"""

from __future__ import annotations

import dataclasses
import logging
import typing as t
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

import requests
from rich.console import Console
from rich.table import Table

from dbt_core_interface.project import DbtProject


def _raise_required(field_name: str) -> t.NoReturn:
    """Raise an error for required fields in dataclass inheritance."""
    raise TypeError(f"__init__ missing required argument: '{field_name}'")


__all__ = [
    "QualityCheck",
    "QualityCheckType",
    "QualityCheckResult",
    "QualityAlert",
    "QualityMonitor",
    "RowCountCheck",
    "NullPercentageCheck",
    "DuplicateCheck",
    "ValueRangeCheck",
    "CustomSqlCheck",
    "AlertChannel",
    "WebhookAlertChannel",
    "LogAlertChannel",
    "ConsoleAlertChannel",
    "Severity",
    "CheckStatus",
]

logger = logging.getLogger(__name__)
console = Console()


class QualityCheckType(str, Enum):
    """Types of quality checks available."""

    ROW_COUNT = "row_count"
    NULL_PERCENTAGE = "null_percentage"
    DUPLICATE = "duplicate"
    VALUE_RANGE = "value_range"
    CUSTOM_SQL = "custom_sql"


class CheckStatus(str, Enum):
    """Status of a quality check execution."""

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class Severity(str, Enum):
    """Severity levels for quality check failures."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True)
class QualityCheckResult:
    """Result of executing a quality check."""

    check_name: str
    check_type: QualityCheckType
    status: CheckStatus
    severity: Severity
    message: str
    actual_value: t.Any | None = None
    expected_value: t.Any | None = None
    threshold: t.Any | None = None
    sql_executed: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    execution_time_ms: float = 0.0

    def to_dict(self) -> dict[str, t.Any]:
        """Convert result to dictionary."""
        return dataclasses.asdict(self)


@dataclass
class QualityAlert:
    """Represents an alert triggered by a quality check failure."""

    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    check_result: QualityCheckResult | None = None
    project_name: str = ""
    model_name: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: dict[str, t.Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, t.Any]:
        """Convert alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "project_name": self.project_name,
            "model_name": self.model_name,
            "timestamp": self.timestamp,
            "check_result": self.check_result.to_dict() if self.check_result else None,
            "metadata": self.metadata,
        }


class AlertChannel(ABC):
    """Abstract base class for alert channels."""

    @abstractmethod
    def send(self, alert: QualityAlert) -> bool:
        """Send an alert through this channel."""

    @abstractmethod
    def close(self) -> None:
        """Close the channel and release resources."""


class LogAlertChannel(AlertChannel):
    """Alert channel that logs alerts to the configured logger."""

    def __init__(self, logger_instance: logging.Logger | None = None) -> None:
        """Initialize the log alert channel."""
        self.logger = logger_instance or logging.getLogger(__name__)

    def send(self, alert: QualityAlert) -> bool:
        """Log the alert."""
        if alert.check_result:
            level = {
                Severity.INFO: logging.INFO,
                Severity.WARNING: logging.WARNING,
                Severity.ERROR: logging.ERROR,
                Severity.CRITICAL: logging.CRITICAL,
            }.get(alert.check_result.severity, logging.WARNING)

            self.logger.log(
                level,
                "Quality Alert [%s]: %s - %s: %s",
                alert.alert_id,
                alert.project_name,
                alert.model_name,
                alert.check_result.message,
                extra={"alert": alert.to_dict()},
            )
        return True

    def close(self) -> None:
        """No-op for log channel."""


class ConsoleAlertChannel(AlertChannel):
    """Alert channel that prints alerts to the console using Rich formatting."""

    def send(self, alert: QualityAlert) -> bool:
        """Print the alert to console."""
        if alert.check_result:
            status_color = {
                CheckStatus.PASSED: "green",
                CheckStatus.FAILED: "red",
                CheckStatus.ERROR: "yellow",
                CheckStatus.SKIPPED: "blue",
            }.get(alert.check_result.status, "white")

            severity_icon = {
                Severity.INFO: "ℹ️ ",
                Severity.WARNING: "⚠️ ",
                Severity.ERROR: "❌",
                Severity.CRITICAL: "🚨",
            }.get(alert.check_result.severity, "")

            table = Table(title=f"{severity_icon} Quality Alert: {alert.alert_id[:8]}")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style=status_color)

            table.add_row("Project", alert.project_name)
            table.add_row("Model", alert.model_name)
            table.add_row("Check", alert.check_result.check_name)
            table.add_row("Type", alert.check_result.check_type.value)
            table.add_row("Status", alert.check_result.status.value)
            table.add_row("Severity", alert.check_result.severity.value)
            table.add_row("Message", alert.check_result.message)

            if alert.check_result.actual_value is not None:
                table.add_row("Actual", str(alert.check_result.actual_value))
            if alert.check_result.expected_value is not None:
                table.add_row("Expected", str(alert.check_result.expected_value))
            if alert.check_result.threshold is not None:
                table.add_row("Threshold", str(alert.check_result.threshold))

            table.add_row("Timestamp", alert.timestamp)

            console.print(table)
        return True

    def close(self) -> None:
        """No-op for console channel."""


class WebhookAlertChannel(AlertChannel):
    """Alert channel that sends alerts via HTTP webhook."""

    def __init__(
        self,
        url: str,
        timeout: float = 5.0,
        headers: dict[str, str] | None = None,
        verify_ssl: bool = True,
    ) -> None:
        """Initialize the webhook alert channel."""
        self.url = url
        self.timeout = timeout
        self.headers = headers or {"Content-Type": "application/json"}
        self.verify_ssl = verify_ssl
        self._session: requests.Session | None = None

    @property
    def session(self) -> requests.Session:
        """Get or create the requests session."""
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def send(self, alert: QualityAlert) -> bool:
        """Send alert via webhook."""
        try:
            response = self.session.post(
                self.url,
                json=alert.to_dict(),
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            logger.debug(f"Webhook alert sent successfully to {self.url}")
            return True
        except Exception as e:
            logger.error(f"Failed to send webhook alert to {self.url}: {e}")
            return False

    def close(self) -> None:
        """Close the requests session."""
        if self._session:
            self._session.close()
            self._session = None


@dataclass
class QualityCheck(ABC):
    """Abstract base class for quality checks."""

    name: str
    description: str = ""
    severity: Severity = Severity.WARNING
    enabled: bool = True

    @abstractmethod
    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the quality check."""

    def get_sql(self, model_name: str) -> str:
        """Get the SQL for this check. Override in subclasses."""
        return ""

    def _execute_query(self, project: DbtProject, sql: str, model_name: str) -> tuple[t.Any, float]:
        """Execute a query and return the result with execution time."""
        import time

        # Resolve {{ ref('model_name') }} in the SQL
        node = project.ref(model_name)
        if not node:
            raise ValueError(f"Model '{model_name}' not found in project")

        start_time = time.perf_counter()
        result = project.execute_sql(sql, compile=True)
        execution_time = (time.perf_counter() - start_time) * 1000

        if not result.table.rows:
            return None, execution_time

        return result.table.rows[0][0], execution_time


@dataclass
class RowCountCheck(QualityCheck):
    """Check that the row count of a model is within expected bounds."""

    min_rows: int | None = None
    max_rows: int | None = None

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.min_rows is None and self.max_rows is None:
            raise ValueError("At least one of min_rows or max_rows must be specified")

    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the row count check."""
        sql = self.get_sql(model_name)
        try:
            actual_count, exec_time = self._execute_query(project, sql, model_name)

            if actual_count is None:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.ROW_COUNT,
                    status=CheckStatus.ERROR,
                    severity=self.severity,
                    message="Query returned no results",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            if self.min_rows is not None and actual_count < self.min_rows:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.ROW_COUNT,
                    status=CheckStatus.FAILED,
                    severity=self.severity,
                    message=f"Row count {actual_count} is below minimum {self.min_rows}",
                    actual_value=actual_count,
                    expected_value=self.min_rows,
                    threshold="min",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            if self.max_rows is not None and actual_count > self.max_rows:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.ROW_COUNT,
                    status=CheckStatus.FAILED,
                    severity=self.severity,
                    message=f"Row count {actual_count} exceeds maximum {self.max_rows}",
                    actual_value=actual_count,
                    expected_value=self.max_rows,
                    threshold="max",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.ROW_COUNT,
                status=CheckStatus.PASSED,
                severity=Severity.INFO,
                message=f"Row count {actual_count} is within bounds",
                actual_value=actual_count,
                sql_executed=sql,
                execution_time_ms=exec_time,
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.ROW_COUNT,
                status=CheckStatus.ERROR,
                severity=Severity.ERROR,
                message=f"Error executing check: {e}",
                sql_executed=sql,
            )

    def get_sql(self, model_name: str) -> str:
        """Get the SQL for row count check."""
        return f"SELECT COUNT(*) AS row_count FROM {{{{ ref('{model_name}') }}}}"  # noqa: S608


@dataclass
class NullPercentageCheck(QualityCheck):
    """Check that null percentage in a column is within acceptable bounds."""

    column_name: str = field(default_factory=lambda: _raise_required("column_name"))
    max_null_percentage: float = 0.0

    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the null percentage check."""
        sql = self.get_sql(model_name)
        try:
            actual_percentage, exec_time = self._execute_query(project, sql, model_name)

            if actual_percentage is None:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.NULL_PERCENTAGE,
                    status=CheckStatus.ERROR,
                    severity=self.severity,
                    message="Query returned no results",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            if actual_percentage > self.max_null_percentage:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.NULL_PERCENTAGE,
                    status=CheckStatus.FAILED,
                    severity=self.severity,
                    message=f"Null percentage {actual_percentage:.2f}% in column '{self.column_name}' exceeds threshold {self.max_null_percentage:.2f}%",
                    actual_value=round(actual_percentage, 2),
                    expected_value=self.max_null_percentage,
                    threshold="max",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.NULL_PERCENTAGE,
                status=CheckStatus.PASSED,
                severity=Severity.INFO,
                message=f"Null percentage {actual_percentage:.2f}% is within bounds",
                actual_value=round(actual_percentage, 2),
                sql_executed=sql,
                execution_time_ms=exec_time,
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.NULL_PERCENTAGE,
                status=CheckStatus.ERROR,
                severity=Severity.ERROR,
                message=f"Error executing check: {e}",
                sql_executed=sql,
            )

    def get_sql(self, model_name: str) -> str:  # noqa: S608
        """Get the SQL for null percentage check."""
        return f"""  # noqa: S608
        SELECT
            100.0 * COUNT(NULLIF({self.column_name}, NULL)) / NULLIF(COUNT(*), 0) AS null_percentage
        FROM {{{{ ref('{model_name}') }}}}
        """  # noqa: S608


@dataclass
class DuplicateCheck(QualityCheck):
    """Check for duplicate rows based on specified columns."""

    columns: list[str] = field(default_factory=list)
    max_duplicate_percentage: float = 0.0

    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the duplicate check."""
        sql = self.get_sql(model_name)
        try:
            duplicate_percentage, exec_time = self._execute_query(project, sql, model_name)

            if duplicate_percentage is None:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.DUPLICATE,
                    status=CheckStatus.ERROR,
                    severity=self.severity,
                    message="Query returned no results",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            if duplicate_percentage > self.max_duplicate_percentage:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.DUPLICATE,
                    status=CheckStatus.FAILED,
                    severity=self.severity,
                    message=f"Duplicate percentage {duplicate_percentage:.2f}% exceeds threshold {self.max_duplicate_percentage:.2f}%",
                    actual_value=round(duplicate_percentage, 2),
                    expected_value=self.max_duplicate_percentage,
                    threshold="max",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.DUPLICATE,
                status=CheckStatus.PASSED,
                severity=Severity.INFO,
                message=f"Duplicate percentage {duplicate_percentage:.2f}% is within bounds",
                actual_value=round(duplicate_percentage, 2),
                sql_executed=sql,
                execution_time_ms=exec_time,
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.DUPLICATE,
                status=CheckStatus.ERROR,
                severity=Severity.ERROR,
                message=f"Error executing check: {e}",
                sql_executed=sql,
            )

    def get_sql(self, model_name: str) -> str:  # noqa: S608
        """Get the SQL for duplicate check."""
        cols = ", ".join(self.columns)
        return f"""  # noqa: S608
        SELECT
            100.0 * SUM(duplicate_count) / NULLIF(SUM(total_count), 0) AS duplicate_percentage
        FROM (
            SELECT
                {cols},
                COUNT(*) - 1 AS duplicate_count,
                COUNT(*) AS total_count
            FROM {{{{ ref('{model_name}') }}}}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        ) duplicates
        COALESCE(
            (SELECT 0 FROM (SELECT 1) WHERE NOT EXISTS (
                SELECT 1 FROM {{{{ ref('{model_name}') }}}} GROUP BY {cols} HAVING COUNT(*) > 1
            )),
            0
        )
        """  # noqa: S608


@dataclass
class ValueRangeCheck(QualityCheck):
    """Check that values in a column are within a specified range."""

    column_name: str = field(default_factory=lambda: _raise_required("column_name"))
    min_value: float | int | None = None
    max_value: float | int | None = None

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.min_value is None and self.max_value is None:
            raise ValueError("At least one of min_value or max_value must be specified")

    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the value range check."""
        sql = self.get_sql(model_name)
        try:
            result, exec_time = self._execute_query(project, sql, model_name)

            if result is None:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.VALUE_RANGE,
                    status=CheckStatus.ERROR,
                    severity=self.severity,
                    message="Query returned no results",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            min_violated = self.min_value is not None and result < self.min_value
            max_violated = self.max_value is not None and result > self.max_value

            if min_violated or max_violated:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.VALUE_RANGE,
                    status=CheckStatus.FAILED,
                    severity=self.severity,
                    message=f"Value {result} in column '{self.column_name}' is outside allowed range [{self.min_value}, {self.max_value}]",
                    actual_value=result,
                    expected_value={"min": self.min_value, "max": self.max_value},
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.VALUE_RANGE,
                status=CheckStatus.PASSED,
                severity=Severity.INFO,
                message=f"All values in column '{self.column_name}' are within range",
                actual_value=result,
                sql_executed=sql,
                execution_time_ms=exec_time,
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.VALUE_RANGE,
                status=CheckStatus.ERROR,
                severity=Severity.ERROR,
                message=f"Error executing check: {e}",
                sql_executed=sql,
            )

    def get_sql(self, model_name: str) -> str:  # noqa: S608
        """Get the SQL for value range check."""
        conditions = []
        if self.min_value is not None:
            conditions.append(f"{self.column_name} < {self.min_value}")
        if self.max_value is not None:
            conditions.append(f"{self.column_name} > {self.max_value}")

        where_clause = " OR ".join(conditions)
        return f"""  # noqa: S608
        SELECT COUNT(*) FROM {{{{ ref('{model_name}') }}}}
        WHERE {where_clause}
        LIMIT 1
        """  # noqa: S608


@dataclass
class CustomSqlCheck(QualityCheck):
    """Custom SQL check that returns a boolean or numeric result."""

    sql_template: str = field(default_factory=lambda: _raise_required("sql_template"))
    expect_true: bool = True

    def execute(self, project: DbtProject, model_name: str) -> QualityCheckResult:
        """Execute the custom SQL check."""
        sql = self.get_sql(model_name)
        try:
            result, exec_time = self._execute_query(project, sql, model_name)

            if result is None:
                return QualityCheckResult(
                    check_name=self.name,
                    check_type=QualityCheckType.CUSTOM_SQL,
                    status=CheckStatus.ERROR,
                    severity=self.severity,
                    message="Query returned no results",
                    sql_executed=sql,
                    execution_time_ms=exec_time,
                )

            is_passed = bool(result) == self.expect_true

            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.CUSTOM_SQL,
                status=CheckStatus.PASSED if is_passed else CheckStatus.FAILED,
                severity=self.severity if not is_passed else Severity.INFO,
                message=f"Custom check returned: {result}",
                actual_value=result,
                expected_value=self.expect_true,
                sql_executed=sql,
                execution_time_ms=exec_time,
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=self.name,
                check_type=QualityCheckType.CUSTOM_SQL,
                status=CheckStatus.ERROR,
                severity=Severity.ERROR,
                message=f"Error executing check: {e}",
                sql_executed=sql,
            )

    def get_sql(self, model_name: str) -> str:
        """Get the SQL for custom check."""
        return self.sql_template


@dataclass
class QualityMonitor:
    """Monitor for managing and executing quality checks."""

    project: DbtProject
    checks: dict[str, list[QualityCheck]] = field(default_factory=dict)
    alert_channels: list[AlertChannel] = field(default_factory=list)

    def add_check(self, model_name: str, check: QualityCheck) -> None:
        """Add a quality check for a model."""
        if model_name not in self.checks:
            self.checks[model_name] = []
        self.checks[model_name].append(check)

    def remove_check(self, model_name: str, check_name: str) -> bool:
        """Remove a quality check."""
        if model_name not in self.checks:
            return False
        self.checks[model_name] = [c for c in self.checks[model_name] if c.name != check_name]
        if not self.checks[model_name]:
            del self.checks[model_name]
        return True

    def add_alert_channel(self, channel: AlertChannel) -> None:
        """Add an alert channel."""
        self.alert_channels.append(channel)

    def run_checks(
        self, model_name: str | None = None, only_enabled: bool = True
    ) -> list[QualityCheckResult]:
        """Run quality checks for a model or all models."""
        results: list[QualityCheckResult] = []

        models_to_check = [model_name] if model_name else list(self.checks.keys())

        for model in models_to_check:
            if model not in self.checks:
                continue

            for check in self.checks[model]:
                if only_enabled and not check.enabled:
                    continue

                try:
                    result = check.execute(self.project, model)
                    results.append(result)

                    # Send alerts for failed/error checks
                    if result.status in (CheckStatus.FAILED, CheckStatus.ERROR):
                        alert = QualityAlert(
                            check_result=result,
                            project_name=self.project.project_name,
                            model_name=model,
                        )
                        self._send_alert(alert)

                except Exception as e:
                    logger.error(f"Error running check {check.name} on {model}: {e}")
                    results.append(
                        QualityCheckResult(
                            check_name=check.name,
                            check_type=QualityCheckType.CUSTOM_SQL,
                            status=CheckStatus.ERROR,
                            severity=Severity.ERROR,
                            message=f"Unexpected error: {e}",
                        )
                    )

        return results

    def run_check_by_name(self, model_name: str, check_name: str) -> QualityCheckResult | None:
        """Run a specific check by name."""
        if model_name not in self.checks:
            return None

        for check in self.checks[model_name]:
            if check.name == check_name:
                result = check.execute(self.project, model_name)
                if result.status in (CheckStatus.FAILED, CheckStatus.ERROR):
                    alert = QualityAlert(
                        check_result=result,
                        project_name=self.project.project_name,
                        model_name=model_name,
                    )
                    self._send_alert(alert)
                return result

        return None

    def get_checks(self, model_name: str | None = None) -> dict[str, list[dict[str, t.Any]]]:
        """Get all checks, optionally filtered by model."""
        result: dict[str, list[dict[str, t.Any]]] = {}

        models = [model_name] if model_name else list(self.checks.keys())

        for model in models:
            if model not in self.checks:
                continue
            result[model] = [
                {
                    "name": check.name,
                    "type": type(check).__name__,
                    "description": check.description,
                    "severity": check.severity.value,
                    "enabled": check.enabled,
                }
                for check in self.checks[model]
            ]

        return result

    def _send_alert(self, alert: QualityAlert) -> None:
        """Send alert through all configured channels."""
        for channel in self.alert_channels:
            try:
                channel.send(alert)
            except Exception as e:
                logger.error(f"Failed to send alert via channel: {e}")

    def close(self) -> None:
        """Close all alert channels."""
        for channel in self.alert_channels:
            try:
                channel.close()
            except Exception as e:
                logger.error(f"Error closing alert channel: {e}")
        self.alert_channels.clear()
