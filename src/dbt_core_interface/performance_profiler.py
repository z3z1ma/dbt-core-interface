#!/usr/bin/env python
# pyright: reportPrivateImportUsage=false,reportAny=false
"""Automated model performance profiling for dbt-core-interface.

Tracks query execution times, identifies slow models, and suggests
optimization opportunities based on dbt run results and adapter responses.
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
import time
import threading
import typing as t
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

if t.TYPE_CHECKING:
    from dbt.adapters.contracts.connection import AdapterResponse
    from dbt.contracts.graph.manifest import Manifest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PerformanceMetric:
    """A single performance measurement for a model execution."""

    model_name: str
    model_unique_id: str
    execution_time_ms: float
    rows_affected: int
    bytes_processed: int
    timestamp: float
    run_id: str
    materialization: str = "table"
    database: str = ""
    schema: str = ""

    def to_dict(self) -> dict[str, t.Any]:
        """Convert to dictionary."""
        return {
            "model_name": self.model_name,
            "model_unique_id": self.model_unique_id,
            "execution_time_ms": self.execution_time_ms,
            "rows_affected": self.rows_affected,
            "bytes_processed": self.bytes_processed,
            "timestamp": self.timestamp,
            "run_id": self.run_id,
            "materialization": self.materialization,
            "database": self.database,
            "schema": self.schema,
        }


@dataclass(frozen=True)
class PerformanceSummary:
    """Summary statistics for a model's performance over time."""

    model_name: str
    model_unique_id: str
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    p50_time_ms: float
    p95_time_ms: float
    p99_time_ms: float
    sample_count: int
    last_execution: float

    def to_dict(self) -> dict[str, t.Any]:
        """Convert to dictionary."""
        return {
            "model_name": self.model_name,
            "model_unique_id": self.model_unique_id,
            "avg_time_ms": self.avg_time_ms,
            "min_time_ms": self.min_time_ms,
            "max_time_ms": self.max_time_ms,
            "p50_time_ms": self.p50_time_ms,
            "p95_time_ms": self.p95_time_ms,
            "p99_time_ms": self.p99_time_ms,
            "sample_count": self.sample_count,
            "last_execution": self.last_execution,
        }


@dataclass(frozen=True)
class OptimizationSuggestion:
    """Suggestion for improving model performance."""

    model_name: str
    model_unique_id: str
    suggestion_type: str
    priority: str  # "low", "medium", "high"
    description: str
    current_value: t.Any
    suggested_value: t.Any
    estimated_improvement: str

    def to_dict(self) -> dict[str, t.Any]:
        """Convert to dictionary."""
        return {
            "model_name": self.model_name,
            "model_unique_id": self.model_unique_id,
            "suggestion_type": self.suggestion_type,
            "priority": self.priority,
            "description": self.description,
            "current_value": self.current_value,
            "suggested_value": self.suggested_value,
            "estimated_improvement": self.estimated_improvement,
        }


class PerformanceProfiler:
    """Tracks and analyzes dbt model performance over time."""

    _lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        storage_path: Path | str | None = None,
        slow_threshold_ms: float = 5000.0,
        history_days: int = 30,
    ) -> None:
        """Initialize the performance profiler.

        Args:
            storage_path: Path to store performance data (defaults to ~/.dbt_core_interface/performance.db)
            slow_threshold_ms: Threshold in milliseconds for considering a model "slow"
            history_days: Number of days to keep performance history
        """
        if storage_path is None:
            storage_path = Path.home() / ".dbt_core_interface" / "performance.db"
        else:
            storage_path = Path(storage_path)

        self._storage_path: Path = storage_path
        self._slow_threshold_ms: float = slow_threshold_ms
        self._history_days: int = history_days
        self._conn: sqlite3.Connection | None = None
        self._run_id: str = str(uuid.uuid4())

        # Initialize storage
        self._init_storage()

    def _init_storage(self) -> None:
        """Initialize SQLite storage for performance metrics."""
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._storage_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                model_unique_id TEXT NOT NULL,
                execution_time_ms REAL NOT NULL,
                rows_affected INTEGER DEFAULT 0,
                bytes_processed INTEGER DEFAULT 0,
                timestamp REAL NOT NULL,
                run_id TEXT NOT NULL,
                materialization TEXT DEFAULT 'table',
                database TEXT DEFAULT '',
                schema TEXT DEFAULT ''
            )
        """
        )

        # Create indexes for common queries
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_name
            ON performance_metrics(model_name)
        """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_timestamp
            ON performance_metrics(timestamp)
        """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_execution_time
            ON performance_metrics(execution_time_ms)
        """
        )
        self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _cleanup_old_metrics(self) -> None:
        """Remove metrics older than history_days."""
        if self._conn is None:
            return

        cutoff_time = time.time() - (self._history_days * 86400)
        cursor = self._conn.execute(
            "DELETE FROM performance_metrics WHERE timestamp < ?",
            (cutoff_time,),
        )
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old performance metrics")
            self._conn.commit()

    def record_metric(
        self,
        model_name: str,
        model_unique_id: str,
        execution_time_ms: float,
        rows_affected: int = 0,
        bytes_processed: int = 0,
        materialization: str = "table",
        database: str = "",
        schema: str = "",
    ) -> PerformanceMetric:
        """Record a performance metric for a model execution.

        Args:
            model_name: Name of the model
            model_unique_id: Unique ID of the model (e.g., "model.project.name")
            execution_time_ms: Execution time in milliseconds
            rows_affected: Number of rows affected by the query
            bytes_processed: Estimated bytes processed
            materialization: Materialization type (table, view, incremental, etc.)
            database: Database name
            schema: Schema name

        Returns:
            The recorded PerformanceMetric
        """
        timestamp = time.time()
        metric = PerformanceMetric(
            model_name=model_name,
            model_unique_id=model_unique_id,
            execution_time_ms=execution_time_ms,
            rows_affected=rows_affected,
            bytes_processed=bytes_processed,
            timestamp=timestamp,
            run_id=self._run_id,
            materialization=materialization,
            database=database,
            schema=schema,
        )

        with self._lock:
            if self._conn is None:
                return metric

            self._conn.execute(
                """
                INSERT INTO performance_metrics
                (model_name, model_unique_id, execution_time_ms, rows_affected,
                 bytes_processed, timestamp, run_id, materialization, database, schema)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    model_name,
                    model_unique_id,
                    execution_time_ms,
                    rows_affected,
                    bytes_processed,
                    timestamp,
                    self._run_id,
                    materialization,
                    database,
                    schema,
                ),
            )
            self._conn.commit()

            # Periodically clean up old metrics
            if int(timestamp) % 3600 == 0:  # Every hour roughly
                self._cleanup_old_metrics()

        logger.debug(
            f"Recorded performance metric: {model_name} - {execution_time_ms:.2f}ms"
        )
        return metric

    def record_from_adapter_response(
        self,
        model_name: str,
        model_unique_id: str,
        adapter_response: AdapterResponse,
        materialization: str = "table",
        database: str = "",
        schema: str = "",
    ) -> PerformanceMetric:
        """Record performance metric from dbt adapter response.

        Args:
            model_name: Name of the model
            model_unique_id: Unique ID of the model
            adapter_response: The AdapterResponse from dbt adapter execution
            materialization: Materialization type
            database: Database name
            schema: Schema name

        Returns:
            The recorded PerformanceMetric
        """
        # Extract timing from adapter response
        execution_time_ms = (
            getattr(adapter_response, "_elapsed_time", None) or
            getattr(adapter_response, "elapsed_time", None) or
            0.0
        ) * 1000  # Convert to milliseconds

        # Try to get rows affected from response
        rows_affected = (
            getattr(adapter_response, "rows_affected", None) or
            getattr(adapter_response, "_rows_affected", None) or
            0
        )

        return self.record_metric(
            model_name=model_name,
            model_unique_id=model_unique_id,
            execution_time_ms=execution_time_ms,
            rows_affected=rows_affected,
            bytes_processed=0,  # Not reliably available
            materialization=materialization,
            database=database,
            schema=schema,
        )

    def get_slow_models(
        self,
        limit: int = 10,
        threshold_ms: float | None = None,
    ) -> list[PerformanceSummary]:
        """Get models with the slowest average execution times.

        Args:
            limit: Maximum number of models to return
            threshold_ms: Optional override of slow_threshold_ms

        Returns:
            List of PerformanceSummary for slow models
        """
        threshold = threshold_ms or self._slow_threshold_ms

        if self._conn is None:
            return []

        cutoff_time = time.time() - (7 * 86400)  # Last 7 days

        cursor = self._conn.execute(
            """
            SELECT
                model_name,
                model_unique_id,
                AVG(execution_time_ms) as avg_time,
                MIN(execution_time_ms) as min_time,
                MAX(execution_time_ms) as max_time,
                COUNT(*) as sample_count,
                MAX(timestamp) as last_execution
            FROM performance_metrics
            WHERE timestamp > ?
            GROUP BY model_name, model_unique_id
            HAVING avg_time > ?
            ORDER BY avg_time DESC
            LIMIT ?
        """,
            (cutoff_time, threshold, limit),
        )

        results = []
        for row in cursor.fetchall():
            # Get percentiles
            model_id = row[1]
            p_cursor = self._conn.execute(
                """
                SELECT execution_time_ms
                FROM performance_metrics
                WHERE model_unique_id = ? AND timestamp > ?
                ORDER BY execution_time_ms
            """,
                (model_id, cutoff_time),
            )
            times = [r[0] for r in p_cursor.fetchall()]

            p50 = self._percentile(times, 50)
            p95 = self._percentile(times, 95)
            p99 = self._percentile(times, 99)

            results.append(
                PerformanceSummary(
                    model_name=row[0],
                    model_unique_id=row[1],
                    avg_time_ms=row[2],
                    min_time_ms=row[3],
                    max_time_ms=row[4],
                    p50_time_ms=p50,
                    p95_time_ms=p95,
                    p99_time_ms=p99,
                    sample_count=row[5],
                    last_execution=row[6],
                )
            )

        return results

    def get_model_summary(
        self,
        model_name: str | None = None,
        model_unique_id: str | None = None,
        days: int = 7,
    ) -> PerformanceSummary | None:
        """Get performance summary for a specific model.

        Args:
            model_name: Filter by model name
            model_unique_id: Filter by model unique ID
            days: Number of days to look back

        Returns:
            PerformanceSummary or None if no data found
        """
        if self._conn is None:
            return None

        cutoff_time = time.time() - (days * 86400)

        where_clause = "timestamp > ?"
        params: list[t.Any] = [cutoff_time]

        if model_unique_id:
            where_clause += " AND model_unique_id = ?"
            params.append(model_unique_id)
        elif model_name:
            where_clause += " AND model_name = ?"
            params.append(model_name)
        else:
            return None

        cursor = self._conn.execute(
            f"""
            SELECT
                model_name,
                model_unique_id,
                AVG(execution_time_ms) as avg_time,
                MIN(execution_time_ms) as min_time,
                MAX(execution_time_ms) as max_time,
                COUNT(*) as sample_count,
                MAX(timestamp) as last_execution
            FROM performance_metrics
            WHERE {where_clause}
            GROUP BY model_name, model_unique_id
        """,
            params,
        )

        row = cursor.fetchone()
        if not row:
            return None

        # Get percentiles
        p_cursor = self._conn.execute(
            """
            SELECT execution_time_ms
            FROM performance_metrics
            WHERE model_unique_id = ? AND timestamp > ?
            ORDER BY execution_time_ms
        """,
            (row[1], cutoff_time),
        )
        times = [r[0] for r in p_cursor.fetchall()]

        p50 = self._percentile(times, 50)
        p95 = self._percentile(times, 95)
        p99 = self._percentile(times, 99)

        return PerformanceSummary(
            model_name=row[0],
            model_unique_id=row[1],
            avg_time_ms=row[2],
            min_time_ms=row[3],
            max_time_ms=row[4],
            p50_time_ms=p50,
            p95_time_ms=p95,
            p99_time_ms=p99,
            sample_count=row[5],
            last_execution=row[6],
        )

    def get_metrics_history(
        self,
        model_name: str | None = None,
        model_unique_id: str | None = None,
        limit: int = 100,
    ) -> list[PerformanceMetric]:
        """Get historical metrics for a model.

        Args:
            model_name: Filter by model name
            model_unique_id: Filter by model unique ID
            limit: Maximum number of records to return

        Returns:
            List of PerformanceMetric
        """
        if self._conn is None:
            return []

        where_clause = "1=1"
        params: list[t.Any] = []

        if model_unique_id:
            where_clause = "model_unique_id = ?"
            params.append(model_unique_id)
        elif model_name:
            where_clause = "model_name = ?"
            params.append(model_name)

        params.append(limit)

        cursor = self._conn.execute(
            f"""
            SELECT
                model_name, model_unique_id, execution_time_ms,
                rows_affected, bytes_processed, timestamp, run_id,
                materialization, database, schema
            FROM performance_metrics
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
        """,
            params,
        )

        results = []
        for row in cursor.fetchall():
            results.append(
                PerformanceMetric(
                    model_name=row[0],
                    model_unique_id=row[1],
                    execution_time_ms=row[2],
                    rows_affected=row[3],
                    bytes_processed=row[4],
                    timestamp=row[5],
                    run_id=row[6],
                    materialization=row[7],
                    database=row[8],
                    schema=row[9],
                )
            )

        return results

    @staticmethod
    def _percentile(data: list[float], p: int) -> float:
        """Calculate percentile of a list of values.

        Args:
            data: List of numeric values
            p: Percentile to calculate (0-100)

        Returns:
            Percentile value
        """
        if not data:
            return 0.0
        if len(data) == 1:
            return data[0]
        k = (len(data) - 1) * (p / 100)
        f = int(k)
        c = f + 1
        if c >= len(data):
            c = len(data) - 1
        if f == c:
            return data[f]
        return data[f] + (k - f) * (data[c] - data[f])

    def suggest_optimizations(
        self,
        manifest: Manifest,
        threshold_ms: float | None = None,
    ) -> list[OptimizationSuggestion]:
        """Generate optimization suggestions based on performance data.

        Args:
            manifest: The dbt manifest to analyze
            threshold_ms: Override slow threshold

        Returns:
            List of OptimizationSuggestion
        """
        suggestions: list[OptimizationSuggestion] = []
        slow_models = self.get_slow_models(limit=50, threshold_ms=threshold_ms)

        for summary in slow_models:
            # Find the node in the manifest
            node = None
            for unique_id, manifest_node in manifest.nodes.items():
                if unique_id == summary.model_unique_id:
                    node = manifest_node
                    break

            if not node:
                continue

            # Check materialization
            if node.config.get("materialized") == "table":
                if summary.sample_count >= 3:
                    # If stable and relatively fast, suggest view
                    if summary.p95_time_ms < 2000:
                        suggestions.append(
                            OptimizationSuggestion(
                                model_name=summary.model_name,
                                model_unique_id=summary.model_unique_id,
                                suggestion_type="materialization",
                                priority="low",
                                description="Model is fast and stable. Consider using view for less overhead.",
                                current_value="table",
                                suggested_value="view",
                                estimated_improvement="Reduced storage overhead",
                            )
                        )

            # Check for incremental opportunities
            if node.config.get("materialized") == "table":
                # Look for dependencies on sources or large tables
                has_source_dep = False
                for dep in node.depends_on.get("nodes", []):
                    if dep.startswith("source."):
                        has_source_dep = True
                        break

                if has_source_dep and summary.avg_time_ms > 10000:
                    suggestions.append(
                        OptimizationSuggestion(
                            model_name=summary.model_name,
                            model_unique_id=summary.model_unique_id,
                            suggestion_type="incremental",
                            priority="high",
                            description="Model has source dependencies and is slow. Incremental materialization could improve performance.",
                            current_value="table",
                            suggested_value="incremental",
                            estimated_improvement="~50-90% faster on subsequent runs",
                        )
                    )

            # Check for sorting/distinct hints (for very slow models)
            if summary.avg_time_ms > 30000:
                suggestions.append(
                    OptimizationSuggestion(
                        model_name=summary.model_name,
                        model_unique_id=summary.model_unique_id,
                        suggestion_type="indexing",
                        priority="medium",
                        description="Model execution time is very high. Consider adding database indexes on join keys.",
                        current_value="N/A",
                        suggested_value="Add indexes on frequently joined columns",
                        estimated_improvement="~20-50% faster queries",
                    )
                )

            # Suggest partitioning for very large datasets
            if summary.rows_affected > 1_000_000 and node.config.get("materialized") == "table":
                suggestions.append(
                    OptimizationSuggestion(
                        model_name=summary.model_name,
                        model_unique_id=summary.model_unique_id,
                        suggestion_type="partitioning",
                        priority="medium",
                        description="Model processes large row counts. Consider date partitioning.",
                        current_value="Unpartitioned",
                        suggested_value="Partition by date column",
                        estimated_improvement="~30-60% faster for time-bounded queries",
                    )
                )

        return suggestions

    def export_metrics(
        self,
        output_path: Path | str,
        format: str = "json",  # noqa: A001
        model_name: str | None = None,
        days: int = 30,
    ) -> None:
        """Export performance metrics to a file.

        Args:
            output_path: Path to write the export
            format: Export format ('json' or 'csv')
            model_name: Optional model name filter
            days: Number of days to include
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        metrics = self.get_metrics_history(
            model_name=model_name,
            limit=10000,  # Large limit to get all relevant data
        )

        # Filter by date
        cutoff = time.time() - (days * 86400)
        metrics = [m for m in metrics if m.timestamp > cutoff]

        if format.lower() == "csv":
            with open(output_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "model_name",
                        "model_unique_id",
                        "execution_time_ms",
                        "rows_affected",
                        "bytes_processed",
                        "timestamp",
                        "run_id",
                        "materialization",
                        "database",
                        "schema",
                    ]
                )
                for m in metrics:
                    writer.writerow(
                        [
                            m.model_name,
                            m.model_unique_id,
                            m.execution_time_ms,
                            m.rows_affected,
                            m.bytes_processed,
                            m.timestamp,
                            m.run_id,
                            m.materialization,
                            m.database,
                            m.schema,
                        ]
                    )
        else:  # json
            data = [m.to_dict() for m in metrics]
            output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

        logger.info(f"Exported {len(metrics)} metrics to {output_path}")

    def get_stats(self) -> dict[str, t.Any]:
        """Get overall profiler statistics.

        Returns:
            Dictionary with profiler stats
        """
        if self._conn is None:
            return {}

        cursor = self._conn.execute(
            """
            SELECT
                COUNT(DISTINCT model_name) as total_models,
                COUNT(*) as total_samples,
                MIN(timestamp) as first_sample,
                MAX(timestamp) as last_sample,
                AVG(execution_time_ms) as avg_execution_time
            FROM performance_metrics
        """
        )

        row = cursor.fetchone()
        if not row:
            return {
                "total_models": 0,
                "total_samples": 0,
                "first_sample": None,
                "last_sample": None,
                "avg_execution_time_ms": 0,
            }

        return {
            "total_models": row[0],
            "total_samples": row[1],
            "first_sample": row[2],
            "last_sample": row[3],
            "avg_execution_time_ms": row[4],
            "storage_path": str(self._storage_path),
            "slow_threshold_ms": self._slow_threshold_ms,
            "history_days": self._history_days,
        }


__all__ = [
    "PerformanceMetric",
    "PerformanceSummary",
    "OptimizationSuggestion",
    "PerformanceProfiler",
]
