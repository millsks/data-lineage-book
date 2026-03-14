"""Chapter 13 Exercise — Data Quality and Lineage.

Combine data quality checks with lineage propagation.

Usage:
    pixi run -e exercises python docs/exercises/ch13_quality_lineage.py
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime

import networkx as nx


@dataclass
class QualityCheck:
    """Result of a quality check on a dataset."""
    dataset: str
    check_name: str
    passed: bool
    value: float = 0.0
    threshold: float = 0.0
    checked_at: datetime = field(default_factory=datetime.now)


class QualityAwareLineage:
    """Lineage graph with quality propagation."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.quality: dict[str, list[QualityCheck]] = {}

    def add_dataset(self, name: str, **attrs):
        self.graph.add_node(name, type="dataset", **attrs)

    def add_job(self, name: str, inputs: list[str], outputs: list[str]):
        self.graph.add_node(name, type="job")
        for inp in inputs:
            self.graph.add_edge(inp, name)
        for out in outputs:
            self.graph.add_edge(name, out)

    def record_check(self, check: QualityCheck):
        self.quality.setdefault(check.dataset, []).append(check)

    def dataset_health(self, dataset: str) -> str:
        """Return health status for a dataset."""
        checks = self.quality.get(dataset, [])
        if not checks:
            return "UNKNOWN"
        recent = sorted(checks, key=lambda c: c.checked_at)[-5:]
        if all(c.passed for c in recent):
            return "HEALTHY"
        if any(not c.passed for c in recent[-2:]):
            return "DEGRADED"
        return "WARNING"

    def propagate_failure(self, failed_dataset: str) -> list[str]:
        """Find all downstream datasets affected by a quality failure."""
        affected = []
        for desc in nx.descendants(self.graph, failed_dataset):
            if self.graph.nodes[desc].get("type") == "dataset":
                affected.append(desc)
        return affected

    def is_anomaly(self, dataset: str, metric_values: list[float],
                   current: float, z_threshold: float = 2.5) -> bool:
        """Detect anomalies using z-score."""
        if len(metric_values) < 5:
            return False
        mean = statistics.mean(metric_values)
        stdev = statistics.stdev(metric_values)
        if stdev == 0:
            return False
        z = abs(current - mean) / stdev
        return z > z_threshold


def main():
    print("=== Chapter 13: Data Quality and Lineage ===\n")

    lineage = QualityAwareLineage()

    # Build pipeline
    lineage.add_dataset("raw_orders")
    lineage.add_dataset("raw_customers")
    lineage.add_job("clean", ["raw_orders"], ["stg_orders"])
    lineage.add_dataset("stg_orders")
    lineage.add_job("enrich", ["stg_orders", "raw_customers"], ["fct_orders"])
    lineage.add_dataset("fct_orders")
    lineage.add_dataset("raw_customers")
    lineage.add_job("report", ["fct_orders"], ["rpt_revenue"])
    lineage.add_dataset("rpt_revenue")

    # Record quality checks
    lineage.record_check(QualityCheck("raw_orders", "not_null", True))
    lineage.record_check(QualityCheck("raw_orders", "row_count", True, 10000, 100))
    lineage.record_check(QualityCheck("stg_orders", "not_null", False, 0.15, 0.05))
    lineage.record_check(QualityCheck("fct_orders", "freshness", True, 2.0, 24.0))

    # Check health
    print("Dataset health:")
    for node, data in lineage.graph.nodes(data=True):
        if data.get("type") == "dataset":
            health = lineage.dataset_health(node)
            print(f"  {node}: {health}")
    print()

    # Propagate failure
    print("Impact of stg_orders quality failure:")
    affected = lineage.propagate_failure("stg_orders")
    for ds in affected:
        print(f"  ⚠ {ds}")
    print()

    # Anomaly detection
    historical = [10000, 10200, 9800, 10100, 10050, 9900, 10150]
    current_value = 3000
    is_anom = lineage.is_anomaly("raw_orders", historical, current_value)
    print(f"Row count {current_value} anomalous? {is_anom}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
