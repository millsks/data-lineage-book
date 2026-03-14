"""Tests for the capstone lineage platform."""
from __future__ import annotations

import pytest

from .models import LineageNode, LineageEdge, NodeType, QualityMetrics, DataSLA
from .graph_store import GraphStore
from .quality import QualityMonitor
from .governance import PIIScanner


# --- Unit Tests ---
class TestGraphStore:
    def setup_method(self):
        self.store = GraphStore()

    def test_upsert_and_retrieve(self):
        node = LineageNode("ds/orders", "orders", "warehouse", NodeType.DATASET)
        self.store.upsert_node(node)
        assert self.store.get_node("ds/orders") is not None

    def test_upstream_downstream(self):
        self.store.upsert_node(LineageNode("a", "a", "ns", NodeType.DATASET))
        self.store.upsert_node(LineageNode("b", "b", "ns", NodeType.JOB))
        self.store.upsert_node(LineageNode("c", "c", "ns", NodeType.DATASET))

        self.store.add_edge(LineageEdge("a", "b"))
        self.store.add_edge(LineageEdge("b", "c"))

        upstream = self.store.upstream("c")
        assert len(upstream) == 2

        downstream = self.store.downstream("a")
        assert len(downstream) == 2

    def test_impact_analysis(self):
        self.store.upsert_node(LineageNode("a", "a", "ns", NodeType.DATASET))
        self.store.upsert_node(LineageNode("b", "b", "ns", NodeType.JOB))
        self.store.upsert_node(LineageNode("c", "c", "ns", NodeType.DATASET))
        self.store.upsert_node(LineageNode("d", "d", "ns", NodeType.DATASET))

        self.store.add_edge(LineageEdge("a", "b"))
        self.store.add_edge(LineageEdge("b", "c"))
        self.store.add_edge(LineageEdge("b", "d"))

        impact = self.store.impact_analysis("a")
        assert impact["affected_datasets"] == 2

    def test_search(self):
        self.store.upsert_node(
            LineageNode("a", "raw_orders", "warehouse", NodeType.DATASET)
        )
        self.store.upsert_node(
            LineageNode("b", "stg_orders", "warehouse", NodeType.DATASET)
        )
        results = self.store.search("orders")
        assert len(results) == 2

    def test_stats(self):
        self.store.upsert_node(LineageNode("a", "a", "ns", NodeType.DATASET))
        self.store.upsert_node(LineageNode("b", "b", "ns", NodeType.JOB))
        stats = self.store.stats()
        assert stats["total_nodes"] == 2
        assert stats["datasets"] == 1
        assert stats["jobs"] == 1


class TestQualityMonitor:
    def test_sla_violation(self):
        monitor = QualityMonitor()
        monitor.register_sla(DataSLA("ds/orders", freshness_max_hours=6))
        metrics = QualityMetrics("ds/orders", freshness_hours=12)
        violations = monitor.check_sla("ds/orders", metrics)
        assert len(violations) == 1
        assert "Freshness" in violations[0]

    def test_sla_pass(self):
        monitor = QualityMonitor()
        monitor.register_sla(DataSLA("ds/orders", freshness_max_hours=24))
        metrics = QualityMetrics("ds/orders", freshness_hours=6)
        violations = monitor.check_sla("ds/orders", metrics)
        assert len(violations) == 0


class TestPIIScanner:
    def test_detect_pii(self):
        scanner = PIIScanner()
        findings = scanner.scan_columns(
            "ds/customers",
            ["customer_id", "email", "first_name", "order_date"],
        )
        pii_types = {f.pii_type for f in findings}
        assert "EMAIL" in pii_types
        assert "NAME" in pii_types

    def test_no_pii(self):
        scanner = PIIScanner()
        findings = scanner.scan_columns(
            "ds/metrics",
            ["metric_id", "value", "timestamp"],
        )
        assert len(findings) == 0
