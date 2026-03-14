"""Chapter 15 Exercise — Streaming Lineage.

Model Kafka topic topologies as lineage and track real-time
data flow through stream processors.

Usage:
    pixi run -e exercises python docs/exercises/ch15_streaming_lineage.py
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import networkx as nx


@dataclass
class KafkaTopic:
    name: str
    partitions: int = 3
    retention_hours: int = 168


@dataclass
class StreamProcessor:
    name: str
    group_id: str
    input_topics: list[str] = field(default_factory=list)
    output_topics: list[str] = field(default_factory=list)


class StreamingLineageTracker:
    """Track lineage through a streaming topology."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.topics: dict[str, KafkaTopic] = {}
        self.processors: dict[str, StreamProcessor] = {}

    def add_topic(self, topic: KafkaTopic):
        self.topics[topic.name] = topic
        self.graph.add_node(topic.name, type="topic",
                            partitions=topic.partitions)

    def add_processor(self, proc: StreamProcessor):
        self.processors[proc.name] = proc
        self.graph.add_node(proc.name, type="processor",
                            group_id=proc.group_id)
        for inp in proc.input_topics:
            self.graph.add_edge(inp, proc.name)
        for out in proc.output_topics:
            self.graph.add_edge(proc.name, out)

    def trace_data_flow(self, topic: str) -> list[str]:
        """Trace downstream flow from a topic."""
        return list(nx.descendants(self.graph, topic))

    def trace_sources(self, topic: str) -> list[str]:
        """Trace upstream sources of a topic."""
        return list(nx.ancestors(self.graph, topic))

    def export_mermaid(self) -> str:
        """Export topology as Mermaid diagram."""
        lines = ["graph LR"]
        for node, data in self.graph.nodes(data=True):
            safe = node.replace("-", "_").replace(".", "_")
            if data.get("type") == "topic":
                lines.append(f'    {safe}[("{node}")]')
            else:
                lines.append(f'    {safe}["{node}"]')

        for src, tgt in self.graph.edges():
            s = src.replace("-", "_").replace(".", "_")
            t = tgt.replace("-", "_").replace(".", "_")
            lines.append(f"    {s} --> {t}")

        return "\n".join(lines)


def main():
    print("=== Chapter 15: Streaming Lineage ===\n")

    tracker = StreamingLineageTracker()

    # Define topics
    topics = [
        KafkaTopic("raw-events", partitions=12),
        KafkaTopic("validated-events", partitions=6),
        KafkaTopic("enriched-events", partitions=6),
        KafkaTopic("aggregated-metrics", partitions=3),
        KafkaTopic("alerts", partitions=1),
    ]
    for t in topics:
        tracker.add_topic(t)

    # Define stream processors
    processors = [
        StreamProcessor("validator", "validator-group",
                        ["raw-events"], ["validated-events"]),
        StreamProcessor("enricher", "enricher-group",
                        ["validated-events"], ["enriched-events"]),
        StreamProcessor("aggregator", "agg-group",
                        ["enriched-events"], ["aggregated-metrics"]),
        StreamProcessor("alert-engine", "alert-group",
                        ["aggregated-metrics"], ["alerts"]),
    ]
    for p in processors:
        tracker.add_processor(p)

    print(f"Topology: {tracker.graph.number_of_nodes()} nodes, "
          f"{tracker.graph.number_of_edges()} edges\n")

    # Trace data flow
    print("Downstream of 'raw-events':")
    for node in tracker.trace_data_flow("raw-events"):
        ntype = tracker.graph.nodes[node].get("type", "?")
        print(f"  → [{ntype}] {node}")
    print()

    # Trace sources
    print("Upstream of 'alerts':")
    for node in tracker.trace_sources("alerts"):
        ntype = tracker.graph.nodes[node].get("type", "?")
        print(f"  ← [{ntype}] {node}")
    print()

    # Mermaid export
    print("Mermaid diagram:")
    print(tracker.export_mermaid())
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
