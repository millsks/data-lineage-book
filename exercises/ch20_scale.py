"""Chapter 20 Exercise — Lineage at Scale.

Demonstrate patterns for handling large lineage graphs:
bounded traversal, batch ingestion, and tiered storage.

Usage:
    pixi run -e exercises python docs/exercises/ch20_scale.py
"""
from __future__ import annotations

import time
import random
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

import networkx as nx


# ── 1. Bounded BFS traversal ─────────────────────────────────────────
def bounded_bfs(G: nx.DiGraph, start: str, max_depth: int = 5,
                max_nodes: int = 100) -> list[str]:
    """BFS with depth and node count limits."""
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(start, 0)])
    result: list[str] = []

    while queue and len(result) < max_nodes:
        node, depth = queue.popleft()
        if node in visited or depth > max_depth:
            continue
        visited.add(node)
        if node != start:
            result.append(node)
        for neighbor in G.successors(node):
            if neighbor not in visited:
                queue.append((neighbor, depth + 1))

    return result


# ── 2. Batch event ingestion ─────────────────────────────────────────
@dataclass
class IngestionStats:
    events_received: int = 0
    events_processed: int = 0
    duplicates_skipped: int = 0
    duration_ms: float = 0.0


class BatchIngestionWorker:
    """Micro-batch ingestion with deduplication."""

    def __init__(self, graph: nx.DiGraph, batch_size: int = 100):
        self.graph = graph
        self.batch_size = batch_size
        self.seen_events: set[str] = set()

    def process_batch(self, events: list[dict]) -> IngestionStats:
        stats = IngestionStats(events_received=len(events))
        start = time.monotonic()

        for event in events:
            event_key = f"{event.get('job', '')}-{event.get('run_id', '')}"
            if event_key in self.seen_events:
                stats.duplicates_skipped += 1
                continue
            self.seen_events.add(event_key)

            # Add to graph
            job = event.get("job", "unknown")
            self.graph.add_node(job, type="job")
            for inp in event.get("inputs", []):
                self.graph.add_node(inp, type="dataset")
                self.graph.add_edge(inp, job)
            for out in event.get("outputs", []):
                self.graph.add_node(out, type="dataset")
                self.graph.add_edge(job, out)

            stats.events_processed += 1

        stats.duration_ms = (time.monotonic() - start) * 1000
        return stats


# ── 3. Generate a large graph for benchmarking ───────────────────────
def generate_large_graph(num_layers: int = 5,
                         nodes_per_layer: int = 50) -> nx.DiGraph:
    """Generate a layered DAG for scale testing."""
    G = nx.DiGraph()
    prev_layer: list[str] = []

    for layer in range(num_layers):
        current_layer = []
        for i in range(nodes_per_layer):
            node = f"L{layer}_N{i}"
            ntype = "dataset" if layer % 2 == 0 else "job"
            G.add_node(node, type=ntype, layer=layer)
            current_layer.append(node)

            # Connect to random nodes in previous layer
            if prev_layer:
                num_edges = random.randint(1, min(3, len(prev_layer)))
                for parent in random.sample(prev_layer, num_edges):
                    G.add_edge(parent, node)

        prev_layer = current_layer

    return G


def benchmark_traversal(G: nx.DiGraph):
    """Benchmark different traversal strategies."""
    start_node = list(G.nodes())[0]

    # Unbounded
    t0 = time.monotonic()
    full = list(nx.descendants(G, start_node))
    t_full = (time.monotonic() - t0) * 1000

    # Bounded
    t0 = time.monotonic()
    bounded = bounded_bfs(G, start_node, max_depth=3, max_nodes=50)
    t_bounded = (time.monotonic() - t0) * 1000

    return {
        "full_traversal": {"count": len(full), "ms": round(t_full, 2)},
        "bounded_traversal": {"count": len(bounded), "ms": round(t_bounded, 2)},
    }


def main():
    print("=== Chapter 20: Lineage at Scale ===\n")

    # Generate large graph
    print("Generating large graph ...")
    G = generate_large_graph(num_layers=8, nodes_per_layer=100)
    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    # Benchmark traversals
    print("Traversal benchmark:")
    results = benchmark_traversal(G)
    for name, stats in results.items():
        print(f"  {name}: {stats['count']} nodes in {stats['ms']}ms")
    print()

    # Batch ingestion
    print("Batch ingestion demo:")
    ingestion_graph = nx.DiGraph()
    worker = BatchIngestionWorker(ingestion_graph, batch_size=50)

    events = []
    for i in range(200):
        events.append({
            "job": f"job_{i % 20}",
            "run_id": f"run_{i}",
            "inputs": [f"dataset_{i % 10}"],
            "outputs": [f"dataset_{(i % 10) + 10}"],
        })
    # Add some duplicates
    events.extend(events[:20])

    stats = worker.process_batch(events)
    print(f"  Received: {stats.events_received}")
    print(f"  Processed: {stats.events_processed}")
    print(f"  Duplicates skipped: {stats.duplicates_skipped}")
    print(f"  Duration: {stats.duration_ms:.1f}ms")
    print(f"  Graph: {ingestion_graph.number_of_nodes()} nodes, "
          f"{ingestion_graph.number_of_edges()} edges")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
