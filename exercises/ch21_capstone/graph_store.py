"""In-memory lineage graph using NetworkX."""
from __future__ import annotations

from collections import deque

import networkx as nx

from .models import LineageEdge, LineageNode, NodeType


class GraphStore:
    """In-memory lineage graph store."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[LineageEdge] = []

    def upsert_node(self, node: LineageNode):
        """Add or update a node in the graph."""
        self.nodes[node.id] = node
        self.graph.add_node(
            node.id,
            name=node.name,
            namespace=node.namespace,
            type=node.node_type.value,
            classification=node.classification.value,
        )

    def add_edge(self, edge: LineageEdge):
        """Add an edge to the graph."""
        self.edges.append(edge)
        self.graph.add_edge(
            edge.source_id,
            edge.target_id,
            type=edge.edge_type,
        )

    def get_node(self, node_id: str) -> LineageNode | None:
        return self.nodes.get(node_id)

    def upstream(self, node_id: str, max_depth: int = 10) -> list[LineageNode]:
        """BFS upstream traversal."""
        if node_id not in self.graph:
            return []

        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(node_id, 0)])
        result: list[LineageNode] = []

        while queue:
            current, depth = queue.popleft()
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            if current != node_id and current in self.nodes:
                result.append(self.nodes[current])

            for pred in self.graph.predecessors(current):
                if pred not in visited:
                    queue.append((pred, depth + 1))

        return result

    def downstream(self, node_id: str, max_depth: int = 10) -> list[LineageNode]:
        """BFS downstream traversal."""
        if node_id not in self.graph:
            return []

        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(node_id, 0)])
        result: list[LineageNode] = []

        while queue:
            current, depth = queue.popleft()
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            if current != node_id and current in self.nodes:
                result.append(self.nodes[current])

            for succ in self.graph.successors(current):
                if succ not in visited:
                    queue.append((succ, depth + 1))

        return result

    def impact_analysis(self, node_id: str) -> dict:
        """Analyze the downstream impact of a node."""
        downstream_nodes = self.downstream(node_id)
        datasets = [n for n in downstream_nodes if n.node_type == NodeType.DATASET]
        jobs = [n for n in downstream_nodes if n.node_type == NodeType.JOB]

        return {
            "source": node_id,
            "total_downstream": len(downstream_nodes),
            "affected_datasets": len(datasets),
            "affected_jobs": len(jobs),
            "datasets": [{"id": d.id, "name": d.name} for d in datasets],
            "jobs": [{"id": j.id, "name": j.name} for j in jobs],
        }

    def search(self, query: str) -> list[LineageNode]:
        """Search nodes by name or namespace."""
        q = query.lower()
        return [
            node for node in self.nodes.values()
            if q in node.name.lower() or q in node.namespace.lower()
        ]

    def stats(self) -> dict:
        """Graph statistics."""
        return {
            "total_nodes": self.graph.number_of_nodes(),
            "total_edges": self.graph.number_of_edges(),
            "datasets": sum(
                1 for n in self.nodes.values() if n.node_type == NodeType.DATASET
            ),
            "jobs": sum(
                1 for n in self.nodes.values() if n.node_type == NodeType.JOB
            ),
        }
