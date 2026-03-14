"""Graph traversal query helpers."""
from __future__ import annotations

from .graph_store import GraphStore
from .models import NodeType


def get_all_datasets(store: GraphStore) -> list[dict]:
    """Return all dataset nodes."""
    return [
        {"id": n.id, "name": n.name, "namespace": n.namespace}
        for n in store.nodes.values()
        if n.node_type == NodeType.DATASET
    ]


def get_all_jobs(store: GraphStore) -> list[dict]:
    """Return all job nodes."""
    return [
        {"id": n.id, "name": n.name, "namespace": n.namespace}
        for n in store.nodes.values()
        if n.node_type == NodeType.JOB
    ]


def shortest_path(store: GraphStore, source: str, target: str) -> list[str] | None:
    """Find the shortest path between two nodes."""
    import networkx as nx
    try:
        return nx.shortest_path(store.graph, source, target)
    except (nx.NodeNotFound, nx.NetworkXNoPath):
        return None
