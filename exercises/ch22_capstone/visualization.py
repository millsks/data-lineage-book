"""Export lineage graphs as Mermaid diagrams."""
from __future__ import annotations

from .graph_store import GraphStore
from .models import NodeType


class MermaidExporter:
    """Generate Mermaid diagrams from the lineage graph."""

    def __init__(self, store: GraphStore):
        self.store = store

    def _safe_id(self, node_id: str) -> str:
        return node_id.replace("/", "_").replace(".", "_").replace("-", "_")

    def _node_shape(self, node_id: str) -> str:
        node = self.store.get_node(node_id)
        safe = self._safe_id(node_id)
        name = node.name if node else node_id
        if node and node.node_type == NodeType.DATASET:
            return f'{safe}[("{name}")]'
        elif node and node.node_type == NodeType.JOB:
            return f'{safe}["{name}"]'
        return f'{safe}["{name}"]'

    def full_graph_mermaid(self) -> str:
        """Export the entire lineage graph as Mermaid."""
        lines = ["graph LR"]
        seen_nodes: set[str] = set()

        for src, dst in self.store.graph.edges():
            if src not in seen_nodes:
                lines.append(f"    {self._node_shape(src)}")
                seen_nodes.add(src)
            if dst not in seen_nodes:
                lines.append(f"    {self._node_shape(dst)}")
                seen_nodes.add(dst)
            lines.append(
                f"    {self._safe_id(src)} --> {self._safe_id(dst)}"
            )

        return "\n".join(lines)

    def subgraph_mermaid(self, root: str, direction: str = "downstream",
                         depth: int = 5) -> str:
        """Export a subgraph rooted at a node."""
        if direction == "downstream":
            nodes = self.store.downstream(root, max_depth=depth)
        else:
            nodes = self.store.upstream(root, max_depth=depth)

        node_ids = {root} | {n.id for n in nodes}
        lines = ["graph LR"]

        for nid in node_ids:
            lines.append(f"    {self._node_shape(nid)}")

        for src, dst in self.store.graph.edges():
            if src in node_ids and dst in node_ids:
                lines.append(
                    f"    {self._safe_id(src)} --> {self._safe_id(dst)}"
                )

        return "\n".join(lines)
