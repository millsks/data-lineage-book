"""Chapter 4 Exercise — Your First Lineage Graph.

Build a data lineage graph using NetworkX, visualise it,
and run upstream/downstream traversals.

Usage:
    pixi run -e exercises python docs/exercises/ch04_first_graph.py
"""
from __future__ import annotations

import networkx as nx


# ── 1. Build a simple lineage graph ────────────────────────────────────
def build_lineage_graph() -> nx.DiGraph:
    """Create a sample lineage DAG."""
    G = nx.DiGraph()

    # Datasets (sources)
    G.add_node("raw_orders", type="dataset", layer="raw")
    G.add_node("raw_customers", type="dataset", layer="raw")

    # Jobs (transformations)
    G.add_node("clean_orders", type="job")
    G.add_node("clean_customers", type="job")
    G.add_node("join_order_customer", type="job")

    # Datasets (outputs)
    G.add_node("stg_orders", type="dataset", layer="staging")
    G.add_node("stg_customers", type="dataset", layer="staging")
    G.add_node("fct_order_summary", type="dataset", layer="mart")

    # Edges: data flows from source → job → target
    G.add_edge("raw_orders", "clean_orders")
    G.add_edge("clean_orders", "stg_orders")
    G.add_edge("raw_customers", "clean_customers")
    G.add_edge("clean_customers", "stg_customers")
    G.add_edge("stg_orders", "join_order_customer")
    G.add_edge("stg_customers", "join_order_customer")
    G.add_edge("join_order_customer", "fct_order_summary")

    return G


# ── 2. Traversal helpers ───────────────────────────────────────────────
def get_upstream(G: nx.DiGraph, node: str) -> list[str]:
    """Return all upstream ancestors of a node."""
    return list(nx.ancestors(G, node))


def get_downstream(G: nx.DiGraph, node: str) -> list[str]:
    """Return all downstream descendants of a node."""
    return list(nx.descendants(G, node))


# ── 3. Validate the graph is a DAG ────────────────────────────────────
def validate_dag(G: nx.DiGraph) -> bool:
    """Check that the lineage graph has no cycles."""
    return nx.is_directed_acyclic_graph(G)


# ── 4. Exercises ──────────────────────────────────────────────────────
def main():
    G = build_lineage_graph()

    print("=== Chapter 4: Your First Lineage Graph ===\n")
    print(f"Nodes: {G.number_of_nodes()}")
    print(f"Edges: {G.number_of_edges()}")
    print(f"Is DAG: {validate_dag(G)}")
    print()

    # Upstream analysis
    target = "fct_order_summary"
    upstream = get_upstream(G, target)
    print(f"Upstream of '{target}':")
    for node in sorted(upstream):
        print(f"  ← {node}")
    print()

    # Downstream analysis
    source = "raw_orders"
    downstream = get_downstream(G, source)
    print(f"Downstream of '{source}':")
    for node in sorted(downstream):
        print(f"  → {node}")
    print()

    # Exercise: Try adding a new reporting dataset
    # Uncomment and complete:
    # G.add_node("rpt_daily_sales", type="dataset", layer="reporting")
    # G.add_edge("fct_order_summary", "???")
    # print(f"Updated downstream of 'raw_orders': {get_downstream(G, 'raw_orders')}")

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
