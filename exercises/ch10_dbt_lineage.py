"""Chapter 10 Exercise — dbt Lineage.

Parse dbt manifest.json to extract lineage, and build a graph
of model dependencies.

Usage:
    pixi run -e exercises python docs/exercises/ch10_dbt_lineage.py
"""
from __future__ import annotations

import json
import networkx as nx


# ── 1. Simulated dbt manifest ────────────────────────────────────────
SAMPLE_MANIFEST = {
    "nodes": {
        "model.project.stg_orders": {
            "unique_id": "model.project.stg_orders",
            "resource_type": "model",
            "name": "stg_orders",
            "schema": "staging",
            "depends_on": {
                "nodes": ["source.project.raw.orders"],
            },
            "columns": {
                "order_id": {"name": "order_id", "data_type": "integer"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "customer_id": {"name": "customer_id", "data_type": "integer"},
            },
        },
        "model.project.stg_customers": {
            "unique_id": "model.project.stg_customers",
            "resource_type": "model",
            "name": "stg_customers",
            "schema": "staging",
            "depends_on": {
                "nodes": ["source.project.raw.customers"],
            },
            "columns": {
                "customer_id": {"name": "customer_id", "data_type": "integer"},
                "name": {"name": "name", "data_type": "varchar"},
            },
        },
        "model.project.fct_orders": {
            "unique_id": "model.project.fct_orders",
            "resource_type": "model",
            "name": "fct_orders",
            "schema": "marts",
            "depends_on": {
                "nodes": [
                    "model.project.stg_orders",
                    "model.project.stg_customers",
                ],
            },
            "columns": {
                "order_id": {"name": "order_id", "data_type": "integer"},
                "customer_name": {"name": "customer_name", "data_type": "varchar"},
                "amount": {"name": "amount", "data_type": "numeric"},
            },
        },
    },
    "sources": {
        "source.project.raw.orders": {
            "unique_id": "source.project.raw.orders",
            "resource_type": "source",
            "name": "orders",
            "schema": "raw",
            "source_name": "raw",
        },
        "source.project.raw.customers": {
            "unique_id": "source.project.raw.customers",
            "resource_type": "source",
            "name": "customers",
            "schema": "raw",
            "source_name": "raw",
        },
    },
}


# ── 2. Parse manifest into a lineage graph ────────────────────────────
def manifest_to_graph(manifest: dict) -> nx.DiGraph:
    """Build a NetworkX graph from a dbt manifest."""
    G = nx.DiGraph()

    # Add source nodes
    for source_id, source in manifest.get("sources", {}).items():
        G.add_node(
            source_id,
            name=source["name"],
            schema=source["schema"],
            resource_type="source",
        )

    # Add model nodes and edges
    for node_id, node in manifest.get("nodes", {}).items():
        if node["resource_type"] != "model":
            continue
        G.add_node(
            node_id,
            name=node["name"],
            schema=node["schema"],
            resource_type="model",
            columns=list(node.get("columns", {}).keys()),
        )
        for dep in node.get("depends_on", {}).get("nodes", []):
            G.add_edge(dep, node_id)

    return G


# ── 3. Lineage queries ───────────────────────────────────────────────
def get_model_upstream(G: nx.DiGraph, model_id: str) -> list[str]:
    """Get all upstream models/sources for a model."""
    return list(nx.ancestors(G, model_id))


def get_model_downstream(G: nx.DiGraph, model_id: str) -> list[str]:
    """Get all downstream models for a model."""
    return list(nx.descendants(G, model_id))


# ── 4. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 9: dbt Lineage ===\n")

    G = manifest_to_graph(SAMPLE_MANIFEST)

    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    print("All nodes:")
    for node_id, data in G.nodes(data=True):
        print(f"  [{data.get('resource_type', '?')}] {data.get('name', node_id)}"
              f" (schema: {data.get('schema', '?')})")
    print()

    print("Edges (dependencies):")
    for src, tgt in G.edges():
        src_name = G.nodes[src].get("name", src)
        tgt_name = G.nodes[tgt].get("name", tgt)
        print(f"  {src_name} → {tgt_name}")
    print()

    target = "model.project.fct_orders"
    upstream = get_model_upstream(G, target)
    print(f"Upstream of fct_orders:")
    for u in upstream:
        print(f"  ← {G.nodes[u].get('name', u)}")
    print()

    source = "source.project.raw.orders"
    downstream = get_model_downstream(G, source)
    print(f"Downstream of raw.orders:")
    for d in downstream:
        print(f"  → {G.nodes[d].get('name', d)}")
    print()

    # Exercise: Add a dim_customers model that depends on stg_customers
    # Exercise: Add exposures (dashboards) that depend on fct_orders

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
