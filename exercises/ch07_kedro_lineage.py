"""Chapter 7 Exercise — Kedro Lineage.

Build a Kedro-style pipeline with explicit dataset and node
declarations, extract the lineage graph, and query upstream /
downstream dependencies.

Usage:
    pixi run -e exercises python docs/exercises/ch07_kedro_lineage.py
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import networkx as nx


# ── 1. Data Catalog representation ───────────────────────────────────
@dataclass
class CatalogEntry:
    """A single dataset in the Kedro Data Catalog."""

    name: str
    dataset_type: str
    filepath: str
    layer: str = ""


SAMPLE_CATALOG: dict[str, CatalogEntry] = {
    "raw_orders": CatalogEntry(
        name="raw_orders",
        dataset_type="pandas.CSVDataset",
        filepath="data/01_raw/orders.csv",
        layer="raw",
    ),
    "raw_customers": CatalogEntry(
        name="raw_customers",
        dataset_type="pandas.CSVDataset",
        filepath="data/01_raw/customers.csv",
        layer="raw",
    ),
    "cleaned_orders": CatalogEntry(
        name="cleaned_orders",
        dataset_type="pandas.ParquetDataset",
        filepath="data/02_intermediate/cleaned_orders.parquet",
        layer="intermediate",
    ),
    "enriched_orders": CatalogEntry(
        name="enriched_orders",
        dataset_type="pandas.ParquetDataset",
        filepath="data/03_primary/enriched_orders.parquet",
        layer="primary",
    ),
    "order_summary": CatalogEntry(
        name="order_summary",
        dataset_type="pandas.ParquetDataset",
        filepath="data/04_feature/order_summary.parquet",
        layer="feature",
    ),
    "reporting_output": CatalogEntry(
        name="reporting_output",
        dataset_type="pandas.CSVDataset",
        filepath="data/07_model_output/report.csv",
        layer="model_output",
    ),
}


# ── 2. Node and Pipeline representation ──────────────────────────────
@dataclass
class KedroNode:
    """A Kedro pipeline node with explicit inputs/outputs."""

    name: str
    func_name: str
    inputs: list[str]
    outputs: list[str]
    tags: list[str] = field(default_factory=list)


SAMPLE_PIPELINE: list[KedroNode] = [
    KedroNode(
        name="clean_orders_node",
        func_name="clean_raw_orders",
        inputs=["raw_orders"],
        outputs=["cleaned_orders"],
    ),
    KedroNode(
        name="enrich_orders_node",
        func_name="enrich_orders",
        inputs=["cleaned_orders", "raw_customers"],
        outputs=["enriched_orders"],
    ),
    KedroNode(
        name="build_summary_node",
        func_name="build_order_summary",
        inputs=["enriched_orders"],
        outputs=["order_summary"],
    ),
    KedroNode(
        name="generate_report_node",
        func_name="generate_report",
        inputs=["order_summary", "enriched_orders"],
        outputs=["reporting_output"],
    ),
]


# ── 3. Build lineage graph ───────────────────────────────────────────
def build_lineage_graph(
    catalog: dict[str, CatalogEntry],
    pipeline: list[KedroNode],
) -> nx.DiGraph:
    """Build a directed graph representing the Kedro pipeline lineage.

    Nodes in the graph represent both datasets (from the catalog) and
    pipeline nodes (transformations).  Edges represent data flow:
    dataset → node (input) and node → dataset (output).
    """
    G = nx.DiGraph()

    # TODO: Add dataset nodes from the catalog with attributes
    #       (node_type="dataset", dataset_type, filepath, layer)
    #
    # HINT: for name, entry in catalog.items():
    #           G.add_node(name, node_type="dataset", ...)

    # TODO: Add pipeline nodes and edges for inputs/outputs
    #       (node_type="task", func_name attribute)
    #       Edge from each input dataset → node (data consumed)
    #       Edge from node → each output dataset (data produced)
    #
    # HINT: for knode in pipeline:
    #           G.add_node(knode.name, node_type="task", func_name=knode.func_name)
    #           for inp in knode.inputs:
    #               G.add_edge(inp, knode.name)
    #           for out in knode.outputs:
    #               G.add_edge(knode.name, out)

    return G


# ── 4. Lineage queries ──────────────────────────────────────────────
def get_upstream(G: nx.DiGraph, node_name: str) -> set[str]:
    """Return all upstream ancestors of a node/dataset in the lineage graph.

    This answers: "What does this dataset/node depend on?"
    """
    # TODO: Use nx.ancestors() to find all upstream nodes
    # HINT: return nx.ancestors(G, node_name)
    return set()


def get_downstream(G: nx.DiGraph, node_name: str) -> set[str]:
    """Return all downstream descendants of a node/dataset in the lineage graph.

    This answers: "What is affected if this dataset/node changes?"
    """
    # TODO: Use nx.descendants() to find all downstream nodes
    # HINT: return nx.descendants(G, node_name)
    return set()


def get_free_inputs(G: nx.DiGraph) -> list[str]:
    """Return datasets that have no upstream producers (source datasets)."""
    # TODO: Find all nodes with node_type="dataset" and in_degree == 0
    # HINT: [n for n, d in G.nodes(data=True)
    #        if d.get("node_type") == "dataset" and G.in_degree(n) == 0]
    return []


def get_terminal_outputs(G: nx.DiGraph) -> list[str]:
    """Return datasets that have no downstream consumers (final outputs)."""
    # TODO: Find all nodes with node_type="dataset" and out_degree == 0
    # HINT: [n for n, d in G.nodes(data=True)
    #        if d.get("node_type") == "dataset" and G.out_degree(n) == 0]
    return []


# ── 5. Export lineage as JSON ────────────────────────────────────────
def export_lineage_json(
    G: nx.DiGraph,
    catalog: dict[str, CatalogEntry],
) -> dict:
    """Export the lineage graph as a JSON document compatible with Kedro-Viz.

    Returns a dict with "nodes" and "edges" keys.
    """
    nodes = []
    edges = []

    # TODO: Build the nodes list from G.nodes(data=True)
    #       For datasets: include id, type="data", dataset_type, filepath, layer
    #       For tasks: include id, type="task", func_name
    #
    # HINT: for node_id, attrs in G.nodes(data=True):
    #           if attrs.get("node_type") == "dataset":
    #               entry = catalog.get(node_id)
    #               nodes.append({"id": node_id, "type": "data", ...})
    #           else:
    #               nodes.append({"id": node_id, "type": "task", ...})

    # TODO: Build the edges list from G.edges()
    # HINT: for src, tgt in G.edges():
    #           edges.append({"source": src, "target": tgt})

    return {"nodes": nodes, "edges": edges}


# ── 6. Column-level lineage (BONUS) ─────────────────────────────────
COLUMN_LINEAGE: dict[str, dict[str, list[tuple[str, str]]]] = {
    "clean_orders_node": {
        "cleaned_orders.order_id": [("raw_orders", "order_id")],
        "cleaned_orders.customer_id": [("raw_orders", "customer_id")],
        "cleaned_orders.amount": [("raw_orders", "amount")],
    },
    "enrich_orders_node": {
        "enriched_orders.order_id": [("cleaned_orders", "order_id")],
        "enriched_orders.customer_id": [
            ("cleaned_orders", "customer_id"),
            ("raw_customers", "customer_id"),
        ],
        "enriched_orders.amount": [("cleaned_orders", "amount")],
        "enriched_orders.customer_name": [("raw_customers", "name")],
    },
}


def trace_column_provenance(
    column_lineage: dict[str, dict[str, list[tuple[str, str]]]],
    target_dataset: str,
    target_column: str,
) -> list[tuple[str, str]]:
    """Trace a column back to its source(s) across nodes.

    Returns a list of (dataset, column) tuples representing all sources.
    """
    # TODO: Recursively trace column origins through COLUMN_LINEAGE
    #       Start from target_dataset.target_column, look up which node
    #       produces it, find its source columns, and recurse.
    #
    # HINT: key = f"{target_dataset}.{target_column}"
    #       Search each node's column_lineage for this key
    #       For each source (ds, col), recurse if ds is also a target
    #       Base case: no entry found → it's a source column
    return []


# ── Main ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Chapter 7 Exercise — Kedro Lineage")
    print("=" * 60)

    # 1. Catalog inventory
    print("\n── Data Catalog ─────────────────────────────────")
    for name, entry in SAMPLE_CATALOG.items():
        print(f"  {name}: {entry.dataset_type} @ {entry.filepath} [{entry.layer}]")

    # 2. Pipeline nodes
    print("\n── Pipeline Nodes ───────────────────────────────")
    for node in SAMPLE_PIPELINE:
        print(f"  {node.name}: {node.inputs} → {node.outputs}")

    # 3. Build lineage graph
    print("\n── Lineage Graph ────────────────────────────────")
    G = build_lineage_graph(SAMPLE_CATALOG, SAMPLE_PIPELINE)
    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    if G.number_of_nodes() == 0:
        print("  ⚠ Graph is empty — implement build_lineage_graph() above!")
    else:
        # 4. Lineage queries
        print("\n── Upstream of 'order_summary' ──────────────────")
        upstream = get_upstream(G, "order_summary")
        print(f"  {upstream or '⚠ Implement get_upstream()'}")

        print("\n── Downstream of 'raw_customers' ───────────────")
        downstream = get_downstream(G, "raw_customers")
        print(f"  {downstream or '⚠ Implement get_downstream()'}")

        print("\n── Source datasets (free inputs) ────────────────")
        free = get_free_inputs(G)
        print(f"  {free or '⚠ Implement get_free_inputs()'}")

        print("\n── Terminal outputs ─────────────────────────────")
        terminal = get_terminal_outputs(G)
        print(f"  {terminal or '⚠ Implement get_terminal_outputs()'}")

        # 5. Export JSON
        print("\n── Lineage JSON Export ──────────────────────────")
        lineage_json = export_lineage_json(G, SAMPLE_CATALOG)
        if lineage_json.get("nodes"):
            print(json.dumps(lineage_json, indent=2)[:500] + "\n  ...")
        else:
            print("  ⚠ Implement export_lineage_json()")

    # 6. Column lineage (BONUS)
    print("\n── Column Provenance (BONUS) ────────────────────")
    sources = trace_column_provenance(COLUMN_LINEAGE, "enriched_orders", "customer_name")
    if sources:
        print(f"  enriched_orders.customer_name ← {sources}")
    else:
        print("  ⚠ Implement trace_column_provenance()")

    print("\n✅ Exercise complete — check your implementations above!")
