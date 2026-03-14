"""Chapter 11 Exercise — Graph Databases for Lineage (Neo4j).

Store and query lineage in a graph database using Neo4j.
Falls back to NetworkX simulation if Neo4j is unavailable.

Usage:
    pixi run -e exercises python docs/exercises/ch11_neo4j_lineage.py
"""
from __future__ import annotations

import networkx as nx


# ── 1. Neo4j example (requires running Neo4j instance) ───────────────
def neo4j_example():
    """Demonstrate Neo4j lineage storage."""
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("  ⚠ neo4j driver not installed — skipping Neo4j example")
        return False

    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "lineage_dev"

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            # Create lineage nodes and edges
            session.run("""
                MERGE (raw:Dataset {name: 'raw_orders', namespace: 'datalake'})
                MERGE (stg:Dataset {name: 'stg_orders', namespace: 'warehouse'})
                MERGE (job:Job {name: 'transform_orders'})
                MERGE (raw)-[:CONSUMED_BY]->(job)
                MERGE (job)-[:PRODUCES]->(stg)
            """)

            # Query upstream lineage
            result = session.run("""
                MATCH path = (upstream)-[:CONSUMED_BY|PRODUCES*]->(target:Dataset {name: 'stg_orders'})
                RETURN [n IN nodes(path) | n.name] AS lineage
            """)
            for record in result:
                print(f"  Lineage path: {record['lineage']}")

        driver.close()
        return True

    except Exception as e:
        print(f"  ⚠ Neo4j not reachable: {e}")
        return False


# ── 2. NetworkX simulation (always works) ─────────────────────────────
def networkx_graph_db_simulation():
    """Simulate graph database queries with NetworkX."""
    G = nx.DiGraph()

    # Add nodes with properties (like Neo4j node properties)
    G.add_node("raw_orders", label="Dataset", namespace="datalake")
    G.add_node("raw_customers", label="Dataset", namespace="datalake")
    G.add_node("transform_orders", label="Job")
    G.add_node("transform_customers", label="Job")
    G.add_node("stg_orders", label="Dataset", namespace="warehouse")
    G.add_node("stg_customers", label="Dataset", namespace="warehouse")
    G.add_node("build_facts", label="Job")
    G.add_node("fct_orders", label="Dataset", namespace="warehouse")

    # Edges with properties (like Neo4j relationship properties)
    G.add_edge("raw_orders", "transform_orders", type="CONSUMED_BY")
    G.add_edge("transform_orders", "stg_orders", type="PRODUCES")
    G.add_edge("raw_customers", "transform_customers", type="CONSUMED_BY")
    G.add_edge("transform_customers", "stg_customers", type="PRODUCES")
    G.add_edge("stg_orders", "build_facts", type="CONSUMED_BY")
    G.add_edge("stg_customers", "build_facts", type="CONSUMED_BY")
    G.add_edge("build_facts", "fct_orders", type="PRODUCES")

    return G


def cypher_like_query(G: nx.DiGraph, target: str, direction: str = "upstream") -> list[list[str]]:
    """Simulate a Cypher path query using NetworkX."""
    paths = []
    if direction == "upstream":
        for pred in nx.ancestors(G, target):
            for path in nx.all_simple_paths(G, pred, target):
                paths.append(path)
    else:
        for succ in nx.descendants(G, target):
            for path in nx.all_simple_paths(G, target, succ):
                paths.append(path)
    return paths


# ── 3. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 11: Graph Databases for Lineage ===\n")

    # Try Neo4j first
    print("Neo4j example:")
    if not neo4j_example():
        print("  → Using NetworkX simulation instead")
    print()

    # NetworkX simulation
    G = networkx_graph_db_simulation()
    print(f"Simulated graph DB: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    # Upstream query (like Cypher MATCH path)
    target = "fct_orders"
    print(f"Upstream paths to {target}:")
    paths = cypher_like_query(G, target, "upstream")
    for path in paths:
        node_names = " → ".join(path)
        print(f"  {node_names}")
    print()

    # Downstream query
    source = "raw_orders"
    print(f"Downstream paths from {source}:")
    paths = cypher_like_query(G, source, "downstream")
    for path in paths:
        node_names = " → ".join(path)
        print(f"  {node_names}")
    print()

    # Shortest path
    shortest = nx.shortest_path(G, "raw_orders", "fct_orders")
    print(f"Shortest path raw_orders → fct_orders: {' → '.join(shortest)}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
