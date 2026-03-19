"""Chapter 13 Exercise — Lineage REST API with FastAPI.

Build a simple lineage API server using FastAPI.

Usage:
    # Run the server
    pixi run -e exercises uvicorn docs.exercises.ch13_lineage_api:app --reload --port 8001

    # Run the client tests
    pixi run -e exercises python docs/exercises/ch13_lineage_api.py
"""
from __future__ import annotations

import networkx as nx
from collections import deque

# ── FastAPI app ───────────────────────────────────────────────────────
try:
    from fastapi import FastAPI, HTTPException, Query
    from pydantic import BaseModel

    app = FastAPI(title="Lineage API Exercise", version="0.1.0")

    # In-memory graph
    _graph = nx.DiGraph()

    class AddNodeRequest(BaseModel):
        id: str
        name: str
        node_type: str = "dataset"

    class AddEdgeRequest(BaseModel):
        source: str
        target: str

    @app.get("/health")
    async def health():
        return {
            "status": "healthy",
            "nodes": _graph.number_of_nodes(),
            "edges": _graph.number_of_edges(),
        }

    @app.post("/nodes")
    async def add_node(req: AddNodeRequest):
        _graph.add_node(req.id, name=req.name, type=req.node_type)
        return {"status": "created", "id": req.id}

    @app.post("/edges")
    async def add_edge(req: AddEdgeRequest):
        if req.source not in _graph or req.target not in _graph:
            raise HTTPException(400, "Source or target node not found")
        _graph.add_edge(req.source, req.target)
        return {"status": "created", "edge": f"{req.source} → {req.target}"}

    @app.get("/upstream/{node_id}")
    async def upstream(node_id: str, depth: int = Query(default=10)):
        if node_id not in _graph:
            raise HTTPException(404, f"Node {node_id} not found")
        ancestors = list(nx.ancestors(_graph, node_id))
        return {"root": node_id, "upstream": ancestors}

    @app.get("/downstream/{node_id}")
    async def downstream(node_id: str, depth: int = Query(default=10)):
        if node_id not in _graph:
            raise HTTPException(404, f"Node {node_id} not found")
        descendants = list(nx.descendants(_graph, node_id))
        return {"root": node_id, "downstream": descendants}

except ImportError:
    app = None


# ── Client-side exercise (test the API) ──────────────────────────────
def run_client_tests():
    """Test the API using httpx (requires server to be running)."""
    try:
        import httpx
    except ImportError:
        print("  ⚠ httpx not installed — skipping client tests")
        return

    base = "http://localhost:8001"
    try:
        client = httpx.Client(timeout=5.0)

        # Health check
        resp = client.get(f"{base}/health")
        print(f"  Health: {resp.json()}")

        # Add nodes
        nodes = [
            {"id": "raw_orders", "name": "raw_orders", "node_type": "dataset"},
            {"id": "clean_job", "name": "clean_job", "node_type": "job"},
            {"id": "stg_orders", "name": "stg_orders", "node_type": "dataset"},
        ]
        for node in nodes:
            resp = client.post(f"{base}/nodes", json=node)
            print(f"  Add node: {resp.json()}")

        # Add edges
        edges = [
            {"source": "raw_orders", "target": "clean_job"},
            {"source": "clean_job", "target": "stg_orders"},
        ]
        for edge in edges:
            resp = client.post(f"{base}/edges", json=edge)
            print(f"  Add edge: {resp.json()}")

        # Query upstream
        resp = client.get(f"{base}/upstream/stg_orders")
        print(f"  Upstream: {resp.json()}")

        # Query downstream
        resp = client.get(f"{base}/downstream/raw_orders")
        print(f"  Downstream: {resp.json()}")

        client.close()

    except httpx.ConnectError:
        print("  ⚠ Server not running. Start it first:")
        print("    pixi run uvicorn docs.exercises.ch13_lineage_api:app --port 8001")


def main():
    print("=== Chapter 12: Lineage API with FastAPI ===\n")

    if app is None:
        print("  ⚠ FastAPI not installed — install with: pixi install fastapi")
        return

    print("Running client tests against localhost:8001 ...")
    run_client_tests()
    print()
    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
