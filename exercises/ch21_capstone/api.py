"""FastAPI application for the lineage platform."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from .graph_store import GraphStore
from .ingestion import OpenLineageProcessor
from .models import NodeType
from .visualization import MermaidExporter
from .governance import PIIScanner

# Initialize
app = FastAPI(title="Mini Lineage Platform", version="1.0.0")
store = GraphStore()
processor = OpenLineageProcessor(store)
exporter = MermaidExporter(store)
scanner = PIIScanner()


# --- Pydantic response models ---
class NodeResponse(BaseModel):
    id: str
    name: str
    namespace: str
    node_type: str
    classification: str


class ImpactResponse(BaseModel):
    source: str
    total_downstream: int
    affected_datasets: int
    affected_jobs: int
    datasets: list[dict]
    jobs: list[dict]


# --- Endpoints ---
@app.get("/health")
async def health():
    stats = store.stats()
    return {"status": "healthy", "graph": stats}


@app.post("/api/v1/events")
async def ingest_event(event: dict):
    """Ingest an OpenLineage event."""
    success = processor.process_event(event)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to process event")
    return {"status": "accepted", "graph_stats": store.stats()}


@app.post("/api/v1/events/batch")
async def ingest_batch(events: list[dict]):
    """Ingest a batch of OpenLineage events."""
    results = processor.process_batch(events)
    return {"results": results, "graph_stats": store.stats()}


@app.get("/api/v1/datasets")
async def list_datasets():
    """List all datasets in the lineage graph."""
    datasets = [
        NodeResponse(
            id=n.id, name=n.name, namespace=n.namespace,
            node_type=n.node_type.value,
            classification=n.classification.value,
        )
        for n in store.nodes.values()
        if n.node_type == NodeType.DATASET
    ]
    return {"datasets": datasets, "count": len(datasets)}


@app.get("/api/v1/upstream/{node_id:path}")
async def get_upstream(node_id: str,
                       depth: int = Query(default=10, le=50)):
    """Get upstream lineage for a node."""
    nodes = store.upstream(node_id, max_depth=depth)
    return {
        "root": node_id,
        "direction": "upstream",
        "depth": depth,
        "nodes": [
            {"id": n.id, "name": n.name, "type": n.node_type.value}
            for n in nodes
        ],
        "count": len(nodes),
    }


@app.get("/api/v1/downstream/{node_id:path}")
async def get_downstream(node_id: str,
                         depth: int = Query(default=10, le=50)):
    """Get downstream lineage for a node."""
    nodes = store.downstream(node_id, max_depth=depth)
    return {
        "root": node_id,
        "direction": "downstream",
        "depth": depth,
        "nodes": [
            {"id": n.id, "name": n.name, "type": n.node_type.value}
            for n in nodes
        ],
        "count": len(nodes),
    }


@app.get("/api/v1/impact/{node_id:path}")
async def get_impact(node_id: str):
    """Run impact analysis for a node."""
    return store.impact_analysis(node_id)


@app.get("/api/v1/search")
async def search_nodes(q: str = Query(min_length=1)):
    """Search nodes by name or namespace."""
    results = store.search(q)
    return {
        "query": q,
        "results": [
            {"id": n.id, "name": n.name, "type": n.node_type.value}
            for n in results
        ],
        "count": len(results),
    }


@app.get("/api/v1/visualize/{node_id:path}")
async def visualize(node_id: str,
                    direction: str = Query(default="downstream"),
                    depth: int = Query(default=5)):
    """Generate a Mermaid diagram for a lineage subgraph."""
    mermaid = exporter.subgraph_mermaid(node_id, direction, depth)
    return {"node": node_id, "mermaid": mermaid}


@app.get("/api/v1/governance/pii")
async def scan_pii():
    """Scan all datasets for potential PII columns."""
    results = scanner.scan_all_datasets(store)
    return {"pii_datasets": results, "count": len(results)}
