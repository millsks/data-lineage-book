"""Process OpenLineage events into the graph store."""
from __future__ import annotations

from .graph_store import GraphStore
from .models import LineageEdge, LineageNode, NodeType


class OpenLineageProcessor:
    """Process OpenLineage events and update the lineage graph."""

    def __init__(self, store: GraphStore):
        self.store = store
        self.events_processed: int = 0
        self.events_failed: int = 0

    def process_event(self, event: dict) -> bool:
        """Process a single OpenLineage event."""
        try:
            job = event.get("job", {})
            job_namespace = job.get("namespace", "default")
            job_name = job.get("name", "unknown")
            job_id = f"{job_namespace}/{job_name}"

            self.store.upsert_node(LineageNode(
                id=job_id,
                name=job_name,
                namespace=job_namespace,
                node_type=NodeType.JOB,
                metadata=job.get("facets", {}),
            ))

            for inp in event.get("inputs", []):
                ds_namespace = inp.get("namespace", "default")
                ds_name = inp.get("name", "unknown")
                ds_id = f"{ds_namespace}/{ds_name}"

                self.store.upsert_node(LineageNode(
                    id=ds_id,
                    name=ds_name,
                    namespace=ds_namespace,
                    node_type=NodeType.DATASET,
                    metadata=inp.get("facets", {}),
                ))

                self.store.add_edge(LineageEdge(
                    source_id=ds_id,
                    target_id=job_id,
                    edge_type="CONSUMED_BY",
                ))

            for out in event.get("outputs", []):
                ds_namespace = out.get("namespace", "default")
                ds_name = out.get("name", "unknown")
                ds_id = f"{ds_namespace}/{ds_name}"

                self.store.upsert_node(LineageNode(
                    id=ds_id,
                    name=ds_name,
                    namespace=ds_namespace,
                    node_type=NodeType.DATASET,
                    metadata=out.get("facets", {}),
                ))

                self.store.add_edge(LineageEdge(
                    source_id=job_id,
                    target_id=ds_id,
                    edge_type="PRODUCED_BY",
                ))

            self.events_processed += 1
            return True

        except Exception as e:
            self.events_failed += 1
            print(f"Failed to process event: {e}")
            return False

    def process_batch(self, events: list[dict]) -> dict:
        """Process a batch of events."""
        results = {"success": 0, "failed": 0}
        for event in events:
            if self.process_event(event):
                results["success"] += 1
            else:
                results["failed"] += 1
        return results
