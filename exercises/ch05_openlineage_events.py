"""Chapter 5 Exercise — OpenLineage Events.

Create, emit, and inspect OpenLineage RunEvent objects.

Usage:
    pixi run -e exercises python docs/exercises/ch05_openlineage_events.py
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone


# ── 1. Build an OpenLineage event manually (no SDK) ───────────────────
def build_run_event_dict(
    job_name: str,
    namespace: str,
    inputs: list[dict],
    outputs: list[dict],
    event_type: str = "COMPLETE",
) -> dict:
    """Construct an OpenLineage RunEvent as a plain dict."""
    return {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {
            "runId": str(uuid.uuid4()),
        },
        "job": {
            "namespace": namespace,
            "name": job_name,
        },
        "inputs": inputs,
        "outputs": outputs,
        "producer": "https://example.com/exercises",
        "schemaURL": (
            "https://openlineage.io/spec/2-0-2/OpenLineage.json"
            "#/$defs/RunEvent"
        ),
    }


def make_dataset(namespace: str, name: str, schema_fields: list[str]) -> dict:
    """Shorthand helper to create a dataset dict with a schema facet."""
    return {
        "namespace": namespace,
        "name": name,
        "facets": {
            "schema": {
                "_producer": "https://example.com",
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/"
                    "1-1-1/SchemaDatasetFacet.json#/$defs/"
                    "SchemaDatasetFacet"
                ),
                "fields": [{"name": f, "type": "STRING"} for f in schema_fields],
            }
        },
    }


# ── 2. OpenLineage Python SDK example (requires openlineage-python) ──
def sdk_example():
    """Demonstrate the OpenLineage Python SDK (optional)."""
    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.run import (
            RunEvent, RunState, Run, Job,
            InputDataset, OutputDataset,
        )
        from openlineage.client.facet import SchemaDatasetFacet, SchemaField
    except ImportError:
        print("  ⚠ openlineage-python not installed — skipping SDK example")
        return

    run = Run(runId=str(uuid.uuid4()))
    job = Job(namespace="exercise", name="sdk_demo_job")

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run,
        job=job,
        inputs=[
            InputDataset(
                namespace="warehouse",
                name="raw.events",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaField(name="event_id", type="INT"),
                            SchemaField(name="event_ts", type="TIMESTAMP"),
                        ]
                    )
                },
            )
        ],
        outputs=[
            OutputDataset(namespace="warehouse", name="stg.events")
        ],
        producer="https://example.com/exercises",
    )
    print(f"  SDK event type: {event.eventType}")
    print(f"  SDK run ID:     {event.run.runId}")


# ── 3. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 5: OpenLineage Events ===\n")

    # Build a START event
    start_event = build_run_event_dict(
        job_name="ingest_orders",
        namespace="production",
        inputs=[
            make_dataset("source_db", "sales.orders", ["order_id", "amount"]),
        ],
        outputs=[],
        event_type="START",
    )
    print("START event:")
    print(json.dumps(start_event, indent=2)[:500])
    print()

    # Build a COMPLETE event
    complete_event = build_run_event_dict(
        job_name="ingest_orders",
        namespace="production",
        inputs=[
            make_dataset("source_db", "sales.orders", ["order_id", "amount"]),
        ],
        outputs=[
            make_dataset("datalake", "raw.orders", ["order_id", "amount", "loaded_at"]),
        ],
        event_type="COMPLETE",
    )
    print("COMPLETE event:")
    print(json.dumps(complete_event, indent=2)[:500])
    print()

    # SDK demo
    print("SDK example:")
    sdk_example()
    print()

    # Exercise: Create a FAIL event for a broken pipeline
    # Uncomment and complete:
    # fail_event = build_run_event_dict(
    #     job_name="transform_orders",
    #     namespace="production",
    #     inputs=[...],
    #     outputs=[],
    #     event_type="FAIL",
    # )
    # print(json.dumps(fail_event, indent=2))

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
