"""Chapter 8 Exercise — Airflow & Marquez Integration.

Demonstrates creating Airflow-style DAG metadata and sending
OpenLineage events to a Marquez server.

Usage:
    pixi run -e exercises python docs/exercises/ch08_airflow_marquez.py

    Requires: a running Marquez server (docker-compose) for the API calls.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone


# ── 1. Simulate an Airflow DAG structure ──────────────────────────────
def build_dag_metadata() -> dict:
    """Build metadata representing an Airflow DAG."""
    return {
        "dag_id": "daily_order_pipeline",
        "schedule": "0 6 * * *",
        "tasks": [
            {
                "task_id": "extract_orders",
                "operator": "PostgresOperator",
                "inputs": [{"namespace": "source_db", "name": "sales.orders"}],
                "outputs": [{"namespace": "datalake", "name": "raw.orders"}],
            },
            {
                "task_id": "transform_orders",
                "operator": "PythonOperator",
                "inputs": [{"namespace": "datalake", "name": "raw.orders"}],
                "outputs": [{"namespace": "warehouse", "name": "stg_orders"}],
            },
            {
                "task_id": "build_summary",
                "operator": "PythonOperator",
                "inputs": [{"namespace": "warehouse", "name": "stg_orders"}],
                "outputs": [{"namespace": "warehouse", "name": "fct_order_summary"}],
            },
        ],
        "dependencies": {
            "transform_orders": ["extract_orders"],
            "build_summary": ["transform_orders"],
        },
    }


# ── 2. Convert DAG tasks to OpenLineage events ───────────────────────
def dag_to_openlineage_events(dag: dict) -> list[dict]:
    """Convert DAG metadata into OpenLineage RunEvent dicts."""
    events = []
    dag_namespace = f"airflow://{dag['dag_id']}"

    for task in dag["tasks"]:
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {"runId": str(uuid.uuid4())},
            "job": {
                "namespace": dag_namespace,
                "name": task["task_id"],
                "facets": {
                    "jobType": {
                        "processingType": "BATCH",
                        "integration": "AIRFLOW",
                        "jobType": task["operator"],
                    }
                },
            },
            "inputs": task["inputs"],
            "outputs": task["outputs"],
            "producer": "https://github.com/apache/airflow",
        }
        events.append(event)

    return events


# ── 3. Send events to Marquez (if available) ──────────────────────────
def send_to_marquez(events: list[dict], marquez_url: str = "http://localhost:5000"):
    """Post events to a running Marquez server."""
    try:
        import httpx
    except ImportError:
        print("  ⚠ httpx not installed — skipping Marquez push")
        return

    for event in events:
        try:
            resp = httpx.post(
                f"{marquez_url}/api/v1/lineage",
                json=event,
                timeout=5.0,
            )
            job_name = event["job"]["name"]
            print(f"  Sent {job_name}: HTTP {resp.status_code}")
        except httpx.ConnectError:
            print("  ⚠ Marquez not reachable — skipping")
            return


# ── 4. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 7: Airflow & Marquez ===\n")

    dag = build_dag_metadata()
    print(f"DAG: {dag['dag_id']}")
    print(f"Tasks: {[t['task_id'] for t in dag['tasks']]}")
    print(f"Schedule: {dag['schedule']}")
    print()

    events = dag_to_openlineage_events(dag)
    print(f"Generated {len(events)} OpenLineage events:")
    for event in events:
        job = event["job"]["name"]
        inputs = [i["name"] for i in event["inputs"]]
        outputs = [o["name"] for o in event["outputs"]]
        print(f"  {job}: {inputs} → {outputs}")
    print()

    print("Sending to Marquez:")
    send_to_marquez(events)
    print()

    # Exercise: Add an extract_customers task and wire it up
    # Exercise: Add a FAIL event for a broken task

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
