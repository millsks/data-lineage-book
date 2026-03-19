"""Seed the lineage platform with sample data."""
from __future__ import annotations

from datetime import datetime


def generate_sample_events() -> list[dict]:
    """Generate a realistic set of OpenLineage events."""
    events = []

    # Ingestion layer
    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "ingestion", "name": "extract_orders"},
        "inputs": [{"namespace": "source_db", "name": "sales.orders",
                     "facets": {"schema": {"fields": [
                         {"name": "order_id"}, {"name": "customer_email"},
                         {"name": "amount"}, {"name": "order_date"},
                     ]}}}],
        "outputs": [{"namespace": "datalake", "name": "raw.orders"}],
    })

    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "ingestion", "name": "extract_customers"},
        "inputs": [{"namespace": "source_db", "name": "crm.customers",
                     "facets": {"schema": {"fields": [
                         {"name": "customer_id"}, {"name": "first_name"},
                         {"name": "last_name"}, {"name": "email"},
                         {"name": "phone"}, {"name": "date_of_birth"},
                     ]}}}],
        "outputs": [{"namespace": "datalake", "name": "raw.customers"}],
    })

    # Staging layer
    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "dbt", "name": "stg_orders"},
        "inputs": [{"namespace": "datalake", "name": "raw.orders"}],
        "outputs": [{"namespace": "warehouse", "name": "stg_orders"}],
    })

    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "dbt", "name": "stg_customers"},
        "inputs": [{"namespace": "datalake", "name": "raw.customers"}],
        "outputs": [{"namespace": "warehouse", "name": "stg_customers"}],
    })

    # Marts layer
    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "dbt", "name": "fct_orders"},
        "inputs": [
            {"namespace": "warehouse", "name": "stg_orders"},
            {"namespace": "warehouse", "name": "stg_customers"},
        ],
        "outputs": [{"namespace": "warehouse", "name": "fct_orders"}],
    })

    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "dbt", "name": "dim_customers"},
        "inputs": [{"namespace": "warehouse", "name": "stg_customers"}],
        "outputs": [{"namespace": "warehouse", "name": "dim_customers"}],
    })

    # Analytics layer
    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "analytics", "name": "rpt_revenue"},
        "inputs": [{"namespace": "warehouse", "name": "fct_orders"}],
        "outputs": [{"namespace": "analytics", "name": "rpt_daily_revenue"}],
    })

    events.append({
        "eventType": "COMPLETE",
        "eventTime": datetime.now().isoformat(),
        "job": {"namespace": "ml", "name": "churn_features"},
        "inputs": [
            {"namespace": "warehouse", "name": "fct_orders"},
            {"namespace": "warehouse", "name": "dim_customers"},
        ],
        "outputs": [{"namespace": "feature_store", "name": "customer_features"}],
    })

    return events


if __name__ == "__main__":
    from .graph_store import GraphStore
    from .ingestion import OpenLineageProcessor
    from .visualization import MermaidExporter

    store = GraphStore()
    processor = OpenLineageProcessor(store)

    events = generate_sample_events()
    results = processor.process_batch(events)

    print(f"Processed: {results}")
    print(f"Graph stats: {store.stats()}")
    print()

    exporter = MermaidExporter(store)
    print("--- Full Lineage Graph (Mermaid) ---")
    print(exporter.full_graph_mermaid())
    print()

    impact = store.impact_analysis("source_db/crm.customers")
    print(f"Impact of crm.customers change: {impact}")
