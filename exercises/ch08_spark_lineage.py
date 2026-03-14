"""Chapter 8 Exercise — Spark Lineage.

Demonstrates PySpark job metadata and how the OpenLineage Spark
integration captures lineage from Spark query plans.

Usage:
    pixi run -e exercises python docs/exercises/ch08_spark_lineage.py
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone


# ── 1. Simulate Spark logical plan lineage ────────────────────────────
def simulate_spark_plan_lineage() -> dict:
    """Simulate the lineage a Spark listener would capture."""
    return {
        "physical_plan": (
            "HashAggregate(keys=[customer_id], "
            "functions=[sum(amount)])\n"
            "  +- Exchange hashpartitioning(customer_id)\n"
            "     +- HashAggregate(partial)\n"
            "        +- Project [customer_id, amount]\n"
            "           +- Filter (order_date > 2024-01-01)\n"
            "              +- FileScan parquet [raw.orders]"
        ),
        "input_datasets": [
            {
                "namespace": "hdfs://cluster",
                "name": "/data/raw/orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "long"},
                            {"name": "customer_id", "type": "long"},
                            {"name": "amount", "type": "double"},
                            {"name": "order_date", "type": "date"},
                        ]
                    }
                },
            }
        ],
        "output_datasets": [
            {
                "namespace": "hdfs://cluster",
                "name": "/data/mart/customer_revenue",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "long"},
                            {"name": "total_revenue", "type": "double"},
                        ]
                    },
                    "columnLineage": {
                        "fields": {
                            "customer_id": {
                                "inputFields": [
                                    {
                                        "namespace": "hdfs://cluster",
                                        "name": "/data/raw/orders",
                                        "field": "customer_id",
                                    }
                                ]
                            },
                            "total_revenue": {
                                "inputFields": [
                                    {
                                        "namespace": "hdfs://cluster",
                                        "name": "/data/raw/orders",
                                        "field": "amount",
                                    }
                                ],
                                "transformationType": "AGGREGATION",
                                "transformationDescription": "SUM(amount)",
                            },
                        }
                    },
                },
            }
        ],
    }


# ── 2. Build OpenLineage event from Spark plan ───────────────────────
def spark_plan_to_event(plan_lineage: dict) -> dict:
    """Convert Spark plan lineage into an OpenLineage event."""
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": str(uuid.uuid4())},
        "job": {
            "namespace": "spark://production",
            "name": "customer_revenue_aggregation",
            "facets": {
                "sourceCode": {
                    "type": "SPARK_SQL",
                    "plan": plan_lineage["physical_plan"],
                }
            },
        },
        "inputs": plan_lineage["input_datasets"],
        "outputs": plan_lineage["output_datasets"],
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark",
    }


# ── 3. PySpark example (only runs if PySpark is installed) ────────────
def pyspark_demo():
    """Demonstrate a real PySpark transformation."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("  ⚠ PySpark not installed — skipping live Spark example")
        return

    spark = SparkSession.builder.master("local[*]").appName("ch08_exercise").getOrCreate()

    data = [
        (1, 100, 50.0),
        (2, 100, 75.0),
        (3, 200, 30.0),
        (4, 200, 45.0),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "amount"])
    result = df.groupBy("customer_id").agg({"amount": "sum"})
    result.show()

    print("  Logical plan:")
    result.explain(True)

    spark.stop()


# ── 4. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 8: Spark Lineage ===\n")

    plan_lineage = simulate_spark_plan_lineage()
    print("Simulated Spark physical plan:")
    print(plan_lineage["physical_plan"])
    print()

    event = spark_plan_to_event(plan_lineage)
    print("OpenLineage event (truncated):")
    print(json.dumps(event, indent=2)[:600])
    print()

    # Column lineage from the output dataset
    col_lineage = (
        event["outputs"][0]["facets"]
        .get("columnLineage", {})
        .get("fields", {})
    )
    print("Column-level lineage:")
    for col, info in col_lineage.items():
        sources = [
            f"{f['name']}.{f['field']}" for f in info.get("inputFields", [])
        ]
        transform = info.get("transformationDescription", "IDENTITY")
        print(f"  {col} ← {sources} ({transform})")
    print()

    print("PySpark demo:")
    pyspark_demo()
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
