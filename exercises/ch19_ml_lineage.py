"""Chapter 19 Exercise — ML Lineage.

Track lineage through ML pipelines: data → features → model → predictions.

Usage:
    pixi run -e exercises python docs/exercises/ch19_ml_lineage.py
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import networkx as nx


@dataclass
class FeatureDefinition:
    name: str
    source_dataset: str
    transform: str  # e.g., "AVG(amount) OVER 30d"


@dataclass
class ModelVersion:
    name: str
    version: str
    algorithm: str
    features: list[str]
    training_dataset: str
    metrics: dict[str, float] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


class MLLineageTracker:
    """Track lineage across the ML lifecycle."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.features: dict[str, FeatureDefinition] = {}
        self.models: dict[str, ModelVersion] = {}

    def add_dataset(self, name: str):
        self.graph.add_node(name, type="dataset")

    def add_feature(self, feature: FeatureDefinition):
        self.features[feature.name] = feature
        self.graph.add_node(feature.name, type="feature",
                            transform=feature.transform)
        self.graph.add_edge(feature.source_dataset, feature.name)

    def add_model(self, model: ModelVersion):
        key = f"{model.name}:{model.version}"
        self.models[key] = model
        self.graph.add_node(key, type="model", algorithm=model.algorithm)

        # Link features → model
        for feat in model.features:
            self.graph.add_edge(feat, key)

        # Link training data → model
        self.graph.add_edge(model.training_dataset, key)

    def add_prediction_output(self, model_key: str, output_dataset: str):
        self.graph.add_node(output_dataset, type="predictions")
        self.graph.add_edge(model_key, output_dataset)

    def model_data_sources(self, model_key: str) -> list[str]:
        """Find all raw data sources for a model."""
        ancestors = nx.ancestors(self.graph, model_key)
        return [
            n for n in ancestors
            if self.graph.nodes[n].get("type") == "dataset"
        ]

    def feature_impact(self, dataset: str) -> list[str]:
        """If a dataset changes, which features and models are affected?"""
        downstream = nx.descendants(self.graph, dataset)
        models = [
            n for n in downstream
            if self.graph.nodes[n].get("type") == "model"
        ]
        features = [
            n for n in downstream
            if self.graph.nodes[n].get("type") == "feature"
        ]
        return {"features": features, "models": models}


def main():
    print("=== Chapter 18: ML Lineage ===\n")

    tracker = MLLineageTracker()

    # Datasets
    tracker.add_dataset("raw_orders")
    tracker.add_dataset("raw_customers")
    tracker.add_dataset("raw_sessions")

    # Features
    tracker.add_feature(FeatureDefinition(
        "avg_order_value_30d", "raw_orders", "AVG(amount) OVER 30 days"
    ))
    tracker.add_feature(FeatureDefinition(
        "order_count_90d", "raw_orders", "COUNT(*) OVER 90 days"
    ))
    tracker.add_feature(FeatureDefinition(
        "days_since_last_visit", "raw_sessions", "DATEDIFF(NOW(), MAX(session_date))"
    ))
    tracker.add_feature(FeatureDefinition(
        "customer_tenure_days", "raw_customers", "DATEDIFF(NOW(), signup_date)"
    ))

    # Model
    model = ModelVersion(
        name="churn_predictor",
        version="v2.1",
        algorithm="XGBoost",
        features=["avg_order_value_30d", "order_count_90d",
                   "days_since_last_visit", "customer_tenure_days"],
        training_dataset="raw_customers",
        metrics={"auc": 0.87, "precision": 0.82, "recall": 0.79},
    )
    tracker.add_model(model)
    tracker.add_prediction_output("churn_predictor:v2.1", "predictions_churn")

    print(f"Graph: {tracker.graph.number_of_nodes()} nodes, "
          f"{tracker.graph.number_of_edges()} edges\n")

    # Model data sources
    print("Data sources for churn_predictor:v2.1:")
    sources = tracker.model_data_sources("churn_predictor:v2.1")
    for s in sources:
        print(f"  ← {s}")
    print()

    # Impact analysis
    print("Impact of raw_orders change:")
    impact = tracker.feature_impact("raw_orders")
    print(f"  Affected features: {impact['features']}")
    print(f"  Affected models:   {impact['models']}")
    print()

    # Exercise: Add a second model version and compare lineage
    # Exercise: Track feature drift by adding metric history

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
