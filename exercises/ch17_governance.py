"""Chapter 17 Exercise — Compliance, Governance & Privacy.

Track PII through lineage, generate erasure plans,
and classify datasets.

Usage:
    pixi run -e exercises python docs/exercises/ch17_governance.py
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

import networkx as nx


class Classification(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


PII_PATTERNS = {
    "EMAIL": [r"email", r"e_mail"],
    "PHONE": [r"phone", r"tel", r"mobile"],
    "SSN": [r"ssn", r"social_security"],
    "NAME": [r"first_name", r"last_name", r"full_name"],
    "ADDRESS": [r"address", r"street", r"zip_code", r"postal"],
    "DOB": [r"dob", r"date_of_birth", r"birth_date"],
}


@dataclass
class PIIFinding:
    dataset: str
    column: str
    pii_type: str


@dataclass
class ErasureStep:
    dataset: str
    action: str  # DELETE, ANONYMIZE, NOTIFY
    columns: list[str] = field(default_factory=list)


class GovernanceTracker:
    """Track PII, classification, and erasure through lineage."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.schemas: dict[str, list[str]] = {}
        self.classifications: dict[str, Classification] = {}

    def add_dataset(self, name: str, columns: list[str],
                    classification: Classification = Classification.INTERNAL):
        self.graph.add_node(name, type="dataset")
        self.schemas[name] = columns
        self.classifications[name] = classification

    def add_flow(self, source: str, target: str):
        self.graph.add_edge(source, target)

    def scan_pii(self) -> list[PIIFinding]:
        """Scan all datasets for PII column patterns."""
        findings = []
        for ds, columns in self.schemas.items():
            for col in columns:
                col_lower = col.lower()
                for pii_type, patterns in PII_PATTERNS.items():
                    if any(re.search(p, col_lower) for p in patterns):
                        findings.append(PIIFinding(ds, col, pii_type))
                        break
        return findings

    def trace_pii_flow(self, dataset: str) -> list[str]:
        """Find all downstream datasets that inherit PII."""
        return [
            n for n in nx.descendants(self.graph, dataset)
            if self.graph.nodes[n].get("type") == "dataset"
        ]

    def generate_erasure_plan(self, subject_dataset: str) -> list[ErasureStep]:
        """GDPR Article 17: Generate an erasure plan."""
        steps = []

        # Primary dataset
        pii_cols = [
            f.column for f in self.scan_pii()
            if f.dataset == subject_dataset
        ]
        steps.append(ErasureStep(subject_dataset, "DELETE", pii_cols))

        # Downstream datasets
        for downstream in self.trace_pii_flow(subject_dataset):
            ds_pii = [
                f.column for f in self.scan_pii()
                if f.dataset == downstream
            ]
            if ds_pii:
                steps.append(ErasureStep(downstream, "ANONYMIZE", ds_pii))
            else:
                steps.append(ErasureStep(downstream, "NOTIFY"))

        return steps


def main():
    print("=== Chapter 16: Compliance, Governance & Privacy ===\n")

    tracker = GovernanceTracker()

    # Build pipeline
    tracker.add_dataset("crm_customers",
                        ["customer_id", "first_name", "last_name",
                         "email", "phone", "date_of_birth"],
                        Classification.RESTRICTED)
    tracker.add_dataset("stg_customers",
                        ["customer_id", "full_name", "email_hash"],
                        Classification.CONFIDENTIAL)
    tracker.add_dataset("dim_customers",
                        ["customer_id", "name", "region"],
                        Classification.INTERNAL)
    tracker.add_dataset("rpt_customer_cohorts",
                        ["cohort", "count", "avg_order_value"],
                        Classification.PUBLIC)

    tracker.add_flow("crm_customers", "stg_customers")
    tracker.add_flow("stg_customers", "dim_customers")
    tracker.add_flow("dim_customers", "rpt_customer_cohorts")

    # PII scan
    print("PII scan results:")
    findings = tracker.scan_pii()
    for f in findings:
        print(f"  [{f.pii_type}] {f.dataset}.{f.column}")
    print()

    # PII flow trace
    print("PII flow from crm_customers:")
    downstream = tracker.trace_pii_flow("crm_customers")
    for ds in downstream:
        cls = tracker.classifications.get(ds, Classification.INTERNAL)
        print(f"  → {ds} (classification: {cls.value})")
    print()

    # Erasure plan
    print("GDPR erasure plan for crm_customers:")
    plan = tracker.generate_erasure_plan("crm_customers")
    for step in plan:
        cols = ", ".join(step.columns) if step.columns else "n/a"
        print(f"  [{step.action}] {step.dataset} — columns: {cols}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
