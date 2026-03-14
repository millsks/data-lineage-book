"""PII detection and data classification."""
from __future__ import annotations

import re
from dataclasses import dataclass

from .graph_store import GraphStore
from .models import NodeType

PII_PATTERNS = {
    "EMAIL": [r"email", r"e_mail"],
    "PHONE": [r"phone", r"tel", r"mobile"],
    "SSN": [r"ssn", r"social_security"],
    "NAME": [r"first_name", r"last_name", r"full_name"],
    "ADDRESS": [r"address", r"street", r"zip", r"postal"],
    "DOB": [r"dob", r"birth_date", r"date_of_birth"],
}


@dataclass
class PIIFinding:
    dataset_id: str
    column: str
    pii_type: str


class PIIScanner:
    """Scan dataset metadata for PII patterns."""

    def scan_columns(self, dataset_id: str,
                     columns: list[str]) -> list[PIIFinding]:
        findings = []
        for col in columns:
            col_lower = col.lower()
            for pii_type, patterns in PII_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, col_lower):
                        findings.append(PIIFinding(dataset_id, col, pii_type))
                        break
        return findings

    def scan_all_datasets(self, store: GraphStore) -> list[dict]:
        """Scan all datasets in the store for PII."""
        results = []
        for node in store.nodes.values():
            if node.node_type != NodeType.DATASET:
                continue
            columns = node.metadata.get("schema", {}).get("fields", [])
            if not columns:
                continue
            col_names = [
                c.get("name", "") for c in columns if isinstance(c, dict)
            ]
            findings = self.scan_columns(node.id, col_names)
            if findings:
                results.append({
                    "dataset": node.id,
                    "pii_columns": [
                        {"column": f.column, "type": f.pii_type}
                        for f in findings
                    ],
                })
        return results

    def trace_pii_downstream(self, store: GraphStore,
                              dataset_id: str) -> list[str]:
        """Find all downstream datasets that may contain PII."""
        downstream = store.downstream(dataset_id)
        return [n.id for n in downstream]
