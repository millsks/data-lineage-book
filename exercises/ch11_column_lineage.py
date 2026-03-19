"""Chapter 11 Exercise — Column-Level Lineage.

Track data flow at the column (field) level through transformations.

Usage:
    pixi run -e exercises python docs/exercises/ch11_column_lineage.py
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnRef:
    """Reference to a column in a dataset."""
    dataset: str
    column: str

    def __str__(self) -> str:
        return f"{self.dataset}.{self.column}"


@dataclass
class ColumnMapping:
    """Mapping from source columns to a target column."""
    target: ColumnRef
    sources: list[ColumnRef]
    transform: str = "IDENTITY"  # IDENTITY, AGGREGATION, DERIVATION, etc.

    def __str__(self) -> str:
        src_str = ", ".join(str(s) for s in self.sources)
        return f"{self.target} ← [{src_str}] ({self.transform})"


class ColumnLineageTracker:
    """Track column-level lineage across datasets."""

    def __init__(self):
        self.mappings: list[ColumnMapping] = []

    def add_mapping(self, target_ds: str, target_col: str,
                    sources: list[tuple[str, str]],
                    transform: str = "IDENTITY"):
        """Register a column-level lineage mapping."""
        self.mappings.append(ColumnMapping(
            target=ColumnRef(target_ds, target_col),
            sources=[ColumnRef(ds, col) for ds, col in sources],
            transform=transform,
        ))

    def trace_column(self, dataset: str, column: str) -> list[ColumnMapping]:
        """Trace a column back to its sources (upstream)."""
        results = []
        queue = [(dataset, column)]
        visited: set[tuple[str, str]] = set()

        while queue:
            ds, col = queue.pop(0)
            if (ds, col) in visited:
                continue
            visited.add((ds, col))

            for m in self.mappings:
                if m.target.dataset == ds and m.target.column == col:
                    results.append(m)
                    for src in m.sources:
                        queue.append((src.dataset, src.column))

        return results

    def impact_column(self, dataset: str, column: str) -> list[ColumnMapping]:
        """Find all downstream columns affected by a source column."""
        results = []
        queue = [(dataset, column)]
        visited: set[tuple[str, str]] = set()

        while queue:
            ds, col = queue.pop(0)
            if (ds, col) in visited:
                continue
            visited.add((ds, col))

            for m in self.mappings:
                for src in m.sources:
                    if src.dataset == ds and src.column == col:
                        results.append(m)
                        queue.append((m.target.dataset, m.target.column))
                        break

        return results


def main():
    print("=== Chapter 10: Column-Level Lineage ===\n")

    tracker = ColumnLineageTracker()

    # raw_orders → stg_orders
    tracker.add_mapping("stg_orders", "order_id",
                        [("raw_orders", "order_id")])
    tracker.add_mapping("stg_orders", "amount",
                        [("raw_orders", "amount_cents")],
                        transform="DERIVATION: amount_cents / 100")
    tracker.add_mapping("stg_orders", "customer_id",
                        [("raw_orders", "cust_id")])

    # raw_customers → stg_customers
    tracker.add_mapping("stg_customers", "customer_id",
                        [("raw_customers", "id")])
    tracker.add_mapping("stg_customers", "full_name",
                        [("raw_customers", "first_name"),
                         ("raw_customers", "last_name")],
                        transform="DERIVATION: CONCAT(first_name, last_name)")

    # stg_orders + stg_customers → fct_orders
    tracker.add_mapping("fct_orders", "order_id",
                        [("stg_orders", "order_id")])
    tracker.add_mapping("fct_orders", "amount",
                        [("stg_orders", "amount")])
    tracker.add_mapping("fct_orders", "customer_name",
                        [("stg_customers", "full_name")])

    # fct_orders → rpt_revenue
    tracker.add_mapping("rpt_revenue", "total_revenue",
                        [("fct_orders", "amount")],
                        transform="AGGREGATION: SUM(amount)")

    # Print all mappings
    print("All column mappings:")
    for m in tracker.mappings:
        print(f"  {m}")
    print()

    # Trace a column
    print("Trace rpt_revenue.total_revenue back to sources:")
    trace = tracker.trace_column("rpt_revenue", "total_revenue")
    for m in trace:
        print(f"  {m}")
    print()

    # Impact analysis
    print("Impact of raw_orders.amount_cents:")
    impact = tracker.impact_column("raw_orders", "amount_cents")
    for m in impact:
        print(f"  {m}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
