"""Chapter 6 Exercise — SQL Lineage Parsing.

Parse SQL statements to extract table-level and column-level lineage.

Usage:
    pixi run -e exercises python docs/exercises/ch06_sql_parsing.py
"""
from __future__ import annotations

import re


# ── 1. Minimal SQL lineage parser (no dependencies) ──────────────────
def extract_tables_from_sql(sql: str) -> dict[str, list[str]]:
    """Extract source and target tables from simple SQL.

    Returns {"sources": [...], "targets": [...]}.
    """
    sql_upper = sql.upper()
    targets: list[str] = []
    sources: list[str] = []

    # INSERT INTO ... or CREATE TABLE ...
    insert_match = re.search(
        r"(?:INSERT\s+INTO|CREATE\s+TABLE)\s+(\S+)", sql_upper
    )
    if insert_match:
        targets.append(insert_match.group(1).strip("("))

    # FROM and JOIN clauses
    from_matches = re.findall(
        r"(?:FROM|JOIN)\s+(\S+)", sql_upper
    )
    for table in from_matches:
        table = table.strip("()")
        if table not in ("SELECT", "WHERE", "(") and not table.startswith("("):
            sources.append(table)

    return {"sources": sources, "targets": targets}


# ── 2. sqllineage library (if installed) ──────────────────────────────
def sqllineage_example(sql: str):
    """Use the sqllineage library for more accurate parsing."""
    try:
        from sqllineage.runner import LineageRunner
    except ImportError:
        print("  ⚠ sqllineage not installed — skipping library example")
        return

    runner = LineageRunner(sql)
    print(f"  Source tables: {runner.source_tables()}")
    print(f"  Target tables: {runner.target_tables()}")

    # Column-level lineage (if available)
    try:
        for col_lineage in runner.get_column_lineage():
            src, tgt = col_lineage
            print(f"  Column: {src} → {tgt}")
    except Exception:
        pass


# ── 3. Exercises ──────────────────────────────────────────────────────
def main():
    print("=== Chapter 6: SQL Lineage Parsing ===\n")

    # Simple SELECT
    sql1 = """
    SELECT o.order_id, c.customer_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    """
    print("SQL 1 — Simple JOIN:")
    result = extract_tables_from_sql(sql1)
    print(f"  Sources: {result['sources']}")
    print(f"  Targets: {result['targets']}")
    print()

    # INSERT ... SELECT
    sql2 = """
    INSERT INTO fct_order_summary
    SELECT o.order_id, c.name, SUM(o.amount) as total
    FROM stg_orders o
    JOIN stg_customers c ON o.customer_id = c.id
    GROUP BY o.order_id, c.name
    """
    print("SQL 2 — INSERT ... SELECT:")
    result = extract_tables_from_sql(sql2)
    print(f"  Sources: {result['sources']}")
    print(f"  Targets: {result['targets']}")
    print()

    # CTE example
    sql3 = """
    WITH recent_orders AS (
        SELECT * FROM orders WHERE order_date > '2024-01-01'
    )
    INSERT INTO analytics.recent_summary
    SELECT customer_id, COUNT(*) as order_count
    FROM recent_orders
    JOIN customers ON recent_orders.customer_id = customers.id
    GROUP BY customer_id
    """
    print("SQL 3 — CTE:")
    result = extract_tables_from_sql(sql3)
    print(f"  Sources: {result['sources']}")
    print(f"  Targets: {result['targets']}")
    print()

    # sqllineage library
    print("sqllineage library example:")
    sqllineage_example(sql2)
    print()

    # Exercise: Parse a UNION query
    # sql_union = """
    # INSERT INTO combined_events
    # SELECT event_id, event_type FROM web_events
    # UNION ALL
    # SELECT event_id, event_type FROM mobile_events
    # """
    # Uncomment and parse it!

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
