"""Chapter 17 Exercise — Data Mesh & Federated Lineage.

Model data products with input/output ports and stitch
cross-domain lineage graphs together.

Usage:
    pixi run -e exercises python docs/exercises/ch17_data_mesh.py
"""
from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx


@dataclass
class Port:
    """A data product input or output port."""
    name: str
    namespace: str
    schema: list[str] = field(default_factory=list)


@dataclass
class DataProduct:
    """A data product in a data mesh."""
    name: str
    domain: str
    owner: str
    input_ports: list[Port] = field(default_factory=list)
    output_ports: list[Port] = field(default_factory=list)
    sla_freshness_hours: float = 24.0


class FederatedLineageService:
    """Stitch lineage across domain-owned data products."""

    def __init__(self):
        self.products: dict[str, DataProduct] = {}
        self.graph = nx.DiGraph()

    def register_product(self, product: DataProduct):
        self.products[product.name] = product
        self.graph.add_node(product.name, domain=product.domain,
                            owner=product.owner, type="product")

        # Create edges from input ports to the product
        for port in product.input_ports:
            port_id = f"{port.namespace}/{port.name}"
            self.graph.add_node(port_id, type="port")
            self.graph.add_edge(port_id, product.name)

        # Create edges from the product to output ports
        for port in product.output_ports:
            port_id = f"{port.namespace}/{port.name}"
            self.graph.add_node(port_id, type="port")
            self.graph.add_edge(product.name, port_id)

    def stitch(self):
        """Stitch cross-domain links by connecting output ports to
        input ports with matching identifiers."""
        output_ports: dict[str, str] = {}
        input_ports: dict[str, str] = {}

        for pname, product in self.products.items():
            for port in product.output_ports:
                pid = f"{port.namespace}/{port.name}"
                output_ports[pid] = pname
            for port in product.input_ports:
                pid = f"{port.namespace}/{port.name}"
                input_ports[pid] = pname

        # Connect matching ports
        for pid in output_ports:
            if pid in input_ports:
                self.graph.add_edge(output_ports[pid], input_ports[pid],
                                    link_type="cross-domain")

    def trace_upstream(self, product_name: str) -> list[str]:
        return [
            n for n in nx.ancestors(self.graph, product_name)
            if self.graph.nodes[n].get("type") == "product"
        ]

    def trace_downstream(self, product_name: str) -> list[str]:
        return [
            n for n in nx.descendants(self.graph, product_name)
            if self.graph.nodes[n].get("type") == "product"
        ]


def main():
    print("=== Chapter 17: Data Mesh & Federated Lineage ===\n")

    service = FederatedLineageService()

    # Domain: Sales
    service.register_product(DataProduct(
        name="orders-product",
        domain="sales",
        owner="sales-team",
        input_ports=[Port("raw_orders", "source-db")],
        output_ports=[Port("fct_orders", "warehouse")],
        sla_freshness_hours=6,
    ))

    # Domain: Marketing
    service.register_product(DataProduct(
        name="customer-segments",
        domain="marketing",
        owner="marketing-team",
        input_ports=[
            Port("fct_orders", "warehouse"),  # Cross-domain!
            Port("raw_campaigns", "marketing-db"),
        ],
        output_ports=[Port("dim_segments", "warehouse")],
        sla_freshness_hours=12,
    ))

    # Domain: Analytics
    service.register_product(DataProduct(
        name="revenue-dashboard",
        domain="analytics",
        owner="analytics-team",
        input_ports=[
            Port("fct_orders", "warehouse"),
            Port("dim_segments", "warehouse"),
        ],
        output_ports=[Port("rpt_revenue", "analytics")],
        sla_freshness_hours=24,
    ))

    # Stitch cross-domain lineage
    service.stitch()

    print(f"Federated graph: {service.graph.number_of_nodes()} nodes, "
          f"{service.graph.number_of_edges()} edges\n")

    print("Registered data products:")
    for name, product in service.products.items():
        inputs = [p.name for p in product.input_ports]
        outputs = [p.name for p in product.output_ports]
        print(f"  [{product.domain}] {name}: {inputs} → {outputs}")
    print()

    # Cross-domain trace
    print("Upstream of revenue-dashboard:")
    for p in service.trace_upstream("revenue-dashboard"):
        domain = service.graph.nodes[p].get("domain", "?")
        print(f"  ← [{domain}] {p}")
    print()

    print("Downstream of orders-product:")
    for p in service.trace_downstream("orders-product"):
        domain = service.graph.nodes[p].get("domain", "?")
        print(f"  → [{domain}] {p}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
