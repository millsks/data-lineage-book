"""Quality monitoring and SLA checking."""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field

from .graph_store import GraphStore
from .models import DataSLA, QualityMetrics


@dataclass
class QualityMonitor:
    """Monitor data quality with lineage context."""

    history: dict[str, list[QualityMetrics]] = field(default_factory=dict)
    slas: dict[str, DataSLA] = field(default_factory=dict)

    def record_metrics(self, metrics: QualityMetrics):
        self.history.setdefault(metrics.dataset_id, []).append(metrics)

    def register_sla(self, sla: DataSLA):
        self.slas[sla.dataset_id] = sla

    def check_sla(self, dataset_id: str,
                  current: QualityMetrics) -> list[str]:
        """Check if current metrics violate the SLA."""
        sla = self.slas.get(dataset_id)
        if not sla:
            return []

        violations = []
        if current.freshness_hours > sla.freshness_max_hours:
            violations.append(
                f"Freshness SLA violated: {current.freshness_hours:.1f}h > "
                f"{sla.freshness_max_hours}h"
            )
        if current.row_count < sla.min_row_count:
            violations.append(
                f"Row count below minimum: {current.row_count} < "
                f"{sla.min_row_count}"
            )
        for col, rate in current.null_rates.items():
            if rate > sla.max_null_rate:
                violations.append(
                    f"Null rate for '{col}': {rate:.2%} > {sla.max_null_rate:.2%}"
                )
        return violations

    def is_anomaly(self, dataset_id: str, metric: str,
                   value: float, z_threshold: float = 3.0) -> bool:
        """Detect anomalous metric values using z-score."""
        history = self.history.get(dataset_id, [])
        if len(history) < 5:
            return False

        values = [
            getattr(m, metric, 0) for m in history if hasattr(m, metric)
        ]
        if len(values) < 5 or statistics.stdev(values) == 0:
            return False

        z = abs(value - statistics.mean(values)) / statistics.stdev(values)
        return z > z_threshold

    def propagate_quality(self, store: GraphStore,
                          failed_dataset: str) -> list[str]:
        """Use lineage to find datasets affected by a quality failure."""
        downstream = store.downstream(failed_dataset)
        return [n.id for n in downstream]
