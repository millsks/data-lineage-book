"""Chapter 15 Exercise — Data Observability.

Build an observability monitor that tracks pipeline metrics
and detects anomalies using lineage-aware context.

Usage:
    pixi run -e exercises python docs/exercises/ch15_observability.py
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class PipelineMetric:
    """A metric recorded for a pipeline stage."""
    pipeline: str
    stage: str
    metric_name: str
    value: float
    recorded_at: datetime = field(default_factory=datetime.now)


@dataclass
class SLI:
    """Service Level Indicator."""
    name: str
    target: float
    actual: float

    @property
    def met(self) -> bool:
        return self.actual >= self.target


class ObservabilityMonitor:
    """Five-pillar data observability monitor."""

    def __init__(self):
        self.metrics: list[PipelineMetric] = []

    def record(self, pipeline: str, stage: str, name: str, value: float):
        self.metrics.append(PipelineMetric(pipeline, stage, name, value))

    def get_history(self, pipeline: str, stage: str,
                    name: str) -> list[float]:
        return [
            m.value for m in self.metrics
            if m.pipeline == pipeline
            and m.stage == stage
            and m.metric_name == name
        ]

    def detect_anomaly(self, values: list[float], current: float,
                       z_threshold: float = 3.0) -> bool:
        if len(values) < 5:
            return False
        mean = statistics.mean(values)
        stdev = statistics.stdev(values)
        if stdev == 0:
            return False
        return abs(current - mean) / stdev > z_threshold

    def check_freshness(self, last_update: datetime,
                        max_hours: float) -> bool:
        age = datetime.now() - last_update
        return age.total_seconds() / 3600 <= max_hours

    def compute_slis(self, pipeline: str) -> list[SLI]:
        """Compute SLIs for a pipeline based on recorded metrics."""
        slis = []
        stages = {m.stage for m in self.metrics if m.pipeline == pipeline}
        for stage in stages:
            # Completion rate SLI
            completions = self.get_history(pipeline, stage, "completed")
            if completions:
                rate = sum(completions) / len(completions)
                slis.append(SLI(f"{stage}_completion_rate", 0.99, rate))

            # Latency SLI
            latencies = self.get_history(pipeline, stage, "duration_sec")
            if latencies:
                p99 = sorted(latencies)[int(len(latencies) * 0.99)]
                slis.append(SLI(f"{stage}_p99_latency", 300.0, p99))

        return slis


def main():
    print("=== Chapter 14: Data Observability ===\n")

    monitor = ObservabilityMonitor()

    # Simulate historical metrics
    for i in range(20):
        monitor.record("daily_etl", "extract", "row_count", 10000 + i * 50)
        monitor.record("daily_etl", "extract", "duration_sec", 120 + i * 2)
        monitor.record("daily_etl", "extract", "completed", 1.0)
        monitor.record("daily_etl", "transform", "row_count", 9500 + i * 45)
        monitor.record("daily_etl", "transform", "duration_sec", 180 + i * 3)
        monitor.record("daily_etl", "transform", "completed", 1.0)

    # Check for anomalies
    print("Anomaly detection:")
    history = monitor.get_history("daily_etl", "extract", "row_count")
    anomalous_value = 2000  # Way below normal
    is_anom = monitor.detect_anomaly(history, anomalous_value)
    print(f"  Extract row_count={anomalous_value}: anomaly={is_anom}")

    normal_value = 11000
    is_anom = monitor.detect_anomaly(history, normal_value)
    print(f"  Extract row_count={normal_value}: anomaly={is_anom}")
    print()

    # Freshness check
    print("Freshness checks:")
    recent = datetime.now() - timedelta(hours=2)
    stale = datetime.now() - timedelta(hours=48)
    print(f"  Updated 2h ago, max 6h: fresh={monitor.check_freshness(recent, 6)}")
    print(f"  Updated 48h ago, max 6h: fresh={monitor.check_freshness(stale, 6)}")
    print()

    # SLIs
    print("Pipeline SLIs:")
    slis = monitor.compute_slis("daily_etl")
    for sli in slis:
        status = "✓" if sli.met else "✗"
        print(f"  {status} {sli.name}: target={sli.target}, actual={sli.actual:.2f}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
