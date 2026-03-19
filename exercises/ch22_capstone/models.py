"""Core data models for the lineage platform."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class NodeType(Enum):
    DATASET = "DATASET"
    JOB = "JOB"
    RUN = "RUN"


class Classification(Enum):
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"


@dataclass
class LineageNode:
    """A node in the lineage graph."""
    id: str
    name: str
    namespace: str
    node_type: NodeType
    metadata: dict = field(default_factory=dict)
    classification: Classification = Classification.INTERNAL
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class LineageEdge:
    """An edge connecting two nodes."""
    source_id: str
    target_id: str
    edge_type: str = "DERIVED_FROM"
    metadata: dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class QualityMetrics:
    """Quality metrics for a dataset."""
    dataset_id: str
    row_count: int = 0
    null_rates: dict[str, float] = field(default_factory=dict)
    freshness_hours: float = 0.0
    measured_at: datetime = field(default_factory=datetime.now)


@dataclass
class DataSLA:
    """SLA for a dataset."""
    dataset_id: str
    freshness_max_hours: float = 24.0
    min_row_count: int = 0
    max_null_rate: float = 0.1
