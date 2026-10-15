"""
Data Models and Schema Definitions.

Defines schemas for customer records, generic data records, validation results,
and audited reconciliation reports.
"""

from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from src.models.schema import (
    FieldDefinition,
    DatasetSchema,
    SchemaInferenceResult,
    DataRecord,
)


@dataclass
class ValidationResult:
    """Outcome of validating a single record."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class CustomerRecord:
    """Validated and standardized customer record (backward-compatible domain model)."""
    customer_id: str
    name: str
    email: str
    age: int
    country: str
    processed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to standard dictionary."""
        return asdict(self)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


@dataclass
class RejectedRecord:
    """Record rejected during pipeline validation with audit metadata."""
    original_data: Dict[str, Any]
    rejection_reasons: List[str]
    rejected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Flatten or format for serialized output."""
        return {
            "_rejection_reasons": self.rejection_reasons,
            "_rejected_at": self.rejected_at,
            **self.original_data
        }


@dataclass
class ReconciliationReport:
    """
    Comprehensive reconciliation and audit summary for a pipeline run.
    Ensures the invariant: total_input_records = accepted_records + rejected_records.
    """
    run_id: str
    source_file: str
    total_input_records: int
    accepted_records: int
    rejected_records: int
    duplicate_records: int
    data_quality_score: float
    processing_status: str
    processing_duration_seconds: float
    started_at: str
    completed_at: str
    invariant_holds: bool = True
    error_summary: Dict[str, int] = field(default_factory=dict)
    validation_mode: str = "auto"  # "auto" or "configured"
    schema_name: Optional[str] = None
    detected_schema: Optional[Dict[str, Any]] = None
    warnings: List[str] = field(default_factory=list)
    potential_identifier: Optional[str] = None

    def __post_init__(self):
        # Validate invariant: total == accepted + rejected
        self.invariant_holds = (self.total_input_records == (self.accepted_records + self.rejected_records))

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return asdict(self)
