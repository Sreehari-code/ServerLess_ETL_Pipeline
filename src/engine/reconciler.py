"""
Reconciliation & Audit Engine.

Aggregates execution metrics, calculates data quality scores, and enforces
the fundamental reconciliation invariant: Total = Accepted + Rejected.
Schema-agnostic and records validation mode, warnings, and detected schema.
"""

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from src.models.record import RejectedRecord, ReconciliationReport


class Reconciler:
    """Computes reconciliation reports and verifies accounting invariants."""

    @classmethod
    def generate_report(
        cls,
        run_id: str,
        source_file: str,
        total_input_records: int,
        accepted: List[Any],
        rejected: List[RejectedRecord],
        duration_seconds: float,
        started_at: datetime,
        completed_at: datetime,
        validation_mode: str = "auto",
        schema_name: Optional[str] = None,
        detected_schema: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[str]] = None,
        potential_identifier: Optional[str] = None,
    ) -> ReconciliationReport:
        """
        Generate an audited reconciliation report.

        Args:
            run_id: Unique identifier for this execution.
            source_file: Name or path of the processed input file.
            total_input_records: Total count of records parsed from source.
            accepted: List of successfully transformed record objects (CustomerRecord or DataRecord).
            rejected: List of RejectedRecord objects.
            duration_seconds: Wall-clock processing time in seconds.
            started_at: UTC start timestamp.
            completed_at: UTC completion timestamp.
            validation_mode: "auto" or "configured".
            schema_name: Identifier name of applied schema.
            detected_schema: Serialized schema detection metadata.
            warnings: List of non-fatal operational warnings.
            potential_identifier: Detected unique primary key name.

        Returns:
            ReconciliationReport instance.
        """
        accepted_count = len(accepted)
        rejected_count = len(rejected)

        # Audit duplicate records (generic: flags any 'Duplicate <col>:' rejection)
        duplicate_count = 0
        error_counter: Counter = Counter()

        for rec in rejected:
            for reason in rec.rejection_reasons:
                if "duplicate" in reason.lower():
                    duplicate_count += 1
                # Categorize main error prefix
                category = reason.split(":")[0] if ":" in reason else reason
                error_counter[category.strip()] += 1

        # Strict invariant verification
        calculated_sum = accepted_count + rejected_count
        if calculated_sum != total_input_records:
            raise ValueError(
                f"Reconciliation invariant violated: input records ({total_input_records}) "
                f"!= accepted ({accepted_count}) + rejected ({rejected_count})"
            )

        # Data Quality Score (0.0 to 100.0%)
        if total_input_records > 0:
            quality_score = round((accepted_count / total_input_records) * 100.0, 2)
        else:
            quality_score = 100.0

        # Processing Status
        if total_input_records == 0:
            status = "SUCCESS_EMPTY"
        elif accepted_count == total_input_records:
            status = "SUCCESS"
        elif accepted_count > 0 and rejected_count > 0:
            status = "PARTIAL_SUCCESS"
        else:
            status = "FAILED"

        return ReconciliationReport(
            run_id=run_id,
            source_file=source_file,
            total_input_records=total_input_records,
            accepted_records=accepted_count,
            rejected_records=rejected_count,
            duplicate_records=duplicate_count,
            data_quality_score=quality_score,
            processing_status=status,
            processing_duration_seconds=round(duration_seconds, 4),
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            error_summary=dict(error_counter),
            validation_mode=validation_mode,
            schema_name=schema_name,
            detected_schema=detected_schema,
            warnings=warnings or [],
            potential_identifier=potential_identifier,
        )
