"""
Core ETL Pipeline Orchestrator.

Coordinates schema inference/detection, validation, transformation, reconciliation,
and partitioned output generation.
Schema-aware and decoupled from any specific business domain.
"""

import csv
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.engine.extractor import Extractor, ExtractionError
from src.engine.schema_inferrer import SchemaInferrer
from src.engine.validator import SchemaValidator, CustomerValidator
from src.engine.transformer import Transformer
from src.engine.reconciler import Reconciler
from src.models.schema import DatasetSchema, SchemaInferenceResult, DataRecord
from src.models.record import CustomerRecord, RejectedRecord, ReconciliationReport
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class PipelineResult:
    """Complete output and audit metadata from an ETL execution."""
    run_id: str
    report: ReconciliationReport
    accepted_records: List[Any] = field(default_factory=list)
    rejected_records: List[RejectedRecord] = field(default_factory=list)
    output_files: Dict[str, str] = field(default_factory=dict)
    detected_schema: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete result to dictionary."""
        return {
            "run_id": self.run_id,
            "report": self.report.to_dict(),
            "accepted_count": len(self.accepted_records),
            "rejected_count": len(self.rejected_records),
            "output_files": self.output_files,
            "detected_schema": self.detected_schema,
        }


class ETLPipeline:
    """
    Schema-aware ETL engine processing arbitrary tabular or document datasets.
    Supports Automatic Inference Mode and Configured Schema Mode.
    """

    def __init__(
        self,
        output_dir: Optional[Union[str, Path]] = None,
        default_format: str = "json",
        schema: Optional[DatasetSchema] = None,
        validation_mode: str = "auto",
    ):
        self.output_dir = Path(output_dir) if output_dir else Path("output")
        self.default_format = default_format.lower()
        self.schema = schema
        self.validation_mode = validation_mode.lower()
        self.validator = SchemaValidator(schema=schema, mode=self.validation_mode)

    def process_file(
        self,
        file_path: Union[str, Path],
        write_output: bool = True,
        output_format: Optional[str] = None,
        schema: Optional[DatasetSchema] = None,
        validation_mode: Optional[str] = None,
    ) -> PipelineResult:
        """
        Execute pipeline on a local file path.

        Args:
            file_path: Target input file path.
            write_output: Whether to persist outputs to disk.
            output_format: 'json' or 'csv'.
            schema: Optional schema override.
            validation_mode: Optional mode override ('auto' or 'configured').

        Returns:
            PipelineResult containing records, detected schema, and reconciliation report.
        """
        path = Path(file_path)
        source_name = path.name
        ext = path.suffix.lower()

        logger.info(f"Starting ETL execution for file: {path.name}")
        raw_records = Extractor.extract_from_file(path)

        # Extract raw headers for header duplicate detection
        raw_headers = None
        try:
            content_sample = path.read_text(encoding="utf-8-sig")
            raw_headers = Extractor.extract_headers(content_sample, ext)
        except Exception:
            pass

        return self._execute_pipeline(
            raw_records=raw_records,
            source_file=source_name,
            write_output=write_output,
            output_format=output_format or self.default_format,
            raw_headers=raw_headers,
            schema_override=schema,
            mode_override=validation_mode,
        )

    def process_text(
        self,
        content: str,
        source_name: str,
        file_extension: str,
        write_output: bool = False,
        output_format: Optional[str] = None,
        schema: Optional[DatasetSchema] = None,
        validation_mode: Optional[str] = None,
    ) -> PipelineResult:
        """
        Execute pipeline on raw in-memory text.

        Args:
            content: Raw string content of input.
            source_name: Reference name of source.
            file_extension: '.csv' or '.json'.
            write_output: Whether to write outputs to disk.
            output_format: 'json' or 'csv'.
            schema: Optional schema override.
            validation_mode: Optional mode override ('auto' or 'configured').

        Returns:
            PipelineResult.
        """
        logger.info(f"Starting ETL execution for in-memory stream: {source_name}")
        raw_records = Extractor.extract_from_text(content, file_extension)
        raw_headers = Extractor.extract_headers(content, file_extension)

        return self._execute_pipeline(
            raw_records=raw_records,
            source_file=source_name,
            write_output=write_output,
            output_format=output_format or self.default_format,
            raw_headers=raw_headers,
            schema_override=schema,
            mode_override=validation_mode,
        )

    def _execute_pipeline(
        self,
        raw_records: List[Dict[str, Any]],
        source_file: str,
        write_output: bool,
        output_format: str,
        raw_headers: Optional[List[str]] = None,
        schema_override: Optional[DatasetSchema] = None,
        mode_override: Optional[str] = None,
    ) -> PipelineResult:
        """Core execution logic shared across file and text inputs."""
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        started_at = datetime.now(timezone.utc)
        start_time = time.perf_counter()

        mode = (mode_override or self.validation_mode).lower()
        active_schema = schema_override or self.schema
        inference_result: Optional[SchemaInferenceResult] = None
        warnings: List[str] = []

        total_input = len(raw_records)
        logger.info(f"[{run_id}] Extracted {total_input} records from {source_file}")

        # Resolve Schema
        if active_schema is None or mode == "auto":
            # Automatic Mode: infer schema dynamically
            inference_result = SchemaInferrer.infer_schema(
                records=raw_records,
                raw_headers=raw_headers,
            )
            warnings.extend(inference_result.warnings)

            if active_schema is None:
                # Check if dataset matches classic customer fields
                detected_cols = set(inference_result.detected_columns)
                if {"customer_id", "name", "email", "age", "country"}.issubset(detected_cols):
                    active_schema = DatasetSchema.preset_customer()
                else:
                    active_schema = inference_result.inferred_schema
        else:
            # Configured Schema Mode: user supplied an explicit schema
            mode = "configured"

        # Configure validator with the active schema
        self.validator.set_schema(active_schema, mode=mode)
        self.validator.reset()

        accepted: List[Any] = []
        rejected: List[RejectedRecord] = []

        # Process each record
        for raw in raw_records:
            val_res = self.validator.validate_record(raw)
            if val_res.is_valid:
                # Use domain-specific customer model if customer schema, else generic DataRecord
                if active_schema.name == "customer_schema":
                    transformed = Transformer.transform_record(raw)
                else:
                    transformed = Transformer.transform_generic(raw, schema=active_schema)
                accepted.append(transformed)
            else:
                rejected.append(
                    RejectedRecord(
                        original_data=raw,
                        rejection_reasons=val_res.errors
                    )
                )

        duration = time.perf_counter() - start_time
        completed_at = datetime.now(timezone.utc)

        # Generate reconciliation report and assert accounting invariant
        report = Reconciler.generate_report(
            run_id=run_id,
            source_file=source_file,
            total_input_records=total_input,
            accepted=accepted,
            rejected=rejected,
            duration_seconds=duration,
            started_at=started_at,
            completed_at=completed_at,
            validation_mode=mode,
            schema_name=active_schema.name if active_schema else None,
            detected_schema=active_schema.to_dict() if active_schema else None,
            warnings=warnings,
            potential_identifier=active_schema.identifier_field if active_schema else None,
        )

        logger.info(
            f"[{run_id}] Processing complete in {report.processing_duration_seconds}s "
            f"[{mode.upper()} mode, schema='{active_schema.name}']: "
            f"Total={report.total_input_records}, Accepted={report.accepted_records}, "
            f"Rejected={report.rejected_records}, Status={report.processing_status}"
        )

        output_files: Dict[str, str] = {}
        if write_output:
            output_files = self._write_outputs(
                run_id=run_id,
                accepted=accepted,
                rejected=rejected,
                report=report,
                output_format=output_format
            )

        return PipelineResult(
            run_id=run_id,
            report=report,
            accepted_records=accepted,
            rejected_records=rejected,
            output_files=output_files,
            detected_schema=active_schema.to_dict() if active_schema else None,
        )

    def _write_outputs(
        self,
        run_id: str,
        accepted: List[Any],
        rejected: List[RejectedRecord],
        report: ReconciliationReport,
        output_format: str
    ) -> Dict[str, str]:
        """Persist results to local filesystem."""
        fmt = output_format.lower()
        if fmt not in {"csv", "json"}:
            fmt = "json"

        accepted_dir = self.output_dir / "accepted"
        rejected_dir = self.output_dir / "rejected"
        reports_dir = self.output_dir / "reports"

        accepted_dir.mkdir(parents=True, exist_ok=True)
        rejected_dir.mkdir(parents=True, exist_ok=True)
        reports_dir.mkdir(parents=True, exist_ok=True)

        accepted_file = accepted_dir / f"accepted_{run_id}.{fmt}"
        rejected_file = rejected_dir / f"rejected_{run_id}.{fmt}"
        report_file = reports_dir / f"report_{run_id}.json"

        # 1. Write Accepted (dynamic fields for any schema)
        accepted_dicts = [rec.to_dict() for rec in accepted]
        if fmt == "csv":
            if accepted_dicts:
                # Collect all field names dynamically
                all_accepted_keys = []
                for rec in accepted_dicts:
                    for k in rec.keys():
                        if k not in all_accepted_keys:
                            all_accepted_keys.append(k)
            else:
                all_accepted_keys = ["id", "_processed_at"]
            self._write_csv(accepted_file, accepted_dicts, fieldnames=all_accepted_keys)
        else:
            accepted_file.write_text(json.dumps(accepted_dicts, indent=2), encoding="utf-8")

        # 2. Write Rejected (dynamic fields for any schema)
        rejected_dicts = [rec.to_dict() for rec in rejected]
        if fmt == "csv":
            flattened_rejected = []
            for r in rejected:
                flat = {**r.original_data}
                flat["_rejection_reasons"] = "; ".join(r.rejection_reasons)
                flat["_rejected_at"] = r.rejected_at
                flattened_rejected.append(flat)
            all_fields = list({k: True for d in flattened_rejected for k in d.keys()}.keys()) if flattened_rejected else ["_rejection_reasons"]
            self._write_csv(rejected_file, flattened_rejected, fieldnames=all_fields)
        else:
            rejected_file.write_text(json.dumps(rejected_dicts, indent=2), encoding="utf-8")

        # 3. Write Reconciliation Report
        report_file.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")

        logger.info(f"[{run_id}] Wrote outputs to: {self.output_dir}")

        return {
            "accepted": str(accepted_file),
            "rejected": str(rejected_file),
            "report": str(report_file),
        }

    @staticmethod
    def _write_csv(path: Path, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        """Write records to CSV safely."""
        with open(path, mode="w", newline="", encoding="utf-8") as f:
            if not records:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                return

            seen_keys = list(fieldnames)
            for r in records:
                for k in r.keys():
                    if k not in seen_keys:
                        seen_keys.append(k)

            writer = csv.DictWriter(f, fieldnames=seen_keys)
            writer.writeheader()
            for r in records:
                writer.writerow(r)
