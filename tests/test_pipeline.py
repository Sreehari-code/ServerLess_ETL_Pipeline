"""
Unit and integration tests for the ETLPipeline orchestrator.
"""

import json
from pathlib import Path
from src.engine.pipeline import ETLPipeline


def test_pipeline_valid_csv(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path)
    csv_file = sample_data_dir / "customers_valid.csv"

    result = pipeline.process_file(csv_file, write_output=True)

    assert result.report.processing_status == "SUCCESS"
    assert result.report.total_input_records == 5
    assert result.report.accepted_records == 5
    assert result.report.rejected_records == 0
    assert result.report.duplicate_records == 0
    assert result.report.data_quality_score == 100.0

    # Invariant check
    assert result.report.total_input_records == (result.report.accepted_records + result.report.rejected_records)
    assert result.report.invariant_holds is True

    # Output files verified
    assert Path(result.output_files["accepted"]).exists()
    assert Path(result.output_files["rejected"]).exists()
    assert Path(result.output_files["report"]).exists()


def test_pipeline_mixed_csv(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path)
    mixed_file = sample_data_dir / "customers_mixed.csv"

    result = pipeline.process_file(mixed_file, write_output=True)

    assert result.report.processing_status == "PARTIAL_SUCCESS"
    assert result.report.total_input_records == 10
    assert result.report.accepted_records == 3  # John Doe, Sarah Connor, Valid User
    assert result.report.rejected_records == 7
    assert result.report.duplicate_records == 1  # Duplicate John Doe

    # Invariant holds
    assert result.report.total_input_records == (result.report.accepted_records + result.report.rejected_records)
    assert result.report.invariant_holds is True

    # Verify rejected record details
    assert len(result.rejected_records) == 7
    for rej in result.rejected_records:
        assert rej.original_data is not None
        assert len(rej.rejection_reasons) > 0

    # Verify report JSON file contains matching metrics
    report_data = json.loads(Path(result.output_files["report"]).read_text(encoding="utf-8"))
    assert report_data["accepted_records"] == 3
    assert report_data["rejected_records"] == 7
    assert report_data["total_input_records"] == 10


def test_pipeline_empty_csv(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path)
    empty_file = sample_data_dir / "customers_empty.csv"

    result = pipeline.process_file(empty_file, write_output=True)

    assert result.report.processing_status == "SUCCESS_EMPTY"
    assert result.report.total_input_records == 0
    assert result.report.accepted_records == 0
    assert result.report.rejected_records == 0
    assert result.report.invariant_holds is True


def test_pipeline_csv_output_format(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path, default_format="csv")
    valid_file = sample_data_dir / "customers_valid.csv"

    result = pipeline.process_file(valid_file, write_output=True, output_format="csv")

    accepted_path = Path(result.output_files["accepted"])
    assert accepted_path.suffix == ".csv"
    assert accepted_path.exists()
    content = accepted_path.read_text(encoding="utf-8")
    assert "CUST1001" in content
