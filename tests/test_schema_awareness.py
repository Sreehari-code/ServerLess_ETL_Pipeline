"""
Unit and regression tests for Schema-Aware Validation and Inference.

Verifies:
1. Automatic schema inference on non-customer datasets (Employee, Sales, Arbitrary IoT).
2. Configured-schema mode with strict validation and custom constraints.
3. Safe acceptance of optional fields with missing values.
4. Accurate detection and rejection of genuine type failures and duplicate identifiers.
5. Invariant preservation: total_input = accepted + rejected.
"""

import json
import pytest
from pathlib import Path

from src.engine.pipeline import ETLPipeline
from src.engine.schema_inferrer import SchemaInferrer
from src.models.schema import DatasetSchema, FieldDefinition


def test_schema_inferrer_employee():
    sample_records = [
        {"employee_id": "EMP1", "employee_name": "Alice", "salary": "75000.50", "department": "Engineering"},
        {"employee_id": "EMP2", "employee_name": "Bob", "salary": "82000.00", "department": "Marketing"},
    ]
    res = SchemaInferrer.infer_schema(sample_records)
    assert "employee_id" in res.detected_columns
    assert res.inferred_types["employee_id"] == "string"
    assert res.inferred_types["salary"] == "float"
    assert res.potential_identifier == "employee_id"


def test_schema_inferrer_sales():
    sample_records = [
        {"order_id": "ORD-1", "product": "Widget", "quantity": "5", "price": "19.99"},
        {"order_id": "ORD-2", "product": "Gadget", "quantity": "10", "price": "49.50"},
    ]
    res = SchemaInferrer.infer_schema(sample_records)
    assert "order_id" in res.detected_columns
    assert res.inferred_types["quantity"] == "integer"
    assert res.inferred_types["price"] == "float"
    assert res.potential_identifier == "order_id"


def test_pipeline_employee_auto_mode(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path, validation_mode="auto")
    emp_file = sample_data_dir / "employees_mixed.csv"

    result = pipeline.process_file(emp_file, write_output=True)

    # Invariant check
    assert result.report.invariant_holds is True
    assert result.report.total_input_records == 7
    assert result.report.accepted_records == 5
    assert result.report.rejected_records == 2

    # Verify duplicate employee_id EMP201 was rejected
    rejections = [rej.rejection_reasons for rej in result.rejected_records]
    assert any("Duplicate employee_id" in r[0] for r in rejections)

    # Verify invalid salary 'not_a_salary' was rejected
    assert any("Invalid number for 'salary'" in r[0] for r in rejections)

    # Verify output file generated
    assert Path(result.output_files["accepted"]).exists()
    assert Path(result.output_files["rejected"]).exists()
    assert Path(result.output_files["report"]).exists()


def test_pipeline_employee_configured_mode(sample_data_dir, tmp_path):
    schema = DatasetSchema.preset_employee()
    pipeline = ETLPipeline(output_dir=tmp_path, schema=schema, validation_mode="configured")
    emp_file = sample_data_dir / "employees_mixed.csv"

    result = pipeline.process_file(emp_file, write_output=True)

    assert result.report.invariant_holds is True
    assert result.report.total_input_records == 7
    assert result.report.accepted_records == 3
    assert result.report.rejected_records == 4

    # Configured mode strictly rejected:
    # 1. not_a_salary (type failure)
    # 2. blank employee_name (required failure)
    # 3. negative salary -15000 (min_value bound failure)
    # 4. duplicate employee_id EMP201 (uniqueness failure)
    rejection_strings = "; ".join([r for rej in result.rejected_records for r in rej.rejection_reasons])
    assert "Invalid number for 'salary'" in rejection_strings
    assert "Required field 'employee_name' cannot be empty or blank" in rejection_strings
    assert "below minimum" in rejection_strings
    assert "Duplicate employee_id" in rejection_strings


def test_pipeline_sales_auto_mode(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path, validation_mode="auto")
    sales_file = sample_data_dir / "sales_mixed.csv"

    result = pipeline.process_file(sales_file, write_output=True)

    assert result.report.invariant_holds is True
    assert result.report.total_input_records == 6
    assert result.report.accepted_records == 4
    assert result.report.rejected_records == 2

    # Quantity string 'three' rejected, duplicate order_id ORD-701 rejected
    rejection_strings = "; ".join([r for rej in result.rejected_records for r in rej.rejection_reasons])
    assert "Invalid integer for 'quantity'" in rejection_strings
    assert "Duplicate order_id" in rejection_strings


def test_pipeline_arbitrary_iot_sensor(sample_data_dir, tmp_path):
    pipeline = ETLPipeline(output_dir=tmp_path, validation_mode="auto")
    sensor_file = sample_data_dir / "sensor_readings.csv"

    result = pipeline.process_file(sensor_file, write_output=True)

    assert result.report.invariant_holds is True
    assert result.report.total_input_records == 5
    assert result.report.accepted_records == 4
    assert result.report.rejected_records == 1
    assert "reading_value" in result.rejected_records[0].rejection_reasons[0]


def test_pipeline_missing_optional_values_accepted(tmp_path):
    """Optional fields should not cause false record rejections."""
    schema = DatasetSchema(name="optional_test_schema")
    schema.add_field(FieldDefinition("id", data_type="string", required=True, unique=True))
    schema.add_field(FieldDefinition("notes", data_type="string", required=False))

    records = [
        {"id": "REC-01", "notes": "Has note"},
        {"id": "REC-02", "notes": ""},  # Empty optional note
        {"id": "REC-03"},               # Missing optional note key
    ]

    pipeline = ETLPipeline(output_dir=tmp_path, schema=schema, validation_mode="configured")
    result = pipeline.process_text(json.dumps(records), "test.json", ".json", write_output=False)

    assert result.report.total_input_records == 3
    assert result.report.accepted_records == 3
    assert result.report.rejected_records == 0


def test_pipeline_missing_required_column_in_configured_mode(tmp_path):
    """Configured schema with missing required column should reject affected records."""
    schema = DatasetSchema(name="strict_schema")
    schema.add_field(FieldDefinition("id", data_type="string", required=True))
    schema.add_field(FieldDefinition("must_have", data_type="string", required=True))

    records = [
        {"id": "1", "must_have": "value"},
        {"id": "2"},  # Missing 'must_have'
    ]

    pipeline = ETLPipeline(output_dir=tmp_path, schema=schema, validation_mode="configured")
    result = pipeline.process_text(json.dumps(records), "test.json", ".json", write_output=False)

    assert result.report.total_input_records == 2
    assert result.report.accepted_records == 1
    assert result.report.rejected_records == 1
    assert any("Missing required field: 'must_have'" in r for r in result.rejected_records[0].rejection_reasons)
