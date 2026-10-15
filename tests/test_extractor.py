"""
Unit tests for Extractor module.
"""

import pytest
from pathlib import Path
from src.engine.extractor import Extractor, ExtractionError


def test_extract_valid_csv(sample_data_dir):
    csv_path = sample_data_dir / "customers_valid.csv"
    records = Extractor.extract_from_file(csv_path)
    assert len(records) == 5
    assert records[0]["customer_id"] == "CUST1001"
    assert records[0]["country"] == "USA"


def test_extract_valid_json(sample_data_dir):
    json_path = sample_data_dir / "customers_valid.json"
    records = Extractor.extract_from_file(json_path)
    assert len(records) == 3
    assert records[0]["customer_id"] == "CUST2001"
    assert records[0]["name"] == "Fiona Gallagher"


def test_extract_empty_csv(sample_data_dir):
    empty_csv = sample_data_dir / "customers_empty.csv"
    records = Extractor.extract_from_file(empty_csv)
    assert records == []


def test_extract_empty_text():
    records = Extractor.extract_from_text("", ".csv")
    assert records == []
    records_json = Extractor.extract_from_text("   \n ", ".json")
    assert records_json == []


def test_extract_malformed_json():
    with pytest.raises(ExtractionError, match="Malformed JSON syntax"):
        Extractor.extract_from_text('{"customer_id": "123", broken}', ".json")


def test_extract_unsupported_extension(tmp_path):
    bad_file = tmp_path / "data.xml"
    bad_file.write_text("<data></data>")
    with pytest.raises(ExtractionError, match="Unsupported file format"):
        Extractor.extract_from_file(bad_file)


def test_extract_missing_file():
    with pytest.raises(ExtractionError, match="Input file not found"):
        Extractor.extract_from_file("non_existent_file_path.csv")
