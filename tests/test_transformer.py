"""
Unit tests for Transformer module.
"""

from src.engine.transformer import Transformer


def test_transform_valid_record_whitespace_and_lowercase():
    raw = {
        "customer_id": "  CUST_TR_1   ",
        "name": "   Alice in Wonderland  ",
        "email": "  ALICE.WONDER@EXAMPLE.COM   ",
        "age": "28.0",
        "country": "  United States  "
    }
    record = Transformer.transform_record(raw)

    assert record.customer_id == "CUST_TR_1"
    assert record.name == "Alice in Wonderland"
    assert record.email == "alice.wonder@example.com"
    assert record.age == 28
    assert record.country == "USA"
    assert record.processed_at is not None


def test_transform_country_mappings():
    test_cases = [
        ("UK", "GBR"),
        ("United Kingdom", "GBR"),
        ("Great Britain", "GBR"),
        ("US", "USA"),
        ("United States of America", "USA"),
        ("India", "IND"),
        ("Canada", "CAN"),
        ("Germany", "DEU"),
        ("Deutschland", "DEU"),
        ("France", "FRA"),
        ("Australia", "AUS"),
        ("Japan", "JPN"),
        ("Singapore", "SGP"),
        ("BRAZIL", "BRAZIL"),  # Unmapped retains trimmed uppercase
    ]

    for raw_country, expected in test_cases:
        raw = {
            "customer_id": "CUST_1",
            "name": "Name",
            "email": "test@test.com",
            "age": 30,
            "country": raw_country
        }
        res = Transformer.transform_record(raw)
        assert res.country == expected
