"""
Unit tests for CustomerValidator module.
"""

import pytest
from src.engine.validator import CustomerValidator


@pytest.fixture
def validator():
    return CustomerValidator()


def test_validator_valid_record(validator, sample_valid_record):
    res = validator.validate_record(sample_valid_record)
    assert res.is_valid is True
    assert res.errors == []


def test_validator_missing_required_column(validator):
    incomplete_record = {
        "customer_id": "CUST_100",
        "email": "user@example.com",
        "age": 25,
        "country": "USA"
        # missing "name"
    }
    res = validator.validate_record(incomplete_record)
    assert res.is_valid is False
    assert any("Missing required field: 'name'" in err for err in res.errors)


def test_validator_blank_field_value(validator):
    blank_name = {
        "customer_id": "CUST_101",
        "name": "   ",
        "email": "user@example.com",
        "age": 25,
        "country": "USA"
    }
    res = validator.validate_record(blank_name)
    assert res.is_valid is False
    assert any("Required field 'name' cannot be empty or blank" in err for err in res.errors)


@pytest.mark.parametrize("invalid_email", [
    "not-an-email",
    "missingat.com",
    "@missingusername.com",
    "user@nodomain",
    "user name@domain.com",
])
def test_validator_invalid_email_format(validator, invalid_email):
    record = {
        "customer_id": "CUST_102",
        "name": "Test User",
        "email": invalid_email,
        "age": 30,
        "country": "USA"
    }
    res = validator.validate_record(record)
    assert res.is_valid is False
    assert any(f"Invalid email format: '{invalid_email}'" in err for err in res.errors)


@pytest.mark.parametrize("invalid_age", [
    -5,
    "-10",
    125,
    "150",
    "twenty-five",
    "30.5",
])
def test_validator_invalid_age(validator, invalid_age):
    record = {
        "customer_id": "CUST_103",
        "name": "Test User",
        "email": "test@example.com",
        "age": invalid_age,
        "country": "USA"
    }
    res = validator.validate_record(record)
    assert res.is_valid is False
    assert any("Invalid age" in err for err in res.errors)


def test_validator_duplicate_customer_id(validator):
    rec1 = {
        "customer_id": "CUST_DUP_1",
        "name": "First Record",
        "email": "first@test.com",
        "age": 30,
        "country": "USA"
    }
    rec2 = {
        "customer_id": "CUST_DUP_1",
        "name": "Duplicate Record",
        "email": "dup@test.com",
        "age": 35,
        "country": "Canada"
    }

    res1 = validator.validate_record(rec1)
    assert res1.is_valid is True

    res2 = validator.validate_record(rec2)
    assert res2.is_valid is False
    assert any("Duplicate customer_id: 'CUST_DUP_1'" in err for err in res2.errors)


def test_validator_reset_clears_duplicates(validator):
    rec = {
        "customer_id": "CUST_RESET",
        "name": "Reset Test",
        "email": "reset@test.com",
        "age": 28,
        "country": "USA"
    }
    res1 = validator.validate_record(rec)
    assert res1.is_valid is True

    validator.reset()

    # After reset, the same customer_id should be accepted again
    res2 = validator.validate_record(rec)
    assert res2.is_valid is True
