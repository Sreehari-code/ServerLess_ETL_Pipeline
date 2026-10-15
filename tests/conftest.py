"""
Pytest configuration and shared test fixtures.
"""

import json
import pytest
from pathlib import Path


@pytest.fixture
def sample_valid_record():
    """Returns a valid customer record dictionary."""
    return {
        "customer_id": "CUST_TEST_001",
        "name": "Jane Developer",
        "email": "jane.dev@example.com",
        "age": 30,
        "country": "United States"
    }


@pytest.fixture
def sample_data_dir():
    """Path to sample data directory."""
    return Path(__file__).parent.parent / "sample_data"


@pytest.fixture
def sample_s3_event():
    """Load mock S3 event fixture from test_fixtures."""
    fixture_path = Path(__file__).parent.parent / "test_fixtures" / "s3_event.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))
