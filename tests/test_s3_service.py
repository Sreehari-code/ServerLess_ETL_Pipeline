"""
Unit tests for S3Service using mocks.
Tests are completely isolated from AWS credentials and network calls.
"""

from unittest.mock import MagicMock, patch
import pytest
from src.aws.s3_service import S3Service, S3ServiceError
from src.models.record import CustomerRecord, RejectedRecord, ReconciliationReport


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client."""
    client = MagicMock()
    return client


def test_s3_is_configured_true(mock_s3_client):
    service = S3Service(bucket_name="test-bucket")
    with patch.object(service, "_get_client", return_value=mock_s3_client):
        mock_s3_client.head_bucket.return_value = {}
        assert service.is_configured() is True
        mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")


def test_s3_is_configured_false_no_bucket():
    service = S3Service(bucket_name="")
    assert service.is_configured() is False


def test_s3_list_input_objects(mock_s3_client):
    service = S3Service(bucket_name="test-bucket")
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "input/customers.csv", "Size": 100},
            {"Key": "input/orders.json", "Size": 200},
            {"Key": "input/notes.txt", "Size": 50},       # Should be filtered out (.txt)
            {"Key": "input/", "Size": 0},                  # Should be filtered out (dir marker)
        ]
    }

    with patch.object(service, "_get_client", return_value=mock_s3_client):
        objects = service.list_input_objects(prefix="input/")
        assert objects == ["input/customers.csv", "input/orders.json"]
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="input/"
        )


def test_s3_download_file(mock_s3_client, tmp_path):
    service = S3Service(bucket_name="test-bucket")
    dest_path = str(tmp_path / "downloaded.csv")

    with patch.object(service, "_get_client", return_value=mock_s3_client):
        result = service.download_file("input/customers.csv", destination_path=dest_path)
        assert result == dest_path
        mock_s3_client.download_file.assert_called_once_with(
            "test-bucket", "input/customers.csv", dest_path
        )


def test_s3_upload_file_enforces_encryption(mock_s3_client, tmp_path):
    local_file = tmp_path / "test.json"
    local_file.write_text("{}")

    service = S3Service(bucket_name="test-bucket")
    with patch.object(service, "_get_client", return_value=mock_s3_client):
        res_key = service.upload_file(str(local_file), "output/test.json", content_type="application/json")
        assert res_key == "output/test.json"
        mock_s3_client.upload_file.assert_called_once_with(
            str(local_file),
            "test-bucket",
            "output/test.json",
            ExtraArgs={"ServerSideEncryption": "AES256", "ContentType": "application/json"}
        )


def test_s3_upload_run_outputs(mock_s3_client):
    service = S3Service(bucket_name="test-bucket")

    accepted = [
        CustomerRecord("CUST1", "Alice", "alice@test.com", 30, "USA")
    ]
    rejected = [
        RejectedRecord({"customer_id": "CUST2"}, ["Invalid email"])
    ]
    report = ReconciliationReport(
        run_id="run_123",
        source_file="test.csv",
        total_input_records=2,
        accepted_records=1,
        rejected_records=1,
        duplicate_records=0,
        data_quality_score=50.0,
        processing_status="PARTIAL_SUCCESS",
        processing_duration_seconds=0.01,
        started_at="2026-10-15T00:00:00Z",
        completed_at="2026-10-15T00:00:01Z"
    )

    with patch.object(service, "_get_client", return_value=mock_s3_client):
        keys = service.upload_run_outputs(
            run_id="run_123",
            accepted=accepted,
            rejected=rejected,
            report=report
        )

        assert "output/accepted/accepted_run_123.json" in keys["accepted_s3_key"]
        assert "output/rejected/rejected_run_123.json" in keys["rejected_s3_key"]
        assert "output/reports/report_run_123.json" in keys["report_s3_key"]
        assert mock_s3_client.put_object.call_count == 3
