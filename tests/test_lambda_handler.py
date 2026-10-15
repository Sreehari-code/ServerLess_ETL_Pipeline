"""
Unit tests for the AWS Lambda handler.
Verifies event structure parsing, URL decoding, recursion guards, and responses.
"""

import json
from unittest.mock import MagicMock, patch
from src.aws.lambda_handler import lambda_handler


def test_lambda_empty_event():
    resp = lambda_handler({}, None)
    assert resp["statusCode"] == 400
    body = json.loads(resp["body"])
    assert "error" in body


def test_lambda_recursion_guard_skips_output_key():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "output/accepted/accepted_run_123.json"}
                }
            }
        ]
    }
    resp = lambda_handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["results"][0]["status"] == "SKIPPED_RECURSION_GUARD"


def test_lambda_skips_unmatched_prefix():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "staging/somefile.csv"}
                }
            }
        ]
    }
    resp = lambda_handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["results"][0]["status"] == "SKIPPED_UNMATCHED_PREFIX"


def test_lambda_skips_unsupported_extension():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "input/archive.zip"}
                }
            }
        ]
    }
    resp = lambda_handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["results"][0]["status"] == "SKIPPED_UNSUPPORTED_EXTENSION"


def test_lambda_success_with_url_decoding(sample_s3_event, tmp_path):
    # Sample event contains: "input/customers%2Bbatch%201.csv"
    # Create sample csv file to return from mock download
    sample_csv = tmp_path / "mock_download.csv"
    sample_csv.write_text(
        "customer_id,name,email,age,country\n"
        "CUST1,John,john@example.com,30,USA\n",
        encoding="utf-8"
    )

    with patch("src.aws.lambda_handler.S3Service") as mock_s3_service_cls:
        mock_instance = MagicMock()
        mock_s3_service_cls.return_value = mock_instance
        mock_instance.download_file.return_value = str(sample_csv)
        mock_instance.upload_run_outputs.return_value = {
            "accepted_s3_key": "output/accepted/accepted_1.json"
        }

        resp = lambda_handler(sample_s3_event, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["results"]) == 1

        res = body["results"][0]
        # Decoded URL should replace %20 with space and %2B with +
        assert res["key"] == "input/customers+batch 1.csv"
        assert res["status"] == "SUCCESS"
        assert res["metrics"]["accepted_records"] == 1
        assert res["metrics"]["total_input_records"] == 1
