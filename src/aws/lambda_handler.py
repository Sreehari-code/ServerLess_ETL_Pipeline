"""
AWS Lambda Function Handler.

Processes S3 ObjectCreated events, invokes the decoupled ETL pipeline,
persists outputs back to S3, and guards against recursive loop invocations.
"""

import json
import os
import sys
import tempfile
import urllib.parse
from typing import Any, Dict

# Ensure src and root are in python path for Lambda runtime
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "../.."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config.settings import get_settings
from src.aws.s3_service import S3Service, S3ServiceError
from src.engine.pipeline import ETLPipeline
from src.engine.extractor import ExtractionError
from src.utils.logger import setup_logger

logger = setup_logger("lambda_handler")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint triggered by S3 ObjectCreated events.

    Args:
        event: S3 event payload dictionary.
        context: Lambda context object with runtime info.

    Returns:
        Structured response dictionary containing status and processing summary.
    """
    logger.info("Lambda invocation received.")
    settings = get_settings()

    # 1. Validate event structure
    if not isinstance(event, dict) or "Records" not in event:
        logger.warning(f"Unrecognized or empty event payload structure: {event}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid event: missing 'Records' list"})
        }

    records = event.get("Records", [])
    if not records:
        logger.warning("Event contains an empty 'Records' list.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No records in event"})
        }

    results = []

    for idx, record in enumerate(records):
        s3_data = record.get("s3")
        if not s3_data:
            logger.warning(f"Record {idx} missing 's3' metadata.")
            continue

        bucket_name = s3_data.get("bucket", {}).get("name")
        raw_key = s3_data.get("object", {}).get("key")

        if not bucket_name or not raw_key:
            logger.error(f"Record {idx} missing bucket or object key.")
            continue

        # 2. Decode URL-encoded object key (e.g., 'customer+data%20v1.csv' -> 'customer data v1.csv')
        object_key = urllib.parse.unquote_plus(raw_key)
        logger.info(f"Target identified: s3://{bucket_name}/{object_key}")

        # 3. CRITICAL: Prevent recursive processing loops!
        # If the key resides in the output prefix or does not match input prefix, skip it!
        output_prefixes = [
            settings.s3_output_accepted_prefix,
            settings.s3_output_rejected_prefix,
            settings.s3_output_reports_prefix,
            "output/",
        ]
        if any(object_key.startswith(p) for p in output_prefixes):
            logger.warning(
                f"Skipping key '{object_key}' to prevent recursive loop; matches output prefix."
            )
            results.append({
                "key": object_key,
                "status": "SKIPPED_RECURSION_GUARD"
            })
            continue

        if not object_key.startswith(settings.s3_input_prefix):
            logger.warning(
                f"Skipping key '{object_key}': does not start with configured input prefix '{settings.s3_input_prefix}'"
            )
            results.append({
                "key": object_key,
                "status": "SKIPPED_UNMATCHED_PREFIX"
            })
            continue

        # Check supported file extensions
        if not object_key.lower().endswith((".csv", ".json")):
            logger.warning(f"Skipping unsupported file extension for key: {object_key}")
            results.append({
                "key": object_key,
                "status": "SKIPPED_UNSUPPORTED_EXTENSION"
            })
            continue

        # 4. Download file from S3 to Lambda's local /tmp directory
        s3_service = S3Service(bucket_name=bucket_name)
        temp_dest = None

        try:
            temp_dest = s3_service.download_file(object_key)
            logger.info(f"Downloaded '{object_key}' to temporary path: {temp_dest}")

            # 5. Invoke decoupled ETL engine
            # In Lambda, write intermediate files to /tmp
            lambda_output_dir = os.path.join(tempfile.gettempdir(), "etl_output")
            pipeline = ETLPipeline(output_dir=lambda_output_dir, default_format="json")

            pipeline_result = pipeline.process_file(temp_dest, write_output=False)
            run_id = pipeline_result.run_id
            report = pipeline_result.report

            logger.info(
                f"[{run_id}] ETL complete for '{object_key}': "
                f"Total={report.total_input_records}, "
                f"Accepted={report.accepted_records}, "
                f"Rejected={report.rejected_records}, "
                f"Duration={report.processing_duration_seconds}s"
            )

            # 6. Upload output artifacts to S3
            upload_summary = s3_service.upload_run_outputs(
                run_id=run_id,
                accepted=pipeline_result.accepted_records,
                rejected=pipeline_result.rejected_records,
                report=report
            )

            results.append({
                "key": object_key,
                "run_id": run_id,
                "status": "SUCCESS",
                "metrics": {
                    "total_input_records": report.total_input_records,
                    "accepted_records": report.accepted_records,
                    "rejected_records": report.rejected_records,
                    "duplicate_records": report.duplicate_records,
                    "data_quality_score": report.data_quality_score,
                    "duration_seconds": report.processing_duration_seconds,
                    "validation_mode": report.validation_mode,
                    "schema_name": report.schema_name,
                    "potential_identifier": report.potential_identifier,
                },
                "s3_destinations": upload_summary
            })

        except ExtractionError as e:
            logger.error(f"Extraction error for '{object_key}': {str(e)}")
            results.append({
                "key": object_key,
                "status": "EXTRACTION_ERROR",
                "error": str(e)
            })
        except Exception as e:
            logger.exception(f"Unexpected error processing '{object_key}': {str(e)}")
            results.append({
                "key": object_key,
                "status": "EXECUTION_ERROR",
                "error": str(e)
            })
        finally:
            # Clean up local temporary file to avoid /tmp exhaustion
            if temp_dest and os.path.exists(temp_dest):
                try:
                    os.remove(temp_dest)
                except OSError:
                    pass

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "ETL batch processing finished",
            "processed_records_count": len(results),
            "results": results
        })
    }
