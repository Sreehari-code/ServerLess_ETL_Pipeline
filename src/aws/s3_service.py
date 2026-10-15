"""
Amazon S3 Service Integration Module.

Provides secure, audited interactions with Amazon S3 using the AWS SDK (boto3).
Respects the standard AWS credential provider chain and environment variables.
Does not hardcode or log credentials.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import get_settings
from src.models.record import CustomerRecord, RejectedRecord, ReconciliationReport
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class S3ServiceError(Exception):
    """Base exception for S3 service errors."""
    pass


class S3Service:
    """
    Encapsulates Amazon S3 operations including object retrieval, listing,
    and structured output publishing.
    """

    def __init__(self, bucket_name: Optional[str] = None, region_name: Optional[str] = None):
        self.settings = get_settings()
        self.bucket_name = self.settings.s3_bucket_name if bucket_name is None else bucket_name
        self.region_name = self.settings.aws_region if region_name is None else region_name
        self._s3_client = None

    def _get_client(self):
        """Lazy-load boto3 client using default credential provider chain."""
        if self._s3_client is None:
            try:
                import boto3
                from botocore.config import Config
            except ImportError as e:
                raise S3ServiceError(
                    "boto3 package is not installed. Run 'pip install boto3' to enable AWS S3."
                ) from e

            session_kwargs = {}
            if self.settings.aws_profile:
                session_kwargs["profile_name"] = self.settings.aws_profile

            session = boto3.Session(**session_kwargs)
            client_config = Config(
                region_name=self.region_name,
                retries={"max_attempts": 3, "mode": "standard"}
            )
            self._s3_client = session.client("s3", config=client_config)

        return self._s3_client

    def is_configured(self) -> bool:
        """
        Check if S3 bucket and credentials are valid and accessible.
        Does NOT throw uncaught exceptions; returns bool.
        """
        if not self.bucket_name:
            return False

        try:
            client = self._get_client()
            # head_bucket verifies bucket existence and user permissions
            client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            logger.debug(f"S3 accessibility check failed for bucket '{self.bucket_name}': {e}")
            return False

    def list_input_objects(self, prefix: Optional[str] = None) -> List[str]:
        """
        List candidate input objects within the configured bucket and prefix.

        Args:
            prefix: Optional prefix filter (defaults to s3_input_prefix, e.g. 'input/')

        Returns:
            List of object keys matching .csv or .json extensions.
        """
        if not self.bucket_name:
            raise S3ServiceError("S3 bucket name is not configured.")

        client = self._get_client()
        target_prefix = prefix if prefix is not None else self.settings.s3_input_prefix

        try:
            response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=target_prefix)
            contents = response.get("Contents", [])
            valid_keys = []
            for item in contents:
                key = item["Key"]
                # Skip directory placeholder markers and non-data files
                if key.endswith("/") or item.get("Size", 0) == 0:
                    continue
                if key.lower().endswith((".csv", ".json")):
                    valid_keys.append(key)
            return valid_keys
        except Exception as e:
            logger.error(f"Failed to list objects in s3://{self.bucket_name}/{target_prefix}: {str(e)}")
            raise S3ServiceError(f"Could not list S3 input objects: {str(e)}") from e

    def download_file(self, s3_key: str, destination_path: Optional[str] = None) -> str:
        """
        Download an S3 object to a local destination or a secure temporary file.

        Args:
            s3_key: Key of the object in S3.
            destination_path: Optional specific path to download to.

        Returns:
            Absolute path to downloaded file.
        """
        if not self.bucket_name:
            raise S3ServiceError("S3 bucket name is not configured.")

        client = self._get_client()

        if destination_path is None:
            # Create a safe temporary file with the same extension
            ext = Path(s3_key).suffix
            fd, destination_path = tempfile.mkstemp(prefix="etl_download_", suffix=ext)
            os.close(fd)

        try:
            logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {destination_path}")
            client.download_file(self.bucket_name, s3_key, destination_path)
            return destination_path
        except Exception as e:
            logger.error(f"Failed downloading s3://{self.bucket_name}/{s3_key}: {str(e)}")
            if os.path.exists(destination_path):
                os.remove(destination_path)
            raise S3ServiceError(f"S3 download failed for '{s3_key}': {str(e)}") from e

    def upload_file(self, local_path: str, s3_key: str, content_type: Optional[str] = None) -> str:
        """
        Upload a local file to S3. Never sets public ACLs.

        Args:
            local_path: Path to local file.
            s3_key: Target key in S3.
            content_type: MIME type header.

        Returns:
            Destination S3 key.
        """
        if not self.bucket_name:
            raise S3ServiceError("S3 bucket name is not configured.")

        client = self._get_client()
        extra_args: Dict[str, Any] = {
            "ServerSideEncryption": "AES256"  # Enforce at-rest encryption
        }
        if content_type:
            extra_args["ContentType"] = content_type

        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{s3_key}")
            client.upload_file(local_path, self.bucket_name, s3_key, ExtraArgs=extra_args)
            return s3_key
        except Exception as e:
            logger.error(f"Failed uploading to s3://{self.bucket_name}/{s3_key}: {str(e)}")
            raise S3ServiceError(f"S3 upload failed for '{s3_key}': {str(e)}") from e

    def upload_json_data(self, data: Any, s3_key: str) -> str:
        """
        Upload in-memory Python object as JSON directly to S3.
        """
        if not self.bucket_name:
            raise S3ServiceError("S3 bucket name is not configured.")

        client = self._get_client()
        body_bytes = json.dumps(data, indent=2).encode("utf-8")

        try:
            client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=body_bytes,
                ContentType="application/json",
                ServerSideEncryption="AES256"
            )
            return s3_key
        except Exception as e:
            logger.error(f"Failed putting JSON object to s3://{self.bucket_name}/{s3_key}: {str(e)}")
            raise S3ServiceError(f"Failed writing JSON to S3 '{s3_key}': {str(e)}") from e

    def upload_run_outputs(
        self,
        run_id: str,
        accepted: List[Any],
        rejected: List[RejectedRecord],
        report: ReconciliationReport,
    ) -> Dict[str, str]:
        """
        Upload accepted records, rejected records, and reconciliation report to S3.
        Organized by run ID in distinct prefixes to avoid unintentional overwrites.
        """
        accepted_key = f"{self.settings.s3_output_accepted_prefix}accepted_{run_id}.json"
        rejected_key = f"{self.settings.s3_output_rejected_prefix}rejected_{run_id}.json"
        report_key = f"{self.settings.s3_output_reports_prefix}report_{run_id}.json"

        # 1. Upload Accepted Records
        accepted_payload = [rec.to_dict() for rec in accepted]
        self.upload_json_data(accepted_payload, accepted_key)

        # 2. Upload Rejected Records
        rejected_payload = [rec.to_dict() for rec in rejected]
        self.upload_json_data(rejected_payload, rejected_key)

        # 3. Upload Reconciliation Report
        self.upload_json_data(report.to_dict(), report_key)

        logger.info(f"Successfully uploaded all run artifacts for {run_id} to S3.")
        return {
            "accepted_s3_key": accepted_key,
            "rejected_s3_key": rejected_key,
            "report_s3_key": report_key,
        }
