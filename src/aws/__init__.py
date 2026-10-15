"""AWS integration package for S3 service and Lambda handler."""
from .s3_service import S3Service, S3ServiceError
from .lambda_handler import lambda_handler

__all__ = ["S3Service", "S3ServiceError", "lambda_handler"]
