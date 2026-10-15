"""
AWS Lambda Standard Root Entrypoint.

Exposes lambda_handler for the default AWS Lambda handler configuration:
'lambda_function.lambda_handler'.
"""

from src.aws.lambda_handler import lambda_handler

__all__ = ["lambda_handler"]
