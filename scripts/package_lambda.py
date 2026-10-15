"""
AWS Lambda Deployment Packaging Script.

Creates a clean, production-ready serverless_etl_pipeline.zip ready for upload
to AWS Lambda. Excludes secrets, tests, caches, and local outputs.
"""

import os
import sys
import zipfile
from pathlib import Path

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def create_lambda_package(output_zip: str = "serverless_etl_pipeline.zip") -> Path:
    """
    Build the deployment zip package for AWS Lambda.

    Args:
        output_zip: Destination zip filename.

    Returns:
        Path to generated zip file.
    """
    root_dir = Path(__file__).parent.parent
    zip_path = root_dir / output_zip

    # Folders and files to include
    include_dirs = ["src", "config"]
    include_files = [
        # Create an entrypoint lambda_function.py that points to lambda_handler
    ]

    # Patterns to strictly exclude
    exclude_patterns = [
        "__pycache__",
        "*.pyc",
        "*.pyo",
        ".pytest_cache",
        ".env",
        ".git",
        "tests",
        "output",
        "sample_data",
        ".DS_Store",
    ]

    print(f"Building AWS Lambda deployment package: {zip_path.name}...")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add src and config directories
        for dir_name in include_dirs:
            target_dir = root_dir / dir_name
            if not target_dir.exists():
                continue

            for file_path in target_dir.rglob("*.py"):
                # Skip __pycache__ or excluded directories
                if any(part in file_path.parts for part in ["__pycache__", ".pytest_cache"]):
                    continue

                rel_path = file_path.relative_to(root_dir)
                zf.write(file_path, arcname=str(rel_path))
                print(f"  + Added: {rel_path}")

        # Add top-level lambda entrypoint wrapper: lambda_function.py
        wrapper_content = (
            '"""AWS Lambda standard root entrypoint."""\n'
            'from src.aws.lambda_handler import lambda_handler\n'
        )
        zf.writestr("lambda_function.py", wrapper_content)
        print("  + Added: lambda_function.py (wrapper entrypoint -> lambda_function.lambda_handler)")

    size_kb = zip_path.stat().st_size / 1024
    print(f"\n[SUCCESS] Package successfully created: {zip_path} ({size_kb:.2f} KB)")
    print("Handler configuration: lambda_function.lambda_handler")
    return zip_path


if __name__ == "__main__":
    create_lambda_package()
