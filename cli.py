"""
Command Line Interface (CLI) for Serverless ETL Pipeline.

Provides commands to run local processing with automatic schema inference
or configured schemas, inspect reconciliation metrics, and test AWS S3 flows.
"""

import argparse
import json
import sys
from pathlib import Path

from src.engine.pipeline import ETLPipeline
from src.engine.extractor import ExtractionError
from src.models.schema import DatasetSchema
from src.utils.logger import setup_logger
from config.settings import get_settings

logger = setup_logger("etl_cli")

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def print_banner():
    """Print hackathon CLI banner."""
    print("=" * 70)
    print(" [SERVERLESS ETL PIPELINE] - Schema-Aware Core Engine")
    print("=" * 70)


def print_report_summary(report_dict: dict):
    """Print readable reconciliation and schema audit summary."""
    print("\n" + "-" * 70)
    print(" [RECONCILIATION & SCHEMA AUDIT REPORT]")
    print("-" * 70)
    print(f" Run ID:             {report_dict.get('run_id')}")
    print(f" Source File:        {report_dict.get('source_file')}")
    print(f" Validation Mode:    {report_dict.get('validation_mode', 'auto').upper()}")
    print(f" Active Schema:      {report_dict.get('schema_name', 'inferred')}")
    if report_dict.get('potential_identifier'):
        print(f" Detected Primary Key: {report_dict.get('potential_identifier')}")
    print(f" Processing Status:  {report_dict.get('processing_status')}")
    print(f" Duration:           {report_dict.get('processing_duration_seconds')}s")
    print(f" Total Input:        {report_dict.get('total_input_records')}")
    print(f" Accepted Records:   {report_dict.get('accepted_records')}")
    print(f" Rejected Records:   {report_dict.get('rejected_records')}")
    print(f" Duplicate IDs:      {report_dict.get('duplicate_records')}")
    print(f" Data Quality Score: {report_dict.get('data_quality_score')}%")
    inv_str = "YES (Total = Accepted + Rejected)" if report_dict.get('invariant_holds') else "FAILED"
    print(f" Invariant Holds:    [OK] {inv_str}")

    warnings = report_dict.get("warnings", [])
    if warnings:
        print("\n [?] Schema Warnings:")
        for w in warnings:
            print(f"   * {w}")

    errors = report_dict.get("error_summary", {})
    if errors:
        print("\n [!] Rejection Breakdown:")
        for reason, count in errors.items():
            print(f"   * {reason}: {count}")
    print("-" * 70 + "\n")


def load_schema_from_args(args) -> tuple:
    """Resolve DatasetSchema and validation mode from arguments."""
    mode = getattr(args, "mode", "auto") or "auto"
    preset = getattr(args, "preset", "auto")
    schema_file = getattr(args, "schema_file", None)

    schema = None
    if schema_file and Path(schema_file).exists():
        raw_schema = json.loads(Path(schema_file).read_text(encoding="utf-8"))
        schema = DatasetSchema.from_dict(raw_schema)
        mode = "configured"
    elif preset == "customer":
        schema = DatasetSchema.preset_customer()
        mode = "configured"
    elif preset == "employee":
        schema = DatasetSchema.preset_employee()
        mode = "configured"
    elif preset == "sales":
        schema = DatasetSchema.preset_sales()
        mode = "configured"

    return schema, mode


def run_local(args) -> int:
    """Execute ETL pipeline on a local file."""
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    settings = get_settings()
    out_dir = args.output_dir or settings.local_output_dir
    fmt = args.format or settings.default_output_format

    schema, mode = load_schema_from_args(args)
    pipeline = ETLPipeline(
        output_dir=out_dir,
        default_format=fmt,
        schema=schema,
        validation_mode=mode,
    )

    try:
        result = pipeline.process_file(
            file_path=input_path,
            write_output=not args.dry_run,
            output_format=fmt,
            schema=schema,
            validation_mode=mode,
        )
        print_report_summary(result.report.to_dict())

        if result.output_files:
            print(" [OUTPUT FILES GENERATED]")
            for out_type, out_file in result.output_files.items():
                print(f"   * {out_type.capitalize()}: {out_file}")
            print()

        return 0
    except ExtractionError as e:
        logger.error(f"Extraction failed: {str(e)}")
        return 1
    except Exception as e:
        logger.exception(f"Pipeline execution encountered an unexpected error: {str(e)}")
        return 1


def run_s3(args) -> int:
    """Execute ETL pipeline by reading from S3 and writing back to S3."""
    from src.aws.s3_service import S3Service

    settings = get_settings()
    bucket = args.bucket or settings.s3_bucket_name
    key = args.key

    if not bucket:
        logger.error("S3 bucket name not specified. Provide --bucket or set S3_BUCKET_NAME in .env")
        return 1
    if not key:
        logger.error("S3 object key required. Provide --key (e.g. input/customers.csv)")
        return 1

    s3_service = S3Service(bucket_name=bucket)
    if not s3_service.is_configured():
        logger.error("AWS S3 is not accessible with current credentials/configuration.")
        return 1

    logger.info(f"Downloading s3://{bucket}/{key} for processing...")
    try:
        local_temp = s3_service.download_file(key)
        schema, mode = load_schema_from_args(args)
        pipeline = ETLPipeline(
            output_dir=settings.local_output_dir,
            schema=schema,
            validation_mode=mode,
        )
        result = pipeline.process_file(local_temp, write_output=True, schema=schema, validation_mode=mode)

        print_report_summary(result.report.to_dict())

        # Upload results to S3
        logger.info(f"Uploading run outputs to S3 bucket '{bucket}'...")
        uploaded = s3_service.upload_run_outputs(
            run_id=result.run_id,
            accepted=result.accepted_records,
            rejected=result.rejected_records,
            report=result.report
        )
        print(" [S3 DESTINATION OBJECTS]")
        for k, v in uploaded.items():
            print(f"   * {k}: s3://{bucket}/{v}")
        return 0
    except Exception as e:
        logger.error(f"S3 Pipeline execution failed: {str(e)}")
        return 1


def main():
    print_banner()
    parser = argparse.ArgumentParser(
        description="Serverless ETL Pipeline - Schema-Aware CLI"
    )
    subparsers = parser.add_subparsers(dest="command", help="Execution mode")

    # Local file execution
    local_parser = subparsers.add_parser("local", help="Process a local CSV or JSON file")
    local_parser.add_argument(
        "-i", "--input", required=True, help="Path to input CSV or JSON file"
    )
    local_parser.add_argument(
        "-o", "--output-dir", default=None, help="Directory to save accepted/rejected/reports"
    )
    local_parser.add_argument(
        "-f", "--format", choices=["json", "csv"], default="json", help="Output serialization format"
    )
    local_parser.add_argument(
        "-m", "--mode", choices=["auto", "configured"], default="auto",
        help="Validation mode: 'auto' infers schema; 'configured' enforces specified schema rules."
    )
    local_parser.add_argument(
        "-p", "--preset", choices=["auto", "customer", "employee", "sales"], default="auto",
        help="Schema preset to enforce when in configured mode."
    )
    local_parser.add_argument(
        "-s", "--schema-file", help="Path to custom JSON schema definition file"
    )
    local_parser.add_argument(
        "--dry-run", action="store_true", help="Process without writing output files to disk"
    )

    # S3 execution
    s3_parser = subparsers.add_parser("s3", help="Process an object directly from Amazon S3")
    s3_parser.add_argument("-b", "--bucket", help="Target S3 bucket name")
    s3_parser.add_argument("-k", "--key", required=True, help="S3 object key (e.g. input/data.csv)")
    s3_parser.add_argument("-m", "--mode", choices=["auto", "configured"], default="auto")
    s3_parser.add_argument("-p", "--preset", choices=["auto", "customer", "employee", "sales"], default="auto")
    s3_parser.add_argument("-s", "--schema-file", help="Path to custom JSON schema definition file")

    args = parser.parse_args()

    if args.command == "local":
        sys.exit(run_local(args))
    elif args.command == "s3":
        sys.exit(run_s3(args))
    else:
        parser.print_help()
        print("\nQuick Test (Auto Mode):       python cli.py local --input sample_data/employees_mixed.csv")
        print("Quick Test (Employee Preset): python cli.py local --input sample_data/employees_mixed.csv --preset employee")
        sys.exit(0)


if __name__ == "__main__":
    main()
