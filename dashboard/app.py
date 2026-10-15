"""
Serverless ETL Pipeline - Monitoring & Operations Dashboard.

Built with Streamlit for live hackathon demonstration.
Schema-aware: supports both Automatic Inference and Configured Schema Modes,
multi-domain datasets (Customer, Employee, Sales, IoT, etc.), and AWS S3 Cloud integration.
"""

import io
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to sys.path
root_dir = Path(__file__).parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
import pandas as pd

from config.settings import get_settings
from src.engine.pipeline import ETLPipeline, PipelineResult
from src.engine.extractor import Extractor, ExtractionError
from src.engine.schema_inferrer import SchemaInferrer
from src.models.schema import DatasetSchema, FieldDefinition
from src.aws.s3_service import S3Service


# Page Configuration
st.set_page_config(
    page_title="Schema-Aware Serverless ETL Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 18px;
        color: #f8fafc;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #38bdf8;
    }
    .metric-label {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94a3b8;
    }
    .schema-badge {
        background-color: #1e3a8a;
        color: #bfdbfe;
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-block;
        margin-right: 6px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        padding-top: 10px;
        padding-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)


# Initialize Session State
if "history" not in st.session_state:
    st.session_state.history = []
if "current_result" not in st.session_state:
    st.session_state.current_result = None


def render_sidebar():
    """Render configuration and environment sidebar."""
    settings = get_settings()

    with st.sidebar:
        st.title("⚡ ETL Control Center")
        st.caption("Schema-Aware Serverless Architecture")
        st.divider()

        # Operational Mode Selector
        st.subheader("⚙️ Processing Mode")
        mode = st.radio(
            "Select Execution Environment",
            ["Local Standalone (Offline)", "Amazon S3 Cloud Mode"],
            help="Local mode runs entirely offline on this machine. S3 mode connects to real AWS infrastructure."
        )

        st.divider()

        # Validation Strategy Selector
        st.subheader("🛡️ Validation Strategy")
        val_strategy = st.radio(
            "Validation Mode",
            ["Automatic Inference (Any Dataset)", "Configured Schema (Strict Rules)"],
            help="Auto mode infers schema, types, and primary keys on the fly. Configured mode enforces explicit business constraints."
        )

        preset_choice = "auto"
        custom_schema_obj = None

        if "Configured Schema" in val_strategy:
            st.markdown("**Select Business Preset:**")
            preset_choice = st.selectbox(
                "Preset Definition",
                ["Customer Profile", "Employee Directory", "Sales Orders", "Custom Configuration"]
            )
            if preset_choice == "Customer Profile":
                custom_schema_obj = DatasetSchema.preset_customer()
            elif preset_choice == "Employee Directory":
                custom_schema_obj = DatasetSchema.preset_employee()
            elif preset_choice == "Sales Orders":
                custom_schema_obj = DatasetSchema.preset_sales()

        st.divider()

        # AWS Environment Inspection
        st.subheader("☁️ AWS Configuration")
        s3_service = S3Service()
        is_s3_ready = s3_service.is_configured()

        st.write(f"**Region:** `{settings.aws_region}`")
        if settings.s3_bucket_name:
            st.write(f"**Target Bucket:** `{settings.s3_bucket_name}`")
        else:
            st.info("ℹ️ S3_BUCKET_NAME not defined.")

        if is_s3_ready:
            st.success("🟢 AWS S3 Connected & Authenticated")
        else:
            st.warning("🟡 AWS S3 Not Connected (Local Mode Active)")

        st.divider()

        # Pipeline Stage Legend
        st.subheader("🔄 Architecture Stages")
        st.markdown("""
        1. **Ingest**: CSV/JSON parsing
        2. **Detect**: Schema & type inference
        3. **Validate**: Bounds, types, uniqueness
        4. **Transform**: Whitespace, formats, ISO
        5. **Reconcile**: Total = Accepted + Rejected
        6. **Persist**: Partitions (Local / S3)
        """)

    return mode, is_s3_ready, s3_service, val_strategy, custom_schema_obj


def render_pipeline_stages(status: Optional[str] = None):
    """Render visual ETL pipeline stage indicator."""
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown("**1. Ingestion**")
        st.caption("📥 CSV / JSON Parsing")
    with col2:
        st.markdown("**2. Schema Detection**")
        st.caption("🔍 Types & Primary Keys")
    with col3:
        st.markdown("**3. Validation**")
        st.caption("🛡️ Types, Nulls, Bounds")
    with col4:
        st.markdown("**4. Reconciliation**")
        st.caption("⚖️ Total = Acc + Rej")
    with col5:
        st.markdown("**5. Cloud Persistence**")
        st.caption("💾 Partitions to S3")

    st.progress(1.0 if status else 0.0)


def run_pipeline_on_content(
    content: str,
    filename: str,
    ext: str,
    mode: str,
    s3_service: S3Service,
    schema: Optional[DatasetSchema] = None,
    val_mode: str = "auto"
):
    """Execute pipeline and update session state."""
    pipeline = ETLPipeline(
        output_dir="output",
        default_format="json",
        schema=schema,
        validation_mode=val_mode
    )

    with st.spinner(f"Executing ETL pipeline on '{filename}' [{val_mode.upper()} mode]..."):
        time.sleep(0.15)
        result = pipeline.process_text(
            content=content,
            source_name=filename,
            file_extension=ext,
            write_output=True,
            output_format="json",
            schema=schema,
            validation_mode=val_mode,
        )

        # S3 upload if cloud mode active
        s3_uploads = None
        if "Amazon S3" in mode and s3_service.is_configured():
            try:
                s3_uploads = s3_service.upload_run_outputs(
                    run_id=result.run_id,
                    accepted=result.accepted_records,
                    rejected=result.rejected_records,
                    report=result.report
                )
            except Exception as e:
                st.error(f"S3 Cloud Upload failed: {str(e)}")

        st.session_state.current_result = result
        st.session_state.history.insert(0, {
            "run_id": result.run_id,
            "filename": filename,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "S3 Cloud" if (s3_uploads and "Amazon S3" in mode) else "Local",
            "val_mode": result.report.validation_mode.upper(),
            "schema": result.report.schema_name or "Inferred",
            "total": result.report.total_input_records,
            "accepted": result.report.accepted_records,
            "rejected": result.report.rejected_records,
            "duplicates": result.report.duplicate_records,
            "quality_score": f"{result.report.data_quality_score}%",
            "status": result.report.processing_status,
            "duration": f"{result.report.processing_duration_seconds}s",
            "s3_uploads": s3_uploads
        })

    st.success(f"Processing complete for '{filename}'! Run ID: `{result.run_id}`")


def main():
    settings = get_settings()
    mode, is_s3_ready, s3_service, val_strategy, custom_schema_obj = render_sidebar()

    # Title & Banner
    st.title("⚡ Schema-Aware Serverless ETL Pipeline")
    st.markdown("Dynamic serverless data pipeline with automated schema inference, multi-domain validation, deduplication, and audited reconciliation.")

    # Ingestion Controls
    st.subheader("🚀 Ingest Dataset")

    col_upload, col_demo = st.columns([2, 1])

    with col_upload:
        uploaded_file = st.file_uploader(
            "Upload Any Tabular Dataset (CSV or JSON)",
            type=["csv", "json"],
            help="Upload datasets of any structure: customers, employees, orders, logs, or sensor data."
        )

    with col_demo:
        st.write("**One-Click Diverse Hackathon Datasets:**")
        demo_choice = st.selectbox(
            "Choose a pre-bundled test dataset:",
            [
                "1. Employee Dataset (Salaries, Depts, Duplicates)",
                "2. Sales Orders Dataset (Quantities, Prices, Duplicates)",
                "3. IoT Sensor Readings (Arbitrary Columns)",
                "4. Customer Dataset (Emails, Ages, Duplicates)",
                "5. Clean Customer Dataset (100% Quality)",
                "6. Empty File (0 Records Edge Case)",
            ]
        )

        if st.button("▶️ Load & Process Sample", type="primary", width="stretch"):
            sample_dir = root_dir / "sample_data"
            if "Employee" in demo_choice:
                sample_path = sample_dir / "employees_mixed.csv"
            elif "Sales" in demo_choice:
                sample_path = sample_dir / "sales_mixed.csv"
            elif "IoT Sensor" in demo_choice:
                sample_path = sample_dir / "sensor_readings.csv"
            elif "Clean" in demo_choice:
                sample_path = sample_dir / "customers_valid.csv"
            elif "Empty" in demo_choice:
                sample_path = sample_dir / "customers_empty.csv"
            else:
                sample_path = sample_dir / "customers_mixed.csv"

            if sample_path.exists():
                content = sample_path.read_text(encoding="utf-8")
                target_mode = "configured" if "Configured" in val_strategy else "auto"
                run_pipeline_on_content(
                    content=content,
                    filename=sample_path.name,
                    ext=sample_path.suffix,
                    mode=mode,
                    s3_service=s3_service,
                    schema=custom_schema_obj,
                    val_mode=target_mode,
                )

    if uploaded_file is not None:
        content_str = uploaded_file.getvalue().decode("utf-8", errors="replace")
        ext = Path(uploaded_file.name).suffix

        # Interactive Schema Discovery for uploaded file
        with st.expander("🔍 Inspect & Customize Detected Schema Before Processing", expanded=True):
            try:
                raw_recs = Extractor.extract_from_text(content_str, ext)
                headers = Extractor.extract_headers(content_str, ext)
                inference = SchemaInferrer.infer_schema(raw_recs, raw_headers=headers)

                col_inf1, col_inf2 = st.columns([1, 2])
                with col_inf1:
                    st.markdown(f"**Detected Columns:** `{len(inference.detected_columns)}`")
                    st.markdown(f"**Primary Key Identifier:** `{inference.potential_identifier or 'None Detected'}`")
                    st.markdown(f"**Sampled Records:** `{inference.total_sampled_records}` rows")
                    if inference.warnings:
                        for w in inference.warnings:
                            st.info(f"ℹ️ {w}")
                with col_inf2:
                    st.markdown("**Inferred Column Schema & Data Profile:**")
                    profile_rows = []
                    for col in inference.detected_columns:
                        typ = inference.inferred_types.get(col, "string")
                        null_cnt = inference.null_counts.get(col, 0)
                        null_pct = (null_cnt / inference.total_sampled_records * 100) if inference.total_sampled_records else 0.0
                        profile_rows.append({
                            "Column Name": col,
                            "Inferred Type": typ,
                            "Missing / Nulls": f"{null_cnt} ({null_pct:.1f}%)",
                            "Role": "🔑 Primary Key" if col == inference.potential_identifier else "Data Column"
                        })
                    st.dataframe(pd.DataFrame(profile_rows), width="stretch", hide_index=True)

            except Exception as e:
                st.warning(f"Could not preview schema: {e}")

        if st.button(f"⚡ Execute Pipeline on {uploaded_file.name}", type="primary"):
            target_mode = "configured" if "Configured" in val_strategy else "auto"
            run_pipeline_on_content(
                content=content_str,
                filename=uploaded_file.name,
                ext=ext,
                mode=mode,
                s3_service=s3_service,
                schema=custom_schema_obj,
                val_mode=target_mode,
            )

    st.divider()

    # Visual Stage Progress
    render_pipeline_stages(status="ACTIVE" if st.session_state.current_result else None)

    if not st.session_state.current_result:
        st.info("👆 Click **'Load & Process Sample'** above or upload a dataset to run the pipeline.")
        return

    result: PipelineResult = st.session_state.current_result
    rep = result.report

    # KPI Summary Cards
    st.subheader("📊 Executive Overview & Reconciliation Metrics")
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    with kpi1:
        st.metric("Total Records Ingested", rep.total_input_records)
    with kpi2:
        st.metric("Accepted Records", rep.accepted_records, delta=f"{rep.accepted_records} valid")
    with kpi3:
        st.metric("Rejected Records", rep.rejected_records, delta=f"-{rep.rejected_records} invalid" if rep.rejected_records else "0", delta_color="inverse")
    with kpi4:
        st.metric("Duplicate IDs Blocked", rep.duplicate_records)
    with kpi5:
        st.metric("Data Quality Score", f"{rep.data_quality_score}%")

    # Schema Context Banner
    col_sb1, col_sb2 = st.columns([3, 1])
    with col_sb1:
        st.markdown(
            f"<span class='schema-badge'>Mode: {rep.validation_mode.upper()}</span>"
            f"<span class='schema-badge'>Schema: {rep.schema_name or 'Auto-Inferred'}</span>"
            + (f"<span class='schema-badge'>Primary Key: {rep.potential_identifier}</span>" if rep.potential_identifier else ""),
            unsafe_allow_html=True
        )
    with col_sb2:
        if rep.invariant_holds:
            st.success("✅ Invariant: Total = Acc + Rej")

    if rep.warnings:
        for w in rep.warnings:
            st.caption(f"⚠️ {w}")

    # Tabs
    tab_acc, tab_rej, tab_schema, tab_rep, tab_hist, tab_cloud = st.tabs([
        f"✅ Accepted Records ({len(result.accepted_records)})",
        f"❌ Rejected Records ({len(result.rejected_records)})",
        "📐 Applied Schema & Rules",
        "📋 Reconciliation Report",
        f"📜 Run History ({len(st.session_state.history)})",
        "☁️ AWS Cloud Persistence"
    ])

    # Tab 1: Accepted Records
    with tab_acc:
        st.markdown("#### Cleaned, Validated, and Standardized Records")
        if result.accepted_records:
            accepted_dicts = [r.to_dict() for r in result.accepted_records]
            df_acc = pd.DataFrame(accepted_dicts)
            st.dataframe(df_acc, width="stretch")

            col_d1, col_d2 = st.columns(2)
            with col_d1:
                csv_bytes = df_acc.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download Accepted CSV",
                    data=csv_bytes,
                    file_name=f"accepted_{result.run_id}.csv",
                    mime="text/csv"
                )
            with col_d2:
                json_bytes = json.dumps(accepted_dicts, indent=2).encode("utf-8")
                st.download_button(
                    "⬇️ Download Accepted JSON",
                    data=json_bytes,
                    file_name=f"accepted_{result.run_id}.json",
                    mime="application/json"
                )
        else:
            st.warning("No records met validation criteria for this run.")

    # Tab 2: Rejected Records
    with tab_rej:
        st.markdown("#### Rejected Anomalies with Audit Reasons")
        if result.rejected_records:
            rej_rows = []
            for r in result.rejected_records:
                flat = {**r.original_data}
                flat["Rejection Reason(s)"] = " • ".join(r.rejection_reasons)
                flat["Rejected At (UTC)"] = r.rejected_at
                rej_rows.append(flat)

            df_rej = pd.DataFrame(rej_rows)
            st.dataframe(df_rej, width="stretch")

            if rep.error_summary:
                st.markdown("**Rejection Breakdown by Category:**")
                error_df = pd.DataFrame(list(rep.error_summary.items()), columns=["Reason", "Count"])
                st.bar_chart(error_df.set_index("Reason"))

            col_dr1, col_dr2 = st.columns(2)
            with col_dr1:
                csv_rej = df_rej.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download Rejected CSV",
                    data=csv_rej,
                    file_name=f"rejected_{result.run_id}.csv",
                    mime="text/csv"
                )
            with col_dr2:
                raw_rej = [r.to_dict() for r in result.rejected_records]
                st.download_button(
                    "⬇️ Download Rejected JSON",
                    data=json.dumps(raw_rej, indent=2).encode("utf-8"),
                    file_name=f"rejected_{result.run_id}.json",
                    mime="application/json"
                )
        else:
            st.success("🎉 Zero rejected records! High quality clean batch.")

    # Tab 3: Applied Schema
    with tab_schema:
        st.markdown("### 📐 Applied Dataset Schema & Constraints")
        if rep.detected_schema:
            schema_meta = rep.detected_schema
            s_name = schema_meta.get("name", "Unnamed Schema")
            s_id = schema_meta.get("identifier_column") or "None"
            s_desc = schema_meta.get("description", "No description provided.")
            fields = schema_meta.get("fields", {})

            col_s1, col_s2, col_s3 = st.columns(3)
            with col_s1:
                st.markdown(f"**Schema Name:** `{s_name}`")
            with col_s2:
                st.markdown(f"**Primary Key Column:** `{s_id}`")
            with col_s3:
                st.markdown(f"**Total Defined Fields:** `{len(fields)}`")
            st.caption(f"_{s_desc}_")

            if fields:
                st.markdown("#### Defined Column Rules:")
                field_rows = []
                for fname, fdef in fields.items():
                    if isinstance(fdef, dict):
                        field_rows.append({
                            "Field Name": fname,
                            "Data Type": fdef.get("data_type", "string"),
                            "Required": "✅ Yes" if fdef.get("required") else "⚪ Optional",
                            "Nullable": "Yes" if fdef.get("nullable", True) else "No",
                            "Constraints": ", ".join(filter(None, [
                                f"Min: {fdef.get('min_value')}" if fdef.get("min_value") is not None else None,
                                f"Max: {fdef.get('max_value')}" if fdef.get("max_value") is not None else None,
                                f"Format: {fdef.get('regex_pattern')}" if fdef.get("regex_pattern") else None
                            ])) or "None"
                        })
                if field_rows:
                    st.dataframe(pd.DataFrame(field_rows), width="stretch", hide_index=True)

            with st.expander("🔍 View Raw Schema Definition (JSON)"):
                st.json(schema_meta)
        else:
            st.info("No explicit schema metadata recorded.")

    # Tab 4: Reconciliation Report
    with tab_rep:
        st.markdown("### 📋 Executive Reconciliation & Audit Report")
        st.caption("Immutable processing ledger, data quality assessment, and validation accounting verification.")

        # Section 1: Execution & Run Metadata
        meta_col1, meta_col2, meta_col3, meta_col4 = st.columns(4)
        with meta_col1:
            st.markdown(f"**Run Identifier:**\n`{rep.run_id}`")
            st.markdown(f"**Source File:**\n`{rep.source_file}`")
        with meta_col2:
            st.markdown(f"**Execution Timestamp:**\n`{rep.completed_at}`")
            st.markdown(f"**Processing Time:**\n`{rep.processing_duration_seconds:.4f}s`")
        with meta_col3:
            status_color = "green" if rep.processing_status == "SUCCESS" else ("orange" if rep.processing_status == "PARTIAL_SUCCESS" else "red")
            st.markdown(f"**Execution Status:**\n:{status_color}[**{rep.processing_status}**]")
            st.markdown(f"**Validation Mode:**\n`{rep.validation_mode.upper()}`")
        with meta_col4:
            st.markdown(f"**Applied Schema:**\n`{rep.schema_name or 'Auto-Inferred'}`")
            st.markdown(f"**Primary Key:**\n`{rep.potential_identifier or 'None'}`")

        st.markdown("---")

        # Section 2: Invariant Check Callout
        if rep.invariant_holds:
            st.success(
                f"**Reconciliation Invariant Verified (100% Zero Record Loss)**\n\n"
                f"$$\\text{{Total Input Records ({rep.total_input_records})}} = "
                f"\\text{{Accepted Records ({rep.accepted_records})}} + "
                f"\\text{{Rejected Records ({rep.rejected_records})}}$$\n\n"
                f"**Discrepancy:** `0 records` • Zero silent record loss or data leakage detected.",
                icon="⚖️"
            )
        else:
            st.error(
                f"**Accounting Invariant Violation Detected!**\n\n"
                f"Total Input: {rep.total_input_records} ≠ Accepted: {rep.accepted_records} + Rejected: {rep.rejected_records}",
                icon="⚠️"
            )

        # Section 3: Ingestion & Quality Ledger Table
        st.markdown("#### 📊 Record Processing Ledger")
        ledger_data = [
            {
                "Classification": "Total Records Ingested",
                "Count": str(rep.total_input_records),
                "Share of Batch": "100.0%",
                "Description": "Raw extracted rows from input source stream / file"
            },
            {
                "Classification": "Accepted (Clean & Transformed)",
                "Count": str(rep.accepted_records),
                "Share of Batch": f"{(rep.accepted_records / rep.total_input_records * 100):.1f}%" if rep.total_input_records else "0.0%",
                "Description": "Conformed to schema rules, validated, cleansed, and exported"
            },
            {
                "Classification": "Rejected (Anomalies & Corrupt)",
                "Count": str(rep.rejected_records),
                "Share of Batch": f"{(rep.rejected_records / rep.total_input_records * 100):.1f}%" if rep.total_input_records else "0.0%",
                "Description": "Failed one or more validation constraints; isolated with root cause"
            },
            {
                "Classification": "Duplicate Identifiers Blocked",
                "Count": str(rep.duplicate_records),
                "Share of Batch": "-",
                "Description": f"Records rejected due to repeated primary key '{rep.potential_identifier or 'ID'}'"
            },
            {
                "Classification": "Overall Data Quality Score",
                "Count": f"{rep.data_quality_score}%",
                "Share of Batch": "-",
                "Description": "Proportion of valid records successfully admitted"
            }
        ]
        st.dataframe(pd.DataFrame(ledger_data), width="stretch", hide_index=True)

        # Section 4: Anomaly Breakdown
        st.markdown("#### 🔍 Anomaly Cause Analysis")
        if rep.error_summary:
            total_errors = sum(rep.error_summary.values())
            breakdown_data = []
            for reason, count in sorted(rep.error_summary.items(), key=lambda x: x[1], reverse=True):
                pct = (count / total_errors * 100) if total_errors > 0 else 0
                breakdown_data.append({
                    "Validation Constraint / Failure Reason": reason,
                    "Occurrences": count,
                    "Share of Violations": f"{pct:.1f}%"
                })
            st.dataframe(pd.DataFrame(breakdown_data), width="stretch", hide_index=True)
        else:
            st.info("🎉 Zero validation failures recorded for this dataset run.")

        # Section 5: Warnings & Operational Notes
        if rep.warnings:
            st.markdown("#### ⚠️ Operational & Schema Warnings")
            for w in rep.warnings:
                st.warning(f"• {w}")

        st.markdown("---")

        # Section 6: Action & JSON Inspection
        col_down, col_blank = st.columns([1, 1])
        rep_dict = rep.to_dict()
        with col_down:
            st.download_button(
                "⬇️ Download Audit Report (JSON)",
                data=json.dumps(rep_dict, indent=2).encode("utf-8"),
                file_name=f"reconciliation_report_{result.run_id}.json",
                mime="application/json",
                width="stretch"
            )

        with st.expander("🔍 View Raw JSON Audit Log (for technical inspection)"):
            st.json(rep_dict)

    # Tab 5: Processing History
    with tab_hist:
        st.markdown("#### Processing Run Audit Trail")
        if st.session_state.history:
            df_hist = pd.DataFrame(st.session_state.history)
            st.dataframe(df_hist.drop(columns=["s3_uploads"], errors="ignore"), width="stretch")
        else:
            st.info("No runs recorded yet in this session.")

    # Tab 6: AWS Cloud Persistence
    with tab_cloud:
        st.markdown("#### AWS S3 Architecture & Storage Verification")
        if "Amazon S3" in mode and is_s3_ready:
            st.success(f"Connected to Amazon S3 Bucket: `{settings.s3_bucket_name}`")
            st.markdown(f"""
            **Configured Storage Prefixes:**
            - Ingestion Prefix: `{settings.s3_input_prefix}`
            - Clean Accepted Prefix: `{settings.s3_output_accepted_prefix}`
            - Rejected Anomalies Prefix: `{settings.s3_output_rejected_prefix}`
            - Reconciliation Reports Prefix: `{settings.s3_output_reports_prefix}`
            """)

            try:
                available_objects = s3_service.list_input_objects()
                st.write(f"**Discovered Input Files in S3 (`{settings.s3_input_prefix}`):**")
                if available_objects:
                    for obj in available_objects:
                        st.markdown(f"- `s3://{settings.s3_bucket_name}/{obj}`")
                else:
                    st.info("No input files currently waiting in S3 input prefix.")
            except Exception as e:
                st.warning(f"Could not list S3 objects: {e}")
        else:
            st.info("Currently running in **Local Standalone Mode**.")


if __name__ == "__main__":
    main()
