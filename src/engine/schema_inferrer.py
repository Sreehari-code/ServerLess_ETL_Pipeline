"""
Schema Inference Engine.

Automatically detects column names, infers data types, tallies null counts,
flags duplicate headers, and identifies potential unique identifier keys.
"""

import re
from typing import Any, Dict, List, Optional, Set
from src.models.schema import DatasetSchema, FieldDefinition, SchemaInferenceResult

# RFC compliant simplified email pattern
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+$")

# Common date formats (YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY)
DATE_PATTERN = re.compile(
    r"^(?:\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4})(?:[T\s]\d{2}:\d{2}(?::\d{2})?)?$"
)

# Common identifier naming suffixes/keywords
ID_KEYWORDS = ("_id", "id", "_key", "key", "_uuid", "uuid", "_code", "code", "_num", "num")


class SchemaInferrer:
    """Infers an explainable, safe schema from a collection of record dictionaries."""

    @classmethod
    def infer_schema(
        cls,
        records: List[Dict[str, Any]],
        raw_headers: Optional[List[str]] = None,
        max_sample_size: int = 500,
    ) -> SchemaInferenceResult:
        """
        Analyze records to construct an inferred DatasetSchema.

        Args:
            records: List of parsed record dictionaries.
            raw_headers: Optional list of raw column headers from CSV/JSON to detect duplicates.
            max_sample_size: Maximum records to inspect for inference performance.

        Returns:
            SchemaInferenceResult with column types, null counts, warnings, and schema.
        """
        warnings: List[str] = []

        # 1. Detect duplicate column names in header if available
        if raw_headers:
            seen_headers: Set[str] = set()
            dup_headers: List[str] = []
            for h in raw_headers:
                h_clean = h.strip()
                if h_clean in seen_headers:
                    dup_headers.append(h_clean)
                seen_headers.add(h_clean)
            if dup_headers:
                warnings.append(
                    f"Duplicate column names detected in source headers: {', '.join(set(dup_headers))}"
                )

        if not records:
            return SchemaInferenceResult(
                detected_columns=list(raw_headers) if raw_headers else [],
                inferred_types={},
                null_counts={},
                total_sampled_records=0,
                potential_identifier=None,
                warnings=["Cannot infer schema: dataset contains 0 records."],
                inferred_schema=DatasetSchema(name="empty_inferred_schema"),
            )

        sample = records[:max_sample_size]
        total_sample = len(sample)

        # 2. Collect all distinct column names across sample
        all_columns: List[str] = []
        if raw_headers:
            all_columns = [h.strip() for h in raw_headers if h and h.strip()]
        for r in sample:
            for k in r.keys():
                if k not in all_columns:
                    all_columns.append(k)

        inferred_types: Dict[str, str] = {}
        null_counts: Dict[str, int] = {}
        column_values: Dict[str, List[Any]] = {col: [] for col in all_columns}

        # 3. Analyze null counts and gather non-empty values
        for r in sample:
            for col in all_columns:
                val = r.get(col)
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    null_counts[col] = null_counts.get(col, 0) + 1
                else:
                    column_values[col].append(val)

        # 4. Infer data type for each column
        for col in all_columns:
            vals = column_values[col]
            inferred_types[col] = cls._infer_column_type(vals)

        # 5. Identify potential identifier column
        potential_id = cls._detect_identifier_column(all_columns, column_values, total_sample)
        if potential_id:
            warnings.append(
                f"Inferred primary identifier column: '{potential_id}' (unique values detected)."
            )

        # Check for columns with high null percentage
        for col, null_c in null_counts.items():
            if null_c > 0 and null_c == total_sample:
                warnings.append(f"Column '{col}' is completely empty across all sampled records.")
            elif (null_c / total_sample) > 0.5:
                warnings.append(
                    f"Column '{col}' has {null_c}/{total_sample} ({int(null_c/total_sample*100)}%) missing values."
                )

        # 6. Build the DatasetSchema
        schema = DatasetSchema(
            name="auto_inferred_schema",
            identifier_field=potential_id,
            allow_extra_fields=True,
            strict_types=False,
            description=f"Automatically inferred from {len(records)} records.",
        )

        for col in all_columns:
            is_id = (col == potential_id)
            field_def = FieldDefinition(
                name=col,
                data_type=inferred_types[col],
                required=is_id,  # In auto mode, only the detected identifier is strictly required
                unique=is_id,    # Uniqueness check applied if identifier
                description=f"Inferred {inferred_types[col]}" + (" (Primary Key)" if is_id else ""),
            )
            schema.add_field(field_def)

        return SchemaInferenceResult(
            detected_columns=all_columns,
            inferred_types=inferred_types,
            null_counts=null_counts,
            total_sampled_records=total_sample,
            potential_identifier=potential_id,
            warnings=warnings,
            inferred_schema=schema,
        )

    @classmethod
    def _infer_column_type(cls, values: List[Any]) -> str:
        """
        Infer column type using majority thresholding (>= 70%).
        Prevents a single invalid anomaly from corrupting the inferred data type.
        """
        if not values:
            return "string"

        total = len(values)
        bool_c = 0
        int_c = 0
        float_c = 0
        email_c = 0
        date_c = 0

        for v in values:
            v_str = str(v).strip()

            # Test boolean
            if v_str.lower() in {"true", "false", "yes", "no", "0", "1"}:
                bool_c += 1

            # Test integer
            try:
                f = float(v_str)
                if f.is_integer() and ("." not in v_str or v_str.endswith(".0")):
                    int_c += 1
            except (ValueError, TypeError):
                pass

            # Test float
            try:
                float(v_str)
                float_c += 1
            except (ValueError, TypeError):
                pass

            # Test email
            if EMAIL_PATTERN.match(v_str):
                email_c += 1

            # Test date
            if DATE_PATTERN.match(v_str):
                date_c += 1

        # Check in order of specificity
        if email_c / total >= 0.7:
            return "email"
        if bool_c / total >= 0.8:
            return "boolean"
        if int_c / total >= 0.7:
            return "integer"
        if float_c / total >= 0.7:
            return "float"
        if date_c / total >= 0.7:
            return "date"

        return "string"

    @classmethod
    def _detect_identifier_column(
        cls,
        columns: List[str],
        column_values: Dict[str, List[Any]],
        total_sample: int,
    ) -> Optional[str]:
        """
        Detect primary key column by combining naming conventions and value uniqueness.
        Catches identifier columns even if the batch contains duplicate anomalies.
        """
        candidate_scores: Dict[str, int] = {}

        for col in columns:
            vals = column_values.get(col, [])
            if not vals:
                continue

            unique_vals = set(str(v).strip() for v in vals)
            uniqueness_ratio = len(unique_vals) / len(vals)

            score = 0
            col_lower = col.lower()
            matches_keyword = any(col_lower.endswith(kw) or col_lower == kw for kw in ID_KEYWORDS)

            if matches_keyword and uniqueness_ratio >= 0.6:
                # Strong hint: column is named like an ID and has high uniqueness
                score += 80
            elif uniqueness_ratio >= 0.95 and total_sample > 1:
                # Pure uniqueness hint
                score += 60

            if score > 0:
                candidate_scores[col] = score

        if not candidate_scores:
            return None

        # Return column with highest candidate score
        return max(candidate_scores.items(), key=lambda x: x[1])[0]
