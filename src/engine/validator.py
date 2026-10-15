"""
Data Validator Module.

Provides schema-driven validation supporting both Automatic Mode (inferred schema)
and Configured-Schema Mode (explicit business constraints).
"""

import re
from typing import Any, Dict, List, Optional, Set
from src.models.record import ValidationResult
from src.models.schema import DatasetSchema, FieldDefinition

# RFC 5322 compliant simplified email regex
EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+$"
)


class SchemaValidator:
    """
    Schema-driven validator.
    Enforces required fields, type constraints, bounds, and identifier uniqueness
    based on a dynamic DatasetSchema.
    """

    def __init__(self, schema: Optional[DatasetSchema] = None, mode: str = "auto"):
        self.schema = schema or DatasetSchema(name="default_open_schema")
        self.mode = mode.lower()  # "auto" or "configured"
        self.seen_unique_values: Dict[str, Set[str]] = {}
        self._init_unique_trackers()

    def _init_unique_trackers(self) -> None:
        """Initialize set trackers for unique/identifier columns."""
        self.seen_unique_values = {}
        for fname, fdef in self.schema.fields.items():
            if fdef.unique:
                self.seen_unique_values[fname] = set()
        if self.schema.identifier_field and self.schema.identifier_field not in self.seen_unique_values:
            self.seen_unique_values[self.schema.identifier_field] = set()

    def reset(self) -> None:
        """Reset internal deduplication state between batch runs."""
        for tracker in self.seen_unique_values.values():
            tracker.clear()

    def set_schema(self, schema: DatasetSchema, mode: Optional[str] = None) -> None:
        """Update schema dynamically (e.g. after schema inference)."""
        self.schema = schema
        if mode:
            self.mode = mode.lower()
        self._init_unique_trackers()

    def validate_record(self, raw: Dict[str, Any]) -> ValidationResult:
        """
        Validate a raw record against the current schema.

        Args:
            raw: Dictionary containing raw extracted fields.

        Returns:
            ValidationResult with boolean flag and descriptive rejection errors.
        """
        errors: List[str] = []
        warnings: List[str] = []

        if not isinstance(raw, dict):
            return ValidationResult(
                is_valid=False,
                errors=[f"Record must be a key-value object, got {type(raw).__name__}"]
            )

        # 1. Validate Required Fields & Check for Missing Values
        for fname, fdef in self.schema.fields.items():
            if fdef.required:
                if fname not in raw:
                    errors.append(f"Missing required field: '{fname}'")
                else:
                    val = raw[fname]
                    if val is None or (isinstance(val, str) and val.strip() == ""):
                        errors.append(f"Required field '{fname}' cannot be empty or blank")

        # 2. Validate Uniqueness (Identifier Columns)
        for fname, seen_set in self.seen_unique_values.items():
            raw_val = raw.get(fname)
            if raw_val is not None and str(raw_val).strip() != "":
                val_str = str(raw_val).strip()
                if val_str in seen_set:
                    errors.append(
                        f"Duplicate {fname}: '{val_str}' already encountered in this batch"
                    )
                else:
                    seen_set.add(val_str)

        # 3. Validate Data Types, Bounds, and Formatting for Present Values
        for fname, raw_val in raw.items():
            if raw_val is None or (isinstance(raw_val, str) and raw_val.strip() == ""):
                # Optional empty field: safe to skip type checks (non-blocking)
                continue

            val_str = str(raw_val).strip()
            fdef = self.schema.get_field(fname)

            if not fdef:
                # Field present in record but not in schema
                if not self.schema.allow_extra_fields:
                    errors.append(f"Unexpected extra field: '{fname}' is not permitted by schema")
                continue

            # Check Integer
            if fdef.data_type == "integer":
                try:
                    f = float(val_str)
                    if not f.is_integer() or ("." in val_str and not val_str.endswith(".0")):
                        errors.append(f"Invalid age: '{val_str}' must be a whole integer" if fname == "age" else f"Invalid integer for '{fname}': '{val_str}' must be a whole integer")
                    else:
                        val_int = int(f)
                        if fdef.min_value is not None and val_int < fdef.min_value:
                            errors.append(f"Invalid age: '{val_int}' is out of acceptable bounds ({int(fdef.min_value)}-{int(fdef.max_value or 0)})" if fname == "age" else f"Value '{val_int}' for '{fname}' is below minimum ({fdef.min_value})")
                        elif fdef.max_value is not None and val_int > fdef.max_value:
                            errors.append(f"Invalid age: '{val_int}' is out of acceptable bounds ({int(fdef.min_value or 0)}-{int(fdef.max_value)})" if fname == "age" else f"Value '{val_int}' for '{fname}' exceeds maximum ({fdef.max_value})")
                except (ValueError, TypeError):
                    errors.append(f"Invalid age: '{val_str}' is not a valid number" if fname == "age" else f"Invalid integer for '{fname}': '{val_str}' is not a valid number")

            # Check Float
            elif fdef.data_type == "float":
                try:
                    val_float = float(val_str)
                    if fdef.min_value is not None and val_float < fdef.min_value:
                        errors.append(f"Value '{val_float}' for '{fname}' is below minimum ({fdef.min_value})")
                    elif fdef.max_value is not None and val_float > fdef.max_value:
                        errors.append(f"Value '{val_float}' for '{fname}' exceeds maximum ({fdef.max_value})")
                except (ValueError, TypeError):
                    errors.append(f"Invalid number for '{fname}': '{val_str}' cannot be converted to float")

            # Check Email
            elif fdef.data_type == "email":
                if not EMAIL_REGEX.match(val_str):
                    errors.append(f"Invalid email format: '{val_str}'")

            # Check Allowed Values
            if fdef.allowed_values and val_str not in fdef.allowed_values:
                errors.append(f"Invalid value '{val_str}' for '{fname}'; must be one of: {', '.join(fdef.allowed_values)}")

            # Check Custom Regex Pattern
            if fdef.regex_pattern:
                if not re.match(fdef.regex_pattern, val_str):
                    errors.append(f"Value '{val_str}' for '{fname}' does not match pattern {fdef.regex_pattern}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )


class CustomerValidator(SchemaValidator):
    """
    Backward-compatible validator pre-configured with the customer profile schema.
    Maintains seen_customer_ids property for legacy tests.
    """

    REQUIRED_FIELDS = ["customer_id", "name", "email", "age", "country"]
    EMAIL_REGEX = EMAIL_REGEX
    MIN_AGE = 0
    MAX_AGE = 120

    def __init__(self):
        super().__init__(schema=DatasetSchema.preset_customer(), mode="configured")

    @property
    def seen_customer_ids(self) -> Set[str]:
        return self.seen_unique_values.setdefault("customer_id", set())

    @seen_customer_ids.setter
    def seen_customer_ids(self, val: Set[str]) -> None:
        self.seen_unique_values["customer_id"] = val
