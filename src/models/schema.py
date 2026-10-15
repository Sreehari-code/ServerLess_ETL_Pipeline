"""
Dataset Schema and Data Modeling.

Provides schema definitions, field rules, and schema inference containers
to decouple the ETL pipeline from any specific domain (e.g. customer, employee, sales).
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Union


@dataclass
class FieldDefinition:
    """Validation and transformation specification for a single column/field."""
    name: str
    data_type: str = "string"  # "string", "integer", "float", "boolean", "email", "date", "any"
    required: bool = False
    unique: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    regex_pattern: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FieldDefinition":
        return cls(
            name=data["name"],
            data_type=data.get("data_type", "string"),
            required=data.get("required", False),
            unique=data.get("unique", False),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            regex_pattern=data.get("regex_pattern"),
            allowed_values=data.get("allowed_values"),
            description=data.get("description"),
        )


@dataclass
class DatasetSchema:
    """Configured or inferred schema for a dataset."""
    name: str = "inferred_schema"
    fields: Dict[str, FieldDefinition] = field(default_factory=dict)
    identifier_field: Optional[str] = None
    allow_extra_fields: bool = True
    strict_types: bool = False
    description: Optional[str] = None

    def add_field(self, field_def: FieldDefinition) -> None:
        self.fields[field_def.name] = field_def
        if field_def.unique and not self.identifier_field:
            self.identifier_field = field_def.name

    def get_field(self, name: str) -> Optional[FieldDefinition]:
        return self.fields.get(name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "identifier_field": self.identifier_field,
            "allow_extra_fields": self.allow_extra_fields,
            "strict_types": self.strict_types,
            "description": self.description,
            "fields": {k: v.to_dict() for k, v in self.fields.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetSchema":
        schema = cls(
            name=data.get("name", "custom_schema"),
            identifier_field=data.get("identifier_field"),
            allow_extra_fields=data.get("allow_extra_fields", True),
            strict_types=data.get("strict_types", False),
            description=data.get("description"),
        )
        fields_data = data.get("fields", {})
        if isinstance(fields_data, dict):
            for fname, fdef in fields_data.items():
                if isinstance(fdef, dict):
                    fdef["name"] = fname
                    schema.add_field(FieldDefinition.from_dict(fdef))
        elif isinstance(fields_data, list):
            for fdef in fields_data:
                if isinstance(fdef, dict):
                    schema.add_field(FieldDefinition.from_dict(fdef))
        return schema

    @classmethod
    def preset_customer(cls) -> "DatasetSchema":
        """Preset schema for customer datasets."""
        schema = cls(name="customer_schema", identifier_field="customer_id")
        schema.add_field(FieldDefinition("customer_id", data_type="string", required=True, unique=True))
        schema.add_field(FieldDefinition("name", data_type="string", required=True))
        schema.add_field(FieldDefinition("email", data_type="email", required=True))
        schema.add_field(FieldDefinition("age", data_type="integer", required=True, min_value=0, max_value=120))
        schema.add_field(FieldDefinition("country", data_type="string", required=True))
        return schema

    @classmethod
    def preset_employee(cls) -> "DatasetSchema":
        """Preset schema for employee datasets."""
        schema = cls(name="employee_schema", identifier_field="employee_id")
        schema.add_field(FieldDefinition("employee_id", data_type="string", required=True, unique=True))
        schema.add_field(FieldDefinition("employee_name", data_type="string", required=True))
        schema.add_field(FieldDefinition("salary", data_type="float", required=True, min_value=0.0))
        schema.add_field(FieldDefinition("department", data_type="string", required=False))
        return schema

    @classmethod
    def preset_sales(cls) -> "DatasetSchema":
        """Preset schema for sales orders datasets."""
        schema = cls(name="sales_schema", identifier_field="order_id")
        schema.add_field(FieldDefinition("order_id", data_type="string", required=True, unique=True))
        schema.add_field(FieldDefinition("product", data_type="string", required=True))
        schema.add_field(FieldDefinition("quantity", data_type="integer", required=True, min_value=1))
        schema.add_field(FieldDefinition("price", data_type="float", required=True, min_value=0.0))
        schema.add_field(FieldDefinition("order_date", data_type="date", required=False))
        return schema


@dataclass
class SchemaInferenceResult:
    """Analysis results from automatic schema detection."""
    detected_columns: List[str]
    inferred_types: Dict[str, str]
    null_counts: Dict[str, int]
    total_sampled_records: int
    potential_identifier: Optional[str]
    warnings: List[str] = field(default_factory=list)
    inferred_schema: Optional[DatasetSchema] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "detected_columns": self.detected_columns,
            "inferred_types": self.inferred_types,
            "null_counts": self.null_counts,
            "total_sampled_records": self.total_sampled_records,
            "potential_identifier": self.potential_identifier,
            "warnings": self.warnings,
            "inferred_schema": self.inferred_schema.to_dict() if self.inferred_schema else None,
        }


@dataclass
class DataRecord:
    """
    Universal domain representation for an accepted record of any schema.
    Provides dictionary-like access while preserving metadata.
    """
    data: Dict[str, Any]
    processed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Flatten record with processed_at metadata."""
        return {**self.data, "_processed_at": self.processed_at}

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()
