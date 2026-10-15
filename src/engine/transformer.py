"""
Data Transformer Module.

Standardizes, cleans, and enriches validated records.
Supports generic schema transformations as well as domain-specific mappings.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from src.models.record import CustomerRecord
from src.models.schema import DatasetSchema, DataRecord


class Transformer:
    """Transforms raw record attributes into clean, standardized domain representations."""

    # Standard country mapping table (aliases -> ISO-3 / standard alpha)
    COUNTRY_MAPPINGS = {
        # United States
        "US": "USA",
        "USA": "USA",
        "UNITED STATES": "USA",
        "UNITED STATES OF AMERICA": "USA",
        "U.S.A.": "USA",
        "U.S.": "USA",

        # United Kingdom
        "UK": "GBR",
        "GB": "GBR",
        "GBR": "GBR",
        "UNITED KINGDOM": "GBR",
        "GREAT BRITAIN": "GBR",

        # India
        "IN": "IND",
        "IND": "IND",
        "INDIA": "IND",

        # Canada
        "CA": "CAN",
        "CAN": "CAN",
        "CANADA": "CAN",

        # Germany
        "DE": "DEU",
        "DEU": "DEU",
        "GERMANY": "DEU",
        "DEUTSCHLAND": "DEU",

        # France
        "FR": "FRA",
        "FRA": "FRA",
        "FRANCE": "FRA",

        # Australia
        "AU": "AUS",
        "AUS": "AUS",
        "AUSTRALIA": "AUS",

        # Japan
        "JP": "JPN",
        "JPN": "JPN",
        "JAPAN": "JPN",

        # Singapore
        "SG": "SGP",
        "SGP": "SGP",
        "SINGAPORE": "SGP",
    }

    @classmethod
    def transform_record(cls, raw: Dict[str, Any]) -> CustomerRecord:
        """
        Clean, normalize, and transform a valid customer record.
        Maintained for customer schema backward-compatibility.

        Args:
            raw: Dictionary containing valid fields.

        Returns:
            CustomerRecord domain model instance.
        """
        # 1. Standardize Customer ID
        customer_id = str(raw["customer_id"]).strip()

        # 2. Trim Whitespace from Name
        name = str(raw["name"]).strip()

        # 3. Normalize Email to lowercase and trimmed
        email = str(raw["email"]).strip().lower()

        # 4. Cast Age to integer
        age = int(float(str(raw["age"]).strip()))

        # 5. Standardize Country
        country_raw = str(raw["country"]).strip().upper()
        country = cls.COUNTRY_MAPPINGS.get(country_raw, country_raw)

        return CustomerRecord(
            customer_id=customer_id,
            name=name,
            email=email,
            age=age,
            country=country,
        )

    @classmethod
    def transform_generic(
        cls,
        raw: Dict[str, Any],
        schema: Optional[DatasetSchema] = None,
    ) -> DataRecord:
        """
        Schema-aware generic record transformation for any dataset.
        Trims whitespace, normalizes types, and applies domain mappings if defined.

        Args:
            raw: Dictionary of valid fields.
            schema: Optional DatasetSchema definition.

        Returns:
            DataRecord domain instance.
        """
        transformed: Dict[str, Any] = {}

        for k, v in raw.items():
            if v is None:
                transformed[k] = None
                continue

            # Universal: strip whitespace from string values
            if isinstance(v, str):
                v_clean = v.strip()
            else:
                v_clean = v

            fdef = schema.get_field(k) if schema else None

            # Type conversions based on schema
            if fdef:
                if fdef.data_type == "integer" and v_clean != "" and v_clean is not None:
                    try:
                        v_clean = int(float(str(v_clean)))
                    except (ValueError, TypeError):
                        pass
                elif fdef.data_type == "float" and v_clean != "" and v_clean is not None:
                    try:
                        v_clean = float(str(v_clean))
                    except (ValueError, TypeError):
                        pass
                elif fdef.data_type == "email" and isinstance(v_clean, str):
                    v_clean = v_clean.lower()
                elif fdef.data_type == "boolean":
                    if isinstance(v_clean, str):
                        v_clean = v_clean.lower() in {"true", "1", "yes"}

            # Domain normalization: normalize email to lowercase if field named 'email'
            if k.lower() == "email" and isinstance(v_clean, str):
                v_clean = v_clean.lower()

            # Domain normalization: normalize country if field named 'country'
            if k.lower() == "country" and isinstance(v_clean, str) and v_clean:
                v_upper = v_clean.upper()
                v_clean = cls.COUNTRY_MAPPINGS.get(v_upper, v_upper)

            transformed[k] = v_clean

        return DataRecord(data=transformed)
