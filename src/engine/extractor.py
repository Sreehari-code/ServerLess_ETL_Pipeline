"""
Data Extractor Module.

Extracts tabular or document records from CSV and JSON files/streams.
Handles empty files, malformed syntax, and unsupported formats gracefully.
"""

import csv
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Union


class ExtractionError(Exception):
    """Raised when an input file cannot be extracted due to format, I/O, or syntax issues."""
    pass


class Extractor:
    """Extracts raw record dictionaries from CSV and JSON sources."""

    SUPPORTED_EXTENSIONS = {".csv", ".json"}

    @classmethod
    def extract_from_file(cls, file_path: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        Extract records from a local file path.

        Args:
            file_path: Path to the target file.

        Returns:
            List of dictionary records.

        Raises:
            ExtractionError: If file is missing, unsupported, empty, or malformed.
        """
        path = Path(file_path)

        if not path.exists():
            raise ExtractionError(f"Input file not found: {path}")

        if not path.is_file():
            raise ExtractionError(f"Provided path is not a file: {path}")

        ext = path.suffix.lower()
        if ext not in cls.SUPPORTED_EXTENSIONS:
            raise ExtractionError(
                f"Unsupported file format '{ext}'. Supported formats are: {', '.join(cls.SUPPORTED_EXTENSIONS)}"
            )

        try:
            content = path.read_text(encoding="utf-8-sig")  # Handles optional BOM
        except Exception as e:
            raise ExtractionError(f"Failed to read file '{path.name}': {str(e)}") from e

        return cls.extract_from_text(content, file_extension=ext)

    @classmethod
    def extract_from_text(cls, content: str, file_extension: str) -> List[Dict[str, Any]]:
        """
        Extract records from an in-memory string.

        Args:
            content: Raw file contents as string.
            file_extension: Format identifier ('.csv' or '.json').

        Returns:
            List of dictionary records.

        Raises:
            ExtractionError: If syntax is malformed or format unsupported.
        """
        ext = file_extension.lower()
        if not ext.startswith("."):
            ext = f".{ext}"

        if ext not in cls.SUPPORTED_EXTENSIONS:
            raise ExtractionError(
                f"Unsupported file format '{ext}'. Supported formats are: {', '.join(cls.SUPPORTED_EXTENSIONS)}"
            )

        stripped = content.strip()
        if not stripped:
            # Gracefully handle empty files by returning an empty list
            return []

        if ext == ".csv":
            return cls._extract_csv(content)
        elif ext == ".json":
            return cls._extract_json(content)

        return []

    @classmethod
    def extract_headers(cls, content: str, file_extension: str) -> List[str]:
        """Extract raw column headers from CSV or JSON to detect duplicates."""
        ext = file_extension.lower()
        if not ext.startswith("."):
            ext = f".{ext}"

        stripped = content.strip()
        if not stripped:
            return []

        if ext == ".csv":
            first_line = stripped.splitlines()[0]
            try:
                reader = csv.reader(io.StringIO(first_line))
                headers = next(reader, [])
                return [h.strip() for h in headers if h is not None]
            except Exception:
                return []
        elif ext == ".json":
            try:
                data = json.loads(content)
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    return list(data[0].keys())
                elif isinstance(data, dict):
                    for k in ("customers", "records", "data", "items"):
                        if k in data and isinstance(data[k], list) and data[k] and isinstance(data[k][0], dict):
                            return list(data[k][0].keys())
                    return list(data.keys())
            except Exception:
                return []
        return []

    @staticmethod
    def _extract_csv(content: str) -> List[Dict[str, Any]]:
        """Parse CSV text into list of dicts."""
        try:
            reader = csv.DictReader(io.StringIO(content))
            if reader.fieldnames is None:
                return []
            
            records = []
            for row_idx, row in enumerate(reader, start=1):
                # Clean up any None keys or values from uneven CSV rows
                cleaned_row = {}
                for k, v in row.items():
                    if k is not None:
                        cleaned_row[k.strip()] = v if v is not None else ""
                records.append(cleaned_row)
            return records
        except Exception as e:
            raise ExtractionError(f"Malformed CSV content: {str(e)}") from e

    @staticmethod
    def _extract_json(content: str) -> List[Dict[str, Any]]:
        """Parse JSON text into list of dicts."""
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ExtractionError(f"Malformed JSON syntax: {str(e)}") from e

        if isinstance(data, list):
            for idx, item in enumerate(data):
                if not isinstance(item, dict):
                    raise ExtractionError(
                        f"Malformed JSON: expected array of objects, found {type(item).__name__} at index {idx}"
                    )
            return data
        elif isinstance(data, dict):
            # Check if wrapped in a top-level key like "customers" or "records"
            for key in ("customers", "records", "data", "items"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # Single object case: treat as list of 1 record
            return [data]
        else:
            raise ExtractionError(
                f"Malformed JSON: root element must be a JSON array or object, got {type(data).__name__}"
            )
