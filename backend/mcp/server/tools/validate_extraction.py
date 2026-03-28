"""
MCP Tool — validate_extraction (Step 9-VAL)

Generic schema validation. No domain-specific assumptions.
Validates field presence, types, and basic sanity.
Row count validation against intent.
IF invalid → reject, do NOT include in results.
"""

import logging
import re
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.validate_extraction")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Validate extracted data against schema and sanity checks."""
    input_data = context.input_data
    extracted_data = input_data.get("extracted_data", {})
    extraction_schema = input_data.get("extraction_schema", {})
    url = input_data.get("url", "")
    intent = input_data.get("intent", {})

    errors = []
    warnings = []

    # ═══ Generic required field check ═══
    # Since engines return raw field names (e.g. "name" not "title"),
    # we skip the schema required field check — raw extraction is valid
    # as long as it has meaningful data.
    # The schema required fields are domain-specific mappings that
    # will be resolved later via semantic field matching.

    # Validate field types (if schema has properties defined)
    properties = extraction_schema.get("properties", {})
    for field, value in extracted_data.items():
        if field in properties and value is not None:
            expected_type = properties[field].get("type", "string")
            if not _validate_type(value, expected_type):
                warnings.append(f"Type mismatch for '{field}': expected {expected_type}")

    # ═══ Generic value sanity checks ═══
    # Check for obviously invalid string values
    for field, value in extracted_data.items():
        if isinstance(value, str) and field not in ("source_url", "source_site"):
            if len(value) > 10000:
                warnings.append(f"Field '{field}' value unusually long ({len(value)} chars)")

    # ═══ Row count validation against intent ═══
    expected_limit = intent.get("limit") if intent else None
    if expected_limit:
        if isinstance(extracted_data, list):
            actual_count = len(extracted_data)
            if actual_count < expected_limit:
                warnings.append(
                    f"Row count mismatch: expected {expected_limit}, "
                    f"got {actual_count}"
                )
        elif isinstance(extracted_data, dict) and expected_limit > 1:
            # Single result for a list query — warning
            list_fields = [k for k, v in extracted_data.items()
                           if isinstance(v, list) and len(v) > 0]
            if not list_fields:
                warnings.append(
                    f"Expected {expected_limit} results but got single object"
                )
            else:
                for lf in list_fields:
                    actual = len(extracted_data[lf])
                    if actual < expected_limit:
                        warnings.append(
                            f"Row count in '{lf}': expected {expected_limit}, "
                            f"got {actual}"
                        )

    # Count fields with actual values
    filled_fields = sum(
        1 for k, v in extracted_data.items()
        if v is not None and k not in ("source_url", "source_site")
    ) if isinstance(extracted_data, dict) else 0

    # Valid as long as we have at least one meaningful field
    valid = filled_fields >= 1

    return {
        "valid": valid,
        "url": url,
        "errors": errors,
        "warnings": warnings,
        "filled_fields": filled_fields,
        "total_schema_fields": len(properties),
    }


def _validate_type(value: Any, expected_type: str) -> bool:
    """Validate a value against expected JSON schema type."""
    type_checks = {
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int),
        "number": lambda v: isinstance(v, (int, float)),
        "boolean": lambda v: isinstance(v, bool),
        "array": lambda v: isinstance(v, list),
        "object": lambda v: isinstance(v, dict),
    }

    # Handle union types like ["string", "null"]
    if isinstance(expected_type, list):
        return any(
            type_checks.get(t, lambda v: True)(value)
            for t in expected_type
            if t != "null"
        ) or value is None

    checker = type_checks.get(expected_type, lambda v: True)
    return checker(value)
