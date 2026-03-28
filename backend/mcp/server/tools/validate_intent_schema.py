"""
MCP Tool — validate_intent_schema (Step 3a)

Deep JSON schema validation (types, ranges, enums).
"""

import json
import logging
from typing import Any, Dict

import jsonschema

logger = logging.getLogger("umsa.tools.validate_intent_schema")

# Standard intent schema for validation
INTENT_SCHEMA = {
    "type": "object",
    "required": ["query_type"],
    "properties": {
        "query_type": {
            "type": "string",
            "enum": ["details", "list", "search", "compare", "collection", "comparison", "discovery"],
        },
        "title": {"type": ["string", "null"]},
        "titles": {"type": ["array", "null"], "items": {"type": "string"}},
        "year": {"type": ["integer", "string", "null"]},
        "year_range": {
            "type": ["object", "null"],
            "properties": {
                "start": {"type": ["integer", "null"]},
                "end": {"type": ["integer", "null"]},
            },
        },
        "language": {"type": ["string", "null"]},
        "genre": {"type": ["string", "null"]},
        "genres": {"type": ["array", "null"], "items": {"type": "string"}},
        "director": {"type": ["string", "null"]},
        "actor": {"type": ["string", "null"]},
        "sort_by": {"type": ["string", "null"]},
        "limit": {"type": ["integer", "null"]},
        "entity": {"type": ["string", "null"]},
        "comparison": {"type": ["boolean", "null"], "default": False},
        "primary_goal": {"type": ["string", "null"]},
        "secondary_goal": {"type": ["string", "null"]},
        "filters": {"type": ["object", "null"]},
        "ranking_strategy": {"type": ["string", "null"]},
        "requested_fields": {"type": ["array", "null"], "items": {"type": "string"}},
        "conditional_constraints": {"type": ["array", "null"], "items": {"type": "string"}},
    },
}


async def execute(context, db_pool) -> Dict[str, Any]:
    """Validate parsed intent against the intent schema."""
    input_data = context.input_data
    parsed_intent = input_data.get("parsed_intent", {})
    domain = input_data.get("domain", "")

    errors = []

    # Validate against JSON schema
    try:
        jsonschema.validate(instance=parsed_intent, schema=INTENT_SCHEMA)
    except jsonschema.ValidationError as e:
        errors.append(f"Schema validation: {e.message}")
    except jsonschema.SchemaError as e:
        errors.append(f"Internal schema error: {e.message}")

    # Additional type checks
    if "year" in parsed_intent and parsed_intent["year"] is not None:
        year = parsed_intent["year"]
        if isinstance(year, str):
            try:
                year = int(year)
            except ValueError:
                errors.append(f"Invalid year value: {year}")
        if isinstance(year, int) and (year < 1888 or year > 2030):
            errors.append(f"Year {year} out of valid range (1888-2030)")

    if "limit" in parsed_intent and parsed_intent["limit"] is not None:
        limit = parsed_intent["limit"]
        if isinstance(limit, int) and (limit < 1 or limit > 250):
            errors.append(f"Limit {limit} out of valid range (1-250)")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "parsed_intent": parsed_intent,
    }
