"""
MCP Tool — validate_intent_constraints (Step 3b)

Business rules: actionability gate, required fields per query_type.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.validate_intent_constraints")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Validate intent against business constraints."""
    input_data = context.input_data
    parsed_intent = input_data.get("parsed_intent", {})
    confidence = input_data.get("confidence", 0.0)

    reasons = []

    # Must have a query_type
    query_type = parsed_intent.get("query_type")
    if not query_type:
        reasons.append("query_type is missing")

    # Confidence gate
    if confidence < 0.75:
        reasons.append(f"confidence {confidence:.2f} is below 0.75 threshold")

    # For 'details' queries — must have a title
    if query_type == "details" and not parsed_intent.get("title"):
        reasons.append("title is required for detail queries")

    # For 'compare' queries — must have at least 2 titles
    if query_type == "compare":
        titles = parsed_intent.get("titles", [])
        if len(titles) < 2:
            reasons.append("compare queries need at least 2 titles")

    # For 'list' or 'search' queries — must have at least one constraint
    if query_type in ("list", "search"):
        filters = parsed_intent.get("filters", {})
        has_rich_constraint = False
        if isinstance(filters, dict) and filters:
            # Check if any filter has a non-null value
            has_rich_constraint = any(v is not None for v in filters.values())

        has_constraint = any([
            parsed_intent.get("title"),
            parsed_intent.get("titles"),
            parsed_intent.get("year"),
            parsed_intent.get("year_range"),
            parsed_intent.get("language"),
            parsed_intent.get("genre"),
            parsed_intent.get("genres"),
            parsed_intent.get("director"),
            parsed_intent.get("actor"),
            parsed_intent.get("sort_by"),
            has_rich_constraint,
        ])
        if not has_constraint:
            reasons.append(
                "query is too broad — specify at least a year, language, genre, director, or title"
            )

    actionable = len(reasons) == 0

    result = {
        "actionable": actionable,
        "reasons": reasons,
    }

    if not actionable:
        result["message"] = (
            "Your request is too broad or ambiguous. "
            "Please specify details such as ranking type, year, genre, or comparison scope. "
            f"(Issues: {'; '.join(reasons)})"
        )

    return result
