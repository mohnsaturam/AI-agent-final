"""
MCP Server — Capability Resolution Tool (Step 4a)

Deterministic capability vector generation from parsed intent.
Converts semantic meaning into behavioral flags.
Zero AI calls. Zero domain-specific logic.

This tool runs AFTER validate_intent_constraints (Step 4)
and BEFORE plan_strategy (Step 4b).
"""

import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.resolve_capabilities")


async def execute(context, db_pool) -> Dict[str, Any]:
    """
    Resolve capability flags from parsed intent.

    Reads from both rich intent fields (filters dict, ranking_strategy)
    and flat backward-compatible fields (year, language, genre, etc.).
    Null/None values are automatically excluded from active filters.

    Returns a capability vector with boolean flags that describe
    what the execution strategy should do — NOT what the query means.
    """
    intent = context.input_data.get("parsed_intent", {})
    num_sites = context.input_data.get("num_sites", 1)

    # ── Read entity and title ──
    title = intent.get("title")
    titles = intent.get("titles")  # for multi-entity comparison

    # ── Read filters from rich 'filters' dict ──
    filters_dict = intent.get("filters", {})
    active_filters: Dict[str, Any] = {}

    if isinstance(filters_dict, dict):
        for key, val in filters_dict.items():
            if val is not None:
                # Handle nested filter objects (e.g., {"operator": ">", "value": 8})
                if isinstance(val, dict):
                    if val.get("value") is not None:
                        active_filters[key] = val
                else:
                    active_filters[key] = val

    # ── Fallback: read from flat backward-compatible fields ──
    # Only populate if the rich filters dict didn't have them
    flat_filter_fields = ("year", "language", "genre", "director", "actor",
                          "min_rating", "country")
    for field in flat_filter_fields:
        if field not in active_filters:
            val = intent.get(field)
            if val is not None:
                active_filters[field] = val

    # Also check year_range
    year_range = intent.get("year_range")
    if year_range and isinstance(year_range, dict):
        if year_range.get("from") is not None or year_range.get("to") is not None:
            if "year" not in active_filters and "release_year" not in active_filters:
                active_filters["year_range"] = year_range

    # ── Read ranking ──
    ranking_strategy = intent.get("ranking_strategy")
    ranking_obj = intent.get("ranking", {})
    sort_by = intent.get("sort_by")

    has_ranking = (
        ranking_strategy is not None
        or (isinstance(ranking_obj, dict) and ranking_obj.get("strategy") is not None)
        or sort_by is not None
    )

    # ── Read limit ──
    limit = intent.get("limit")

    # ── Read comparison flag ──
    comparison = intent.get("comparison", False)

    # ── Compute capability flags ──
    has_title = title is not None
    has_titles = titles is not None and isinstance(titles, list) and len(titles) > 0
    has_filters = len(active_filters) > 0
    has_limit = limit is not None

    # Cardinality: single if a specific entity is named
    if has_title and not has_titles:
        cardinality = "single"
    elif has_titles:
        cardinality = "multiple"
    else:
        cardinality = "multiple"

    # Aggregation: only when comparison is explicit AND multiple sites
    needs_aggregation = bool(comparison) and num_sites > 1

    # Single entity lookup: specific entity without comparison
    single_entity_lookup = has_title and not bool(comparison) and not has_titles

    capability_vector = {
        "cardinality": cardinality,
        "needs_ranking": has_ranking or has_limit,
        "needs_filtering": has_filters,
        "needs_limit": has_limit,
        "needs_aggregation": needs_aggregation,
        "needs_cross_site_alignment": bool(comparison),
        "single_entity_lookup": single_entity_lookup,
        "active_filters": active_filters,
        "filter_values": active_filters,
        "limit_value": limit,
    }

    logger.info(
        "Capability resolved: cardinality=%s, ranking=%s, filtering=%s, "
        "aggregation=%s, single_lookup=%s, filters=%s",
        cardinality, has_ranking, has_filters,
        needs_aggregation, single_entity_lookup,
        list(active_filters.keys()),
    )

    return capability_vector
