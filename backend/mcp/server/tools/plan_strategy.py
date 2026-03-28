"""
MCP Server — Strategy Planning Tool (Step 4b)

Deterministic execution strategy from capability vector.
Converts behavioral flags into explicit execution instructions.
Zero AI calls. Zero domain-specific logic.

This tool runs AFTER resolve_capabilities (Step 4a)
and BEFORE compute_intent_hash (Step 5).

Decision Matrix (exhaustive — 4 cells):
┌─────────────┬───────────────────────┬────────────────────────────┐
│             │ no aggregation        │ needs aggregation          │
├─────────────┼───────────────────────┼────────────────────────────┤
│ single      │ entity_detail_lookup  │ per_site_lookup_then_unify │
│ multiple    │ search_endpoint_lookup│ per_site_lookup_then_unify │
└─────────────┴───────────────────────┴────────────────────────────┘
"""

import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.plan_strategy")


async def execute(context, db_pool) -> Dict[str, Any]:
    """
    Plan execution strategy from capability vector.

    Produces explicit instructions for:
    - URL generation (search vs direct)
    - Expected page type (list vs detail)
    - Extraction mode (single vs multi item)
    - Unification mode (single source vs cross-site merge)
    - Post-extraction transforms (limit, ranking)

    The output constrains all downstream execution — URL gen, scoring,
    extraction, and unification all read from this strategy.
    """
    cap = context.input_data.get("capability_vector", {})
    num_sites = context.input_data.get("num_sites", 1)
    domain_config = context.input_data.get("domain_config", {})

    cardinality = cap.get("cardinality", "multiple")
    needs_aggregation = cap.get("needs_aggregation", False)
    single_entity = cap.get("single_entity_lookup", False)
    needs_ranking = cap.get("needs_ranking", False)
    needs_limit = cap.get("needs_limit", False)
    active_filters = cap.get("active_filters", {})
    limit_value = cap.get("limit_value")

    # ══════════════════════════════════════════════
    # 4-cell decision matrix (exhaustive, no gaps)
    # ══════════════════════════════════════════════

    if single_entity and not needs_aggregation:
        # Case A: "Inception (2010)" on one site
        strategy = "entity_detail_lookup"
        url_hint = "direct"
        page_type = "detail_page"
        extraction_mode = "single_item"

    elif single_entity and needs_aggregation:
        # Case B: "Compare Inception on IMDb vs RT"
        strategy = "per_site_lookup_then_unify"
        url_hint = "direct"
        page_type = "detail_page"
        extraction_mode = "single_item"

    elif cardinality == "multiple" and needs_aggregation:
        # Case C: "Compare top action movies on IMDb vs RT"
        strategy = "per_site_lookup_then_unify"
        url_hint = "search"
        page_type = "list_page"
        extraction_mode = "multi_item"

    else:
        # Case D (default): "Top 5 movies from 2025" — multiple, no aggregation
        strategy = "search_endpoint_lookup"
        url_hint = "search"
        page_type = "list_page"
        extraction_mode = "multi_item"

    # ══════════════════════════════════════════════
    # Unification mode
    # ══════════════════════════════════════════════

    if needs_aggregation:
        unification_mode = "cross_site_merge"
    elif num_sites > 1:
        unification_mode = "best_source"
    else:
        unification_mode = "single_source"

    # ══════════════════════════════════════════════
    # Post-extraction instructions
    # ══════════════════════════════════════════════

    # Ranking: always apply at extraction level (universal fallback)
    ranking_instructions = {}
    if needs_ranking:
        ranking_instructions = {
            "apply_at": "extraction",
            "needs_sort": True,
        }

    # Filter instructions: pass through active filters for URL gen
    filter_instructions = dict(active_filters)

    execution_strategy = {
        "strategy": strategy,
        "url_pattern_hint": url_hint,
        "expected_page_type": page_type,
        "extraction_mode": extraction_mode,
        "unification_mode": unification_mode,
        "post_extraction_limit": limit_value,
        "filter_instructions": filter_instructions,
        "ranking_instructions": ranking_instructions,
    }

    logger.info(
        "Strategy planned: %s | url_hint=%s | page_type=%s | "
        "extraction=%s | unification=%s | limit=%s | filters=%s",
        strategy, url_hint, page_type,
        extraction_mode, unification_mode,
        limit_value, list(filter_instructions.keys()),
    )

    return execution_strategy
