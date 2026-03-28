"""
MCP Tool — check_intent_cache (Step 4a)

Four-level cache check:
  1. Exact intent_hash match in intent_cache
  2. Exact intent_hash match in unified_results (full cached result)
  3. Semantic fallback: match by filters + primary_goal when hash misses
     (handles AI non-determinism in query_type classification)
  4. Semantic key match: normalized canonical key matching across
     semantically identical queries (handles AI non-determinism in
     field naming, e.g. "release_year" vs "year")

Returns cached data if available, skipping all downstream processing.
"""

import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.check_intent_cache")


def _build_semantic_key(parsed_intent: dict, domain: str) -> str:
    """
    Build a normalized semantic key from a parsed intent.

    Extracts canonical fields regardless of how the AI structured them,
    so that semantically identical queries produce the SAME key.

    Example: "movies released in the year of 2025" and "movies in the year of 2025"
    both produce: "movie|movie|2025|_|_|_|_|find_movies"
    """
    # Extract entity
    entity = (parsed_intent.get("entity") or "").strip().lower()

    # Extract year — could be in filters.release_year.value, filters.year, or top-level year
    year = ""
    filters = parsed_intent.get("filters", {})
    if isinstance(filters, dict):
        release_year = filters.get("release_year", {})
        if isinstance(release_year, dict):
            year = str(release_year.get("value", ""))
        elif release_year:
            year = str(release_year)
        if not year:
            filter_year = filters.get("year", "")
            if isinstance(filter_year, dict):
                year = str(filter_year.get("value", ""))
            elif filter_year:
                year = str(filter_year)
    if not year:
        top_year = parsed_intent.get("year", "")
        if top_year:
            year = str(top_year)

    # Extract language
    language = ""
    if isinstance(filters, dict):
        language = (filters.get("language") or "").strip().lower()
    if not language:
        language = (parsed_intent.get("language") or "").strip().lower()

    # Extract title
    title = (parsed_intent.get("title") or "").strip().lower()

    # Extract genre
    genre = ""
    if isinstance(filters, dict):
        genre = (filters.get("genre") or "").strip().lower()
    if not genre:
        genre = (parsed_intent.get("genre") or "").strip().lower()

    # Extract limit
    limit = str(parsed_intent.get("limit", "")) if parsed_intent.get("limit") else ""

    # Extract primary_goal
    primary_goal = (parsed_intent.get("primary_goal") or "").strip().lower()

    # Build the key — use "_" for empty fields
    parts = [
        domain.strip().lower(),
        entity or "_",
        year or "_",
        language or "_",
        title or "_",
        genre or "_",
        limit or "_",
        primary_goal or "_",
    ]
    return "|".join(parts)


async def execute(context, db_pool) -> Dict[str, Any]:
    """Check intent cache for existing results."""
    input_data = context.input_data
    domain = input_data.get("domain", "")
    intent_hash = input_data.get("intent_hash", "")
    schema_version = input_data.get("schema_version", "")
    parsed_intent = input_data.get("parsed_intent", {})
    requested_sites = input_data.get("sites", [])  # Optional: specific sites requested

    # ── Level 1: Check intent cache for parsed intent (exact hash) ──
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT parsed_intent, confidence
            FROM umsa_core.intent_cache
            WHERE domain = $1
              AND intent_hash = $2
              AND schema_version = $3
              AND status = 'valid'
              AND expires_at > now()
            LIMIT 1
            """,
            domain,
            intent_hash,
            schema_version,
        )

    if row:
        logger.info("Intent cache HIT for hash %s", intent_hash[:16])

    # ── Level 2: Check for full unified result cache (exact hash) ──
    # Join with requests to verify the original request actually COMPLETED
    async with db_pool.acquire() as conn:
        unified_row = await conn.fetchrow(
            """
            SELECT ur.unified_data, ur.source_sites,
                   ur.resolved_conflicts, ur.confidence,
                   r.raw_query, r.state
            FROM umsa_core.unified_results ur
            JOIN umsa_core.requests r ON r.id = ur.request_id
            WHERE ur.domain = $1
              AND ur.intent_hash = $2
              AND ur.schema_version = $3
              AND ur.status = 'valid'
              AND ur.expires_at > now()
              AND r.state = 'COMPLETED'
            ORDER BY ur.created_at DESC
            LIMIT 1
            """,
            domain,
            intent_hash,
            schema_version,
        )

    if unified_row:
        # Verify if the cached sites satisfy requested sites
        if _is_cache_valid_for_request(unified_row, requested_sites, parsed_intent):
            logger.info("Full unified cache HIT for hash %s (request COMPLETED)", intent_hash[:16])
            return _build_cache_response(unified_row, "exact_hash")
        else:
            logger.info("Cache hit for hash %s but site/year mismatch - ignoring", intent_hash[:16])

    # ── Level 3: Semantic fallback (exact filters + primary_goal JSON match) ──
    filters = parsed_intent.get("filters", {})
    primary_goal = parsed_intent.get("primary_goal", "")

    if filters and primary_goal:
        filters_json = json.dumps(filters, sort_keys=True, default=str)
        async with db_pool.acquire() as conn:
            semantic_row = await conn.fetchrow(
                """
                SELECT ur.unified_data, ur.source_sites,
                       ur.resolved_conflicts, ur.confidence,
                       ic.parsed_intent,
                       r.raw_query, r.state
                FROM umsa_core.unified_results ur
                JOIN umsa_core.requests r ON r.id = ur.request_id
                JOIN umsa_core.intent_cache ic ON ic.intent_hash = ur.intent_hash
                    AND ic.domain = ur.domain
                WHERE ur.domain = $1
                  AND ur.schema_version = $2
                  AND ur.status = 'valid'
                  AND ur.expires_at > now()
                  AND r.state = 'COMPLETED'
                  AND ic.parsed_intent->'filters' = $3::jsonb
                  AND ic.parsed_intent->>'primary_goal' = $4
                ORDER BY ur.confidence DESC, ur.created_at DESC
                LIMIT 1
                """,
                domain,
                schema_version,
                filters_json,
                primary_goal,
            )

        if semantic_row:
            if _is_cache_valid_for_request(semantic_row, requested_sites, parsed_intent):
                cached_intent = semantic_row["parsed_intent"] if isinstance(semantic_row["parsed_intent"], dict) else json.loads(semantic_row["parsed_intent"])
                logger.info(
                    "Semantic cache HIT! filters=%.50s, primary_goal=%s "
                    "(cached query_type=%s, current may differ)",
                    filters_json, primary_goal,
                    cached_intent.get("query_type", "?"),
                )
                return _build_cache_response(semantic_row, "semantic_match")

    # ── Level 4: Semantic key match (normalized canonical key) ──
    semantic_key = _build_semantic_key(parsed_intent, domain)
    logger.info("Level 4: Checking semantic key: %s", semantic_key)

    try:
        async with db_pool.acquire() as conn:
            # We fetch top candidates and verify them one by one to handle year/site strictness
            rows = await conn.fetch(
                """
                SELECT ur.unified_data, ur.source_sites,
                       ur.resolved_conflicts, ur.confidence,
                       ic.parsed_intent, ic.semantic_key,
                       r.raw_query, r.state
                FROM umsa_core.intent_cache ic
                JOIN umsa_core.unified_results ur
                    ON ur.intent_hash = ic.intent_hash
                    AND ur.domain = ic.domain
                    AND ur.schema_version = ic.schema_version
                JOIN umsa_core.requests r ON r.id = ur.request_id
                WHERE ic.domain = $1
                  AND umsa_core.similarity(ic.semantic_key, $2) > 0.65
                  AND ic.schema_version = $3
                  AND ic.status = 'valid'
                  AND ic.expires_at > now()
                  AND ur.status = 'valid'
                  AND ur.expires_at > now()
                  AND r.state = 'COMPLETED'
                ORDER BY umsa_core.similarity(ic.semantic_key, $2) DESC, ur.confidence DESC
                LIMIT 5
                """,
                domain,
                semantic_key,
                schema_version,
            )
            
            for row in rows:
                if _is_cache_valid_for_request(row, requested_sites, parsed_intent):
                    original_query = row.get("raw_query", "unknown")
                    matched_key = row.get("semantic_key", "unknown")
                    logger.info(
                        "Level 4 Semantic Key HIT! query: '%s', matched_key: '%s' (input_key: '%s') → returning cached result",
                        original_query, matched_key, semantic_key,
                    )
                    response = _build_cache_response(row, "semantic_key")
                    response["semantic_key"] = matched_key
                    return response

    except Exception as e:
        logger.warning(f"Semantic key lookup failed: {e}. Falling back to strict matching.")
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT ur.unified_data, ur.source_sites,
                       ur.resolved_conflicts, ur.confidence,
                       ic.parsed_intent, ic.semantic_key,
                       r.raw_query, r.state
                FROM umsa_core.intent_cache ic
                JOIN umsa_core.unified_results ur
                    ON ur.intent_hash = ic.intent_hash
                    AND ur.domain = ic.domain
                    AND ur.schema_version = ic.schema_version
                JOIN umsa_core.requests r ON r.id = ur.request_id
                WHERE ic.domain = $1
                  AND ic.semantic_key = $2
                  AND ic.schema_version = $3
                  AND ic.status = 'valid'
                  AND ic.expires_at > now()
                  AND ur.status = 'valid'
                  AND ur.expires_at > now()
                  AND r.state = 'COMPLETED'
                ORDER BY ur.confidence DESC
                LIMIT 1
                """,
                domain,
                semantic_key,
                schema_version,
            )
            if row and _is_cache_valid_for_request(row, requested_sites, parsed_intent):
                return _build_cache_response(row, "semantic_key")

    logger.info("Intent cache MISS for hash %s (all 4 levels checked, semantic_key=%s)", intent_hash[:16], semantic_key)
    return {"cache_hit": False, "semantic_key": semantic_key}


def _is_cache_valid_for_request(row, requested_sites: list, current_intent: dict) -> bool:
    """
    Stricter verification of a cache hit.
    Matches Year and Language EXACTLY, and respects requested Sites.
    """
    cached_intent = row["parsed_intent"]
    if isinstance(cached_intent, str):
        cached_intent = json.loads(cached_intent)

    # 1. Verify Year (must match exactly if present)
    current_year = _get_year(current_intent)
    cached_year = _get_year(cached_intent)
    if current_year and cached_year and current_year != cached_year:
        return False
    if (current_year and not cached_year) or (not current_year and cached_year):
        # One has a year filter, the other doesn't
        return False

    # 2. Verify Language
    current_lang = _get_language(current_intent)
    cached_lang = _get_language(cached_intent)
    if current_lang and cached_lang and current_lang != cached_lang:
        return False

    # 3. Verify Sites (if user asked for specific sites, ensure cache covers them)
    if requested_sites:
        source_sites = set(row["source_sites"] or [])
        # If the user asked for ['wikipedia.org'] and cache only has ['imdb.com'], it's a miss.
        # We require that AT LEAST ONE requested site is present in the cache.
        if not any(site in source_sites for site in requested_sites):
            return False

    return True


def _get_year(intent: dict) -> str:
    """Helper to extract year from intent dict."""
    filters = intent.get("filters", {})
    if not isinstance(filters, dict):
        filters = {}
    
    val = filters.get("release_year")
    if isinstance(val, dict):
        return str(val.get("value", ""))
    if val:
        return str(val)
        
    val = filters.get("year")
    if isinstance(val, dict):
        return str(val.get("value", ""))
    if val:
        return str(val)
        
    return str(intent.get("year", ""))


def _get_language(intent: dict) -> str:
    """Helper to extract language from intent dict."""
    filters = intent.get("filters", {})
    if not isinstance(filters, dict):
        filters = {}
    lang = filters.get("language") or intent.get("language")
    return str(lang).strip().lower() if lang else ""


def _build_cache_response(row, match_type: str) -> Dict[str, Any]:
    """Build standardized cache response from a DB row."""
    unified_data = row["unified_data"] if isinstance(row["unified_data"], dict) else json.loads(row["unified_data"])
    source_sites = list(row["source_sites"]) if row["source_sites"] else []

    cached_data = {
        "unified_data": unified_data,
        "source_sites": source_sites,
        "resolved_conflicts": row["resolved_conflicts"] if isinstance(row["resolved_conflicts"], dict) else json.loads(row["resolved_conflicts"]),
        "confidence": float(row["confidence"]),
    }

    response = {
        "cache_hit": True,
        "full_result_cached": True,
        "cached_data": cached_data,
        "match_type": match_type,
    }

    # Include execution status tracking if available
    if "state" in row.keys():
        response["execution_status"] = str(row["state"])
    if "raw_query" in row.keys():
        response["original_query"] = str(row["raw_query"])

    return response
