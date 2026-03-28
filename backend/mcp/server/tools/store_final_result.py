"""
MCP Tool — store_final_result (Step 12b)

Persist result, update request state.
Cache for future reuse (both unified_results AND intent_cache).
"""

import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.store_final_result")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Persist final unified result and cache."""
    input_data = context.input_data
    request_id = input_data.get("request_id", "")
    domain = input_data.get("domain", "")
    intent_hash = input_data.get("intent_hash", "")
    schema_version = input_data.get("schema_version", "")
    unified_data = input_data.get("unified_data", {})
    source_sites = input_data.get("source_sites", [])
    resolved_conflicts = input_data.get("resolved_conflicts", {})
    confidence = float(input_data.get("confidence", 0.0))
    ttl_seconds = input_data.get("ttl_seconds", 1800)
    parsed_intent = input_data.get("parsed_intent", {})

    # Persist to requests table
    try:
        async with db_pool.acquire() as conn:
            result_json = json.dumps({
                "unified_data": unified_data,
                "source_sites": source_sites,
                "confidence": confidence,
                "resolved_conflicts": resolved_conflicts,
            }, default=str)

            await conn.execute(
                """
                UPDATE umsa_core.requests
                SET result = $1::jsonb, updated_at = now()
                WHERE id = $2
                """,
                result_json,
                request_id,
            )
    except Exception as e:
        logger.warning("Failed to update request result: %s", e)

    # Cache unified result for future reuse
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO umsa_core.unified_results
                    (request_id, domain, intent_hash, unified_data,
                     source_sites, resolved_conflicts, confidence,
                     schema_version, status, expires_at)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7,
                        $8, 'valid', now() + make_interval(secs => $9))
                ON CONFLICT ON CONSTRAINT unified_results_pkey DO NOTHING
                """,
                request_id,
                domain,
                intent_hash,
                json.dumps(unified_data, default=str),
                source_sites,
                json.dumps(resolved_conflicts, default=str),
                confidence,
                schema_version,
                ttl_seconds,
            )
    except Exception as e:
        logger.warning("Failed to cache unified result: %s", e)

    # ── NEW: Also populate intent_cache for Step 4a reuse ──
    if intent_hash and parsed_intent:
        # Compute semantic key for Level 4 matching
        from mcp.server.tools.check_intent_cache import _build_semantic_key
        semantic_key = _build_semantic_key(parsed_intent, domain)

        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO umsa_core.intent_cache
                        (domain, intent_hash, schema_version, parsed_intent,
                         confidence, status, expires_at, semantic_key)
                    VALUES ($1, $2, $3, $4::jsonb, $5, 'valid',
                            now() + make_interval(secs => $6), $7)
                    ON CONFLICT DO NOTHING
                    """,
                    domain,
                    intent_hash,
                    schema_version,
                    json.dumps(parsed_intent, default=str),
                    confidence,
                    ttl_seconds,
                    semantic_key,
                )
                logger.info(
                    "Intent cache populated for hash %s (confidence=%.2f, semantic_key=%s)",
                    intent_hash[:16], confidence, semantic_key,
                )
        except Exception as e:
            logger.warning("Failed to populate intent_cache: %s", e)

    return {
        "stored": True,
        "request_id": str(request_id),
        "fields_count": len(unified_data) if isinstance(unified_data, dict) else 0,
        "source_count": len(source_sites),
    }
