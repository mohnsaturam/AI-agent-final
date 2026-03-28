"""
MCP Tool — store_url_pattern (Step 8e)

Persist successful URL pattern for future reuse (P1.3).
Also persists failures to penalize in future lookups (P3.8).
"""

import json
import logging
import re
from typing import Any, Dict
from urllib.parse import urlparse

logger = logging.getLogger("umsa.tools.store_url_pattern")


def _normalize_title(title: str) -> str:
    """Normalize a title for cache key: lowercase, trim, strip punctuation."""
    t = (title or "").strip().lower()
    # Strip non-alphanumeric except spaces
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return t.strip()


def _extract_entity_id(url: str) -> str:
    """
    Extract the entity identifier from the last meaningful URL path segment.
    Strips query parameters for clean IDs.

    Examples:
        https://www.imdb.com/title/tt0816692/?ref_=fn_al_tt_1 → tt0816692
        https://www.metacritic.com/movie/interstellar → interstellar
        https://letterboxd.com/film/the-brutalist/ → the-brutalist
    """
    path = urlparse(url).path
    segments = [s for s in path.split("/") if s]
    if not segments:
        return ""
    # Take last segment, strip any query params that leaked into the path
    entity_id = segments[-1].split("?")[0]
    return entity_id


async def execute(context, db_pool) -> Dict[str, Any]:
    """Persist a URL pattern (success or failure) for cache reuse."""
    input_data = context.input_data
    domain = input_data.get("domain", "")
    site_domain = input_data.get("site_domain", "")
    url = input_data.get("url", "")
    pattern_type = input_data.get("pattern_type", "search")
    confidence = float(input_data.get("confidence", 0.0))
    schema_version = input_data.get("schema_version", "")
    success = input_data.get("success", True)
    ttl_seconds = input_data.get("ttl_seconds", 3600)
    metadata = input_data.get("metadata", {})
    intent = input_data.get("intent", {})

    # Persist NORMALIZED entity title for consistent cache key matching
    if intent.get("title") and "entity_title" not in metadata:
        metadata["entity_title"] = _normalize_title(intent["title"])

    # Extract and persist entity_id from URL path
    if url and "entity_id" not in metadata:
        entity_id = _extract_entity_id(url)
        if entity_id:
            metadata["entity_id"] = entity_id

    # For failures, set negative confidence to penalize
    if not success:
        confidence = max(-1.0, confidence * -1) if confidence > 0 else -0.5

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO umsa_core.url_patterns
                    (domain, site_domain, pattern, pattern_type,
                     confidence, schema_version, status, expires_at, metadata)
                VALUES ($1, $2, $3, $4, $5, $6,
                        CASE WHEN $5 > 0 THEN 'valid'::umsa_core.cache_status
                             ELSE 'invalidated'::umsa_core.cache_status END,
                        now() + make_interval(secs => $7), $8::jsonb)
                ON CONFLICT ON CONSTRAINT url_patterns_pkey DO NOTHING
                """,
                domain,
                site_domain,
                url,
                pattern_type,
                confidence,
                schema_version,
                ttl_seconds,
                json.dumps(metadata, default=str),
            )
    except Exception as e:
        # Try simpler insert without ON CONFLICT
        logger.warning("Primary insert failed for url_pattern, trying fallback: %s", e)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO umsa_core.url_patterns
                        (domain, site_domain, pattern, pattern_type,
                         confidence, schema_version, status, expires_at, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, 'valid',
                            now() + make_interval(secs => $7), $8::jsonb)
                    """,
                    domain,
                    site_domain,
                    url,
                    pattern_type,
                    abs(confidence),
                    schema_version,
                    ttl_seconds,
                    json.dumps(metadata, default=str),
                )
        except Exception as e2:
            logger.error("Failed to store URL pattern: %s", e2)

    status = "stored_success" if success else "stored_failure"
    logger.info(
        "URL pattern %s: %s → %s (confidence: %.2f, entity_title: %s)",
        status, site_domain, url, confidence,
        metadata.get("entity_title", "N/A"),
    )

    return {
        "stored": True,
        "status": status,
        "site_domain": site_domain,
        "url": url,
        "confidence": confidence,
    }

