"""
MCP Tool — store_site_relevance (Step 5-STORE)

Persist scores to site_relevance table for future reuse.
Also updates site_memory for cross-query site knowledge.
Sites below threshold → REJECTED, logged, not pipelined.
"""

import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.store_site_relevance")

RELEVANCE_THRESHOLD = 0.3


async def execute(context, db_pool) -> Dict[str, Any]:
    """Persist site relevance scores, filter by threshold, and update site memory."""
    input_data = context.input_data
    request_id = input_data.get("request_id", "")
    domain = input_data.get("domain", "")
    intent_hash = input_data.get("intent_hash", "")
    schema_version = input_data.get("schema_version", "")
    site_scores = input_data.get("site_scores", [])
    ttl_seconds = input_data.get("ttl_seconds", 1800)

    accepted_sites = []
    rejected_sites = []

    async with db_pool.acquire() as conn:
        for site_score in site_scores:
            site_url = site_score.get("site_url", "")
            score = float(site_score.get("relevance_score", 0.0))
            reasoning = site_score.get("reasoning", "")

            # Persist to per-query site_relevance table
            try:
                await conn.execute(
                    """
                    INSERT INTO umsa_core.site_relevance
                        (request_id, domain, intent_hash, site_url,
                         relevance_score, reasoning, schema_version,
                         status, expires_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'valid',
                            now() + make_interval(secs => $8))
                    ON CONFLICT ON CONSTRAINT site_relevance_pkey DO NOTHING
                    """,
                    request_id if isinstance(request_id, str) and len(request_id) == 36 else None,
                    domain,
                    intent_hash,
                    site_url,
                    score,
                    reasoning,
                    schema_version,
                    ttl_seconds,
                )
            except Exception as e:
                logger.warning("Failed to persist site relevance for %s: %s", site_url, e)

            # ── NEW: Update cross-query site_memory ──
            try:
                is_success = score >= RELEVANCE_THRESHOLD
                await conn.execute(
                    """
                    INSERT INTO umsa_core.site_memory
                        (domain, site_url, avg_relevance_score,
                         total_queries, successful_queries, failed_queries,
                         last_score, is_valid, updated_at)
                    VALUES ($1, $2, $3, 1,
                            CASE WHEN $4 THEN 1 ELSE 0 END,
                            CASE WHEN $4 THEN 0 ELSE 1 END,
                            $3, $4, now())
                    ON CONFLICT (domain, site_url) DO UPDATE SET
                        avg_relevance_score = (
                            umsa_core.site_memory.avg_relevance_score * umsa_core.site_memory.total_queries + $3
                        ) / (umsa_core.site_memory.total_queries + 1),
                        total_queries = umsa_core.site_memory.total_queries + 1,
                        successful_queries = umsa_core.site_memory.successful_queries
                            + CASE WHEN $4 THEN 1 ELSE 0 END,
                        failed_queries = umsa_core.site_memory.failed_queries
                            + CASE WHEN $4 THEN 0 ELSE 1 END,
                        last_score = $3,
                        is_valid = (
                            (umsa_core.site_memory.avg_relevance_score * umsa_core.site_memory.total_queries + $3)
                            / (umsa_core.site_memory.total_queries + 1)
                        ) >= 0.3,
                        updated_at = now()
                    """,
                    domain,
                    site_url,
                    score,
                    is_success,
                )
            except Exception as e:
                logger.warning("Failed to update site_memory for %s: %s", site_url, e)

            if score >= RELEVANCE_THRESHOLD:
                accepted_sites.append({
                    "site_url": site_url,
                    "relevance_score": score,
                    "status": "ACCEPTED",
                })
            else:
                rejected_sites.append({
                    "site_url": site_url,
                    "relevance_score": score,
                    "status": "REJECTED",
                    "reason": f"Below threshold ({RELEVANCE_THRESHOLD})",
                })

    logger.info(
        "Site relevance: %d accepted, %d rejected (site_memory updated)",
        len(accepted_sites), len(rejected_sites),
    )

    return {
        "accepted_sites": accepted_sites,
        "rejected_sites": rejected_sites,
    }
