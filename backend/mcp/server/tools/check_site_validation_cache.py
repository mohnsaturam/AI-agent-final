"""
MCP Tool — check_site_validation_cache (Step 4b)

Two-tier site validation cache:

Tier 1 (exact match): Check site_relevance with exact intent_hash.
   → Reuses scores from the exact same query.

Tier 2 (site memory): Check site_memory for cross-query site knowledge.
   → Reuses general site validity across ALL queries in the same domain.
   → Example: imdb.com scored 0.9 avg across 59 queries → skip AI scoring.

Sites found in either tier skip the AI relevance call.
Only truly unknown sites need AI scoring.
"""

import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.check_site_validation_cache")

# Minimum relevance score threshold for a cached site to be valid
RELEVANCE_THRESHOLD = 0.3

# Minimum number of past queries before trusting site_memory
MIN_QUERIES_FOR_MEMORY = 2


async def execute(context, db_pool) -> Dict[str, Any]:
    """Two-tier site validation cache check."""
    input_data = context.input_data
    domain = input_data.get("domain", "")
    intent_hash = input_data.get("intent_hash", "")
    schema_version = input_data.get("schema_version", "")
    sites = input_data.get("sites", [])

    cached_sites = []
    cached_site_urls = []
    memory_sites = []
    memory_site_urls = []
    uncached_sites = []

    async with db_pool.acquire() as conn:
        for site in sites:
            # ── Tier 1: Exact intent_hash match (per-query cache) ──
            row = await conn.fetchrow(
                """
                SELECT site_url, relevance_score, reasoning
                FROM umsa_core.site_relevance
                WHERE domain = $1
                  AND intent_hash = $2
                  AND schema_version = $3
                  AND site_url = $4
                  AND status = 'valid'
                  AND expires_at > now()
                LIMIT 1
                """,
                domain,
                intent_hash,
                schema_version,
                site,
            )

            if row and float(row["relevance_score"]) >= RELEVANCE_THRESHOLD:
                # Tier 1 hit — exact query match
                cached_sites.append({
                    "site_url": row["site_url"],
                    "relevance_score": float(row["relevance_score"]),
                    "reasoning": row["reasoning"],
                    "cache_hit": True,
                    "cache_tier": "exact_query",
                })
                cached_site_urls.append(row["site_url"])
                logger.info(
                    "Tier 1 HIT: %s (score=%.2f, exact intent_hash match)",
                    site, float(row["relevance_score"]),
                )
                continue

            # ── Tier 2: Cross-query site memory ──
            memory_row = await conn.fetchrow(
                """
                SELECT site_url, avg_relevance_score, total_queries,
                       successful_queries, is_valid, last_score
                FROM umsa_core.site_memory
                WHERE domain = $1
                  AND site_url = $2
                  AND is_valid = true
                  AND total_queries >= $3
                LIMIT 1
                """,
                domain,
                site,
                MIN_QUERIES_FOR_MEMORY,
            )

            if memory_row:
                # Tier 2 hit — site is known-good across multiple queries
                memory_sites.append({
                    "site_url": memory_row["site_url"],
                    "relevance_score": float(memory_row["avg_relevance_score"]),
                    "reasoning": (
                        f"Site memory: avg {memory_row['avg_relevance_score']:.2f} "
                        f"across {memory_row['total_queries']} queries "
                        f"({memory_row['successful_queries']} successful)"
                    ),
                    "cache_hit": True,
                    "cache_tier": "site_memory",
                    "total_queries": memory_row["total_queries"],
                    "successful_queries": memory_row["successful_queries"],
                })
                memory_site_urls.append(memory_row["site_url"])
                logger.info(
                    "Tier 2 MEMORY HIT: %s (avg=%.2f, %d queries, %d successful)",
                    site, float(memory_row["avg_relevance_score"]),
                    memory_row["total_queries"], memory_row["successful_queries"],
                )
                continue

            # No cache or memory hit — needs AI scoring
            uncached_sites.append(site)

            if row:
                # Had Tier 1 cache but below threshold
                logger.info(
                    "Site %s has cached score %.2f (below threshold %.2f) — treating as uncached",
                    site, float(row["relevance_score"]), RELEVANCE_THRESHOLD,
                )
            else:
                # Check if memory exists but rejected (is_valid=false or too few queries)
                rejected_memory = await conn.fetchrow(
                    """
                    SELECT site_url, avg_relevance_score, total_queries, is_valid
                    FROM umsa_core.site_memory
                    WHERE domain = $1 AND site_url = $2
                    LIMIT 1
                    """,
                    domain,
                    site,
                )
                if rejected_memory:
                    if not rejected_memory["is_valid"]:
                        logger.info(
                            "Site %s has memory but is_valid=false (avg=%.2f) — needs fresh AI scoring",
                            site, float(rejected_memory["avg_relevance_score"]),
                        )
                    elif rejected_memory["total_queries"] < MIN_QUERIES_FOR_MEMORY:
                        logger.info(
                            "Site %s has memory but only %d queries (need %d) — needs AI scoring",
                            site, rejected_memory["total_queries"], MIN_QUERIES_FOR_MEMORY,
                        )
                else:
                    logger.info("Site %s: no cache, no memory — needs AI scoring", site)

    # Combine Tier 1 + Tier 2 hits as "cached"
    all_cached_sites = cached_sites + memory_sites
    all_cached_urls = cached_site_urls + memory_site_urls
    all_cached = len(uncached_sites) == 0 and len(all_cached_sites) > 0

    logger.info(
        "Site validation cache: %d Tier1 hits, %d Tier2 memory hits, %d misses | all_cached=%s",
        len(cached_sites), len(memory_sites), len(uncached_sites), all_cached,
    )

    return {
        "cached_sites": all_cached_sites,
        "cached_site_urls": all_cached_urls,
        "uncached_sites": uncached_sites,
        "all_cached": all_cached,
        "tier1_hits": len(cached_sites),
        "tier2_memory_hits": len(memory_sites),
    }
