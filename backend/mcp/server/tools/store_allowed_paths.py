"""
MCP Tool — store_allowed_paths (Step 7c) — REVISED

Persist parsed robots.txt rules + allowed paths.
Detects path changes: if a previously-allowed path is now disallowed,
marks it as "unstable" and invalidates cached URL patterns using that path.
IF fully blocked → SKIP pipeline.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.store_allowed_paths")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Persist robots rules, detect path changes, mark unstable paths."""
    input_data = context.input_data
    domain = input_data.get("domain", "")
    site_domain = input_data.get("site_domain", "")
    rules = input_data.get("rules", {})
    raw_content = input_data.get("raw_content", "")
    status = input_data.get("status", "compliant")
    schema_version = input_data.get("schema_version", "")
    ttl_seconds = input_data.get("ttl_seconds", 86400)
    fetch_latency_ms = input_data.get("fetch_latency_ms", 0)

    # Map status to robots_status enum
    robots_status = "compliant"
    if status == "blocked":
        robots_status = "blocked"
    elif status == "fetch_failed":
        robots_status = "fetch_failed"

    # Extract current allowed/disallowed paths
    wildcard_rules = rules.get("user_agents", {}).get("*", {})
    current_allowed = wildcard_rules.get("allow", [])
    current_disallowed = wildcard_rules.get("disallow", [])
    fully_blocked = "/" in current_disallowed and not current_allowed

    # ═══ Step 1: Load PREVIOUS allowed paths from memory (DB) ═══
    previous_allowed = []
    try:
        async with db_pool.acquire() as conn:
            prev_row = await conn.fetchrow(
                """
                SELECT parsed_rules FROM umsa_core.robots_cache
                WHERE site_domain = $1
                ORDER BY updated_at DESC LIMIT 1
                """,
                site_domain,
            )
        if prev_row and prev_row["parsed_rules"]:
            prev_rules = prev_row["parsed_rules"]
            if isinstance(prev_rules, str):
                prev_rules = json.loads(prev_rules)
            prev_wildcard = prev_rules.get("user_agents", {}).get("*", {})
            previous_allowed = prev_wildcard.get("allow", [])
    except Exception as e:
        logger.warning("Failed to load previous robots rules for %s: %s", site_domain, e)

    # ═══ Step 2: Detect paths that CHANGED from allowed → disallowed ═══
    newly_disallowed = []
    for path in previous_allowed:
        # Path was allowed before but is now in the disallowed list
        # or no longer in the allowed list
        is_now_blocked = False
        for dp in current_disallowed:
            if dp and path.startswith(dp):
                is_now_blocked = True
                break
        if is_now_blocked or (path not in current_allowed and current_disallowed):
            newly_disallowed.append(path)

    # ═══ Step 3: Mark changed paths as "unstable" ═══
    invalidated_url_count = 0
    if newly_disallowed:
        logger.warning(
            "Paths changed from ALLOWED → DISALLOWED for %s: %s",
            site_domain, newly_disallowed,
        )
        try:
            async with db_pool.acquire() as conn:
                # Invalidate cached URL patterns that use these paths
                for path in newly_disallowed:
                    if not path:
                        continue
                    result = await conn.execute(
                        """
                        UPDATE umsa_core.url_patterns
                        SET status = 'invalidated',
                            metadata = metadata || $1::jsonb
                        WHERE site_domain = $2
                          AND pattern LIKE $3
                          AND status = 'valid'
                        """,
                        json.dumps({
                            "invalidation_reason": "robots_path_now_disallowed",
                            "disallowed_path": path,
                            "detected_at": datetime.now().isoformat(),
                        }),
                        site_domain,
                        f"%{path}%",
                    )
                    # result is a string like 'UPDATE N'
                    try:
                        count = int(result.split()[-1])
                        invalidated_url_count += count
                    except (ValueError, IndexError):
                        pass

                # Update domain_health to record path instability
                await conn.execute(
                    """
                    INSERT INTO umsa_core.domain_health
                        (domain, site_domain, status, metadata)
                    VALUES ($1, $2, 'degraded', $3::jsonb)
                    ON CONFLICT (domain, site_domain) DO UPDATE SET
                        metadata = umsa_core.domain_health.metadata || $3::jsonb,
                        updated_at = now()
                    """,
                    domain,
                    site_domain,
                    json.dumps({
                        "paths_became_unstable": newly_disallowed,
                        "detected_at": datetime.now().isoformat(),
                        "invalidated_url_count": invalidated_url_count,
                    }),
                )
        except Exception as e:
            logger.warning(
                "Failed to mark unstable paths for %s: %s", site_domain, e
            )

    # ═══ Step 4: Store CURRENT allowed paths in memory (robots_cache) ═══
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO umsa_core.robots_cache
                    (domain, site_domain, status, raw_content,
                     parsed_rules, schema_version, expires_at, fetch_latency_ms)
                VALUES ($1, $2, $3::umsa_core.robots_status, $4,
                        $5::jsonb, $6, now() + make_interval(secs => $7), $8)
                ON CONFLICT (site_domain) WHERE status != 'fetch_failed'
                DO UPDATE SET
                    status = $3::umsa_core.robots_status,
                    raw_content = $4,
                    parsed_rules = $5::jsonb,
                    expires_at = now() + make_interval(secs => $7),
                    fetch_latency_ms = $8,
                    updated_at = now()
                """,
                domain,
                site_domain,
                robots_status,
                raw_content,
                json.dumps(rules, default=str),
                schema_version,
                ttl_seconds,
                fetch_latency_ms,
            )
    except Exception as e:
        logger.warning("Failed to persist robots cache for %s: %s", site_domain, e)

    return {
        "site_domain": site_domain,
        "status": robots_status,
        "fully_blocked": fully_blocked,
        "allowed_paths": current_allowed,
        "disallowed_paths": current_disallowed,
        "skip_pipeline": fully_blocked,
        "paths_changed": len(newly_disallowed) > 0,
        "newly_disallowed": newly_disallowed,
        "invalidated_url_count": invalidated_url_count,
    }
