"""
MCP Tool — fetch_robots_txt (Step 7a, wraps robots_fetch)

HTTP fetch with 2-retry backoff (P3.7).
Replaces direct robots_fetch_execute.
"""

import logging
import time
import asyncio
from typing import Any, Dict

import httpx

logger = logging.getLogger("umsa.tools.fetch_robots_txt")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Fetch robots.txt with retry and exponential backoff."""
    input_data = context.input_data
    site_domain = input_data.get("site_domain", "")
    domain = input_data.get("domain", "")

    if not site_domain:
        return {
            "success": False,
            "site_domain": site_domain,
            "error": "No site_domain provided",
            "raw_content": "",
            "status": "fetch_failed",
        }

    # Check cache first
    try:
        async with db_pool.acquire() as conn:
            cached = await conn.fetchrow(
                """
                SELECT status, parsed_rules, raw_content
                FROM umsa_core.robots_cache
                WHERE site_domain = $1
                  AND expires_at > now()
                ORDER BY created_at DESC
                LIMIT 1
                """,
                site_domain,
            )
            if cached:
                logger.info("Robots cache HIT for %s", site_domain)
                return {
                    "success": True,
                    "site_domain": site_domain,
                    "raw_content": cached["raw_content"] or "",
                    "status": cached["status"],
                    "cached": True,
                }
    except Exception as e:
        logger.warning("Robots cache check failed: %s", e)

    # Fetch with retry + exponential backoff
    url = f"https://{site_domain}/robots.txt"
    max_retries = 2
    last_error = None

    for attempt in range(max_retries + 1):
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(
                timeout=5.0,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; UMSA/1.0; +https://umsa.dev)"
                },
            ) as client:
                response = await client.get(url)

                latency_ms = int((time.monotonic() - start) * 1000)

                if response.status_code == 200:
                    return {
                        "success": True,
                        "site_domain": site_domain,
                        "raw_content": response.text,
                        "status": "compliant",
                        "status_code": response.status_code,
                        "latency_ms": latency_ms,
                        "cached": False,
                    }
                elif response.status_code == 404:
                    # No robots.txt = all allowed
                    return {
                        "success": True,
                        "site_domain": site_domain,
                        "raw_content": "",
                        "status": "compliant",
                        "status_code": 404,
                        "latency_ms": latency_ms,
                        "cached": False,
                    }
                elif response.status_code in (503, 429):
                    last_error = f"HTTP {response.status_code}"
                    if attempt < max_retries:
                        backoff = (2 ** attempt) * 1.0  # 1s, 2s
                        logger.warning(
                            "Robots fetch %s returned %d, retrying in %.1fs (attempt %d/%d)",
                            site_domain, response.status_code, backoff, attempt + 1, max_retries,
                        )
                        await asyncio.sleep(backoff)
                        continue
                else:
                    return {
                        "success": False,
                        "site_domain": site_domain,
                        "raw_content": "",
                        "status": "fetch_failed",
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code}",
                        "latency_ms": latency_ms,
                        "cached": False,
                    }

        except httpx.TimeoutException:
            last_error = "Timeout"
            if attempt < max_retries:
                backoff = (2 ** attempt) * 1.0
                logger.warning(
                    "Robots fetch %s timed out, retrying in %.1fs",
                    site_domain, backoff,
                )
                await asyncio.sleep(backoff)
                continue
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                backoff = (2 ** attempt) * 1.0
                await asyncio.sleep(backoff)
                continue

    return {
        "success": False,
        "site_domain": site_domain,
        "raw_content": "",
        "status": "fetch_failed",
        "error": f"All retries exhausted: {last_error}",
        "cached": False,
    }
