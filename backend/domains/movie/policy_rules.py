"""
Movie Domain — Policy Rules

Domain-specific policy enforcement for movie scraping.
This module provides the policy check function loaded by the PolicyEngine.

Site validity is determined by AI-driven DOM analysis + relevance scoring (Step 5),
NOT by a static whitelist. Any site the user provides is evaluated dynamically.
"""

from typing import Any
import logging

from mcp.server.policy_engine import PolicyResult

logger = logging.getLogger("umsa.domains.movie.policy")


# Maximum number of concurrent sites per movie request
MAX_SITES_PER_REQUEST = 5

# Minimum acceptable confidence for intent parsing
MIN_INTENT_CONFIDENCE = 0.5


async def check(context: Any) -> PolicyResult:
    """
    Movie domain policy check.
    Called by PolicyEngine.check() when domain == 'movie'.

    NOTE: Site validity is NOT enforced here. The AI relevance agent
    (Step 5: DOM analysis + AI scoring) determines whether a site
    is relevant for the user's query. This allows any user-provided
    site (e.g., BookMyShow, FilmAffinity, etc.) to be evaluated
    dynamically rather than being blocked by a static whitelist.
    """
    input_data = getattr(context, "input_data", {})
    tool_name = getattr(context, "tool_name", "")

    # ── Rule 1: Domain must be 'movie' ──
    domain = getattr(context, "domain", "")
    if domain != "movie":
        return PolicyResult(
            allowed=False,
            reason=f"Domain '{domain}' is not 'movie'",
        )

    # ── Rule 2: (REMOVED) Site whitelist enforcement ──
    # Previously this checked allowed_sites from the DB and BLOCKED any
    # site not in the whitelist. This was wrong because:
    #   - It prevented valid sites like BookMyShow from being evaluated
    #   - The AI relevance agent (Step 5) handles site validation dynamically
    #   - DOM analysis + AI scoring is the correct mechanism, not a static list

    # ── Rule 3: Confidence gate for intent results ──
    if tool_name == "relevance_agent":
        intent = input_data.get("intent", {})
        confidence = intent.get("confidence", 1.0) if isinstance(intent, dict) else 1.0
        if confidence < MIN_INTENT_CONFIDENCE:
            return PolicyResult(
                allowed=False,
                reason=f"Intent confidence {confidence} below threshold {MIN_INTENT_CONFIDENCE}",
            )

    # ── Rule 4: Limit number of candidate sites ──
    candidate_sites = input_data.get("candidate_sites", [])
    if len(candidate_sites) > MAX_SITES_PER_REQUEST:
        return PolicyResult(
            allowed=False,
            reason=(
                f"Too many candidate sites ({len(candidate_sites)}) — "
                f"max {MAX_SITES_PER_REQUEST}"
            ),
        )

    return PolicyResult(allowed=True)
