"""
MCP Client — URL Generation Agent (Tier-1)

Generates candidate URLs using SerpAPI Google Search.
The user's raw query + site:domain restriction is sent to Google,
which returns real search results as candidate URLs.

Deterministic DOM inspection + AI scoring validate candidates downstream.
Timeout: 10s (Section 12).
"""

import json
import logging
from typing import Any, Dict, List
from urllib.parse import urlparse
from uuid import UUID

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.url_generation")

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"


class URLGenerationAgent:
    """Tier-1 agent for smart URL discovery via SerpAPI Google Search."""

    TOOL_NAME = "url_agent"
    CALLER = "site_pipeline"

    @staticmethod
    async def build_input(
        intent: dict,
        site_url: str,
        domain: str,
        schema_version: str,
        request_id: UUID,
        search_templates: dict = None,
    ) -> dict:
        """Build the Tier-1 input envelope for URL generation."""
        return {
            "intent": intent,
            "site_url": site_url,
            "domain": domain,
            "schema_version": schema_version,
            "request_id": str(request_id),
        }

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        Tool implementation — registered in gateway.
        Uses SerpAPI Google Search to find candidate URLs for scraping.

        For multi-item queries (query_type: 'list'):
          → Groq AI expands the query into 3-5 search queries
          → Each query is sent to SerpAPI
          → All candidate URLs are pooled and deduplicated

        For single-item queries:
          → Original raw_query + site: restriction sent to SerpAPI (unchanged)
        """
        input_data = context.input_data
        intent = input_data["intent"]
        site_url = input_data["site_url"]
        domain = input_data["domain"]
        schema_version = input_data["schema_version"]

        # Extract site domain for site-restricted search
        site_domain = _extract_site_domain(site_url)

        # Detect multi-item query — respect orchestrator's resolved decision
        # The orchestrator checks cardinality + execution_strategy and sets
        # _is_single_entity=True when query expansion was skipped.
        # This prevents re-expanding for single movies parsed as query_type=search.
        if intent.get("_is_single_entity"):
            is_multi_item = False
        else:
            query_type = intent.get("query_type", "").lower()
            is_multi_item = query_type in ("list", "search")
            # Also check result structure hints from intent
            if not is_multi_item and intent.get("limit") and int(intent.get("limit", 0)) > 1:
                is_multi_item = True

        if is_multi_item:
            # ── Multi-item: use pre-expanded queries from orchestrator (1 Groq call) ──
            # OR fall back to calling Groq directly if not pre-expanded
            pre_expanded = intent.get("_expanded_queries")
            if pre_expanded and isinstance(pre_expanded, list) and len(pre_expanded) >= 2:
                logger.info(
                    "Multi-item: using %d pre-expanded queries from orchestrator",
                    len(pre_expanded),
                )
                expanded_queries = pre_expanded
            else:
                # Fallback: call Groq directly (should rarely happen)
                from mcp.client.agents.query_expander import expand_query_for_multi_item
                logger.info(
                    "Multi-item detected (query_type=%r) — expanding query via Groq AI",
                    query_type,
                )
                expanded_queries = await expand_query_for_multi_item(intent, site_domain)

            if not expanded_queries:
                # Fallback: use the original query
                expanded_queries = [(intent.get("raw_query") or intent.get("domain", ""))]

            # Send expanded queries to SerpAPI CONCURRENTLY (avoid timeout)
            import asyncio as _asyncio

            all_urls = []
            seen_urls = set()

            async def _fetch_one(qi: int, eq: str):
                """Fetch URLs for one expanded query."""
                search_query = f"{eq} site:{site_domain}" if site_domain else eq
                logger.info(
                    "SerpAPI multi-item query %d/%d: %r",
                    qi + 1, len(expanded_queries), search_query,
                )
                try:
                    return await _serpapi_search(search_query, num_results=5)
                except Exception as e:
                    logger.warning(
                        "SerpAPI query %d failed: %s", qi + 1, e,
                    )
                    return []

            # Run all SerpAPI calls in parallel
            results_per_query = await _asyncio.gather(
                *[_fetch_one(qi, eq) for qi, eq in enumerate(expanded_queries)]
            )

            for results in results_per_query:
                for item in results:
                    if item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        all_urls.append(item)

            if not all_urls:
                raise RuntimeError(
                    f"SerpAPI returned no results for any of {len(expanded_queries)} expanded queries"
                )

            logger.info(
                "Multi-item SerpAPI: %d unique URLs from %d queries",
                len(all_urls), len(expanded_queries),
            )

            # Build output envelope
            url_entries = []
            for i, item in enumerate(all_urls):
                url_entries.append({
                    "url": item["url"],
                    "pattern_type": _infer_pattern_type(item["url"]),
                    "confidence": round(max(0.3, 1.0 - i * 0.08), 2),
                    "reasoning": item.get("snippet", "Google search result"),
                })

            output = {
                "task": "url_reasoning",
                "domain": domain,
                "schema_version": schema_version,
                "result": {"urls": url_entries},
                "confidence": url_entries[0]["confidence"] if url_entries else 0.0,
                "reasoning_trace": (
                    f"SerpAPI Multi-Item ({len(expanded_queries)} queries, "
                    f"{len(all_urls)} unique URLs): "
                    + " | ".join(expanded_queries)
                ),
            }
        else:
            # ── Single-item: existing flow (unchanged) ──
            query = _build_search_query(intent, site_domain)

            logger.info(
                "SerpAPI URL generation: query=%r  site=%s",
                query, site_domain,
            )

            try:
                urls = await _serpapi_search(query, num_results=5)
            except Exception as e:
                raise RuntimeError(f"SerpAPI URL generation failed: {e}")

            if not urls:
                raise RuntimeError(
                    f"SerpAPI returned no results for query: {query}"
                )

            url_entries = []
            for i, item in enumerate(urls):
                url_entries.append({
                    "url": item["url"],
                    "pattern_type": _infer_pattern_type(item["url"]),
                    "confidence": round(max(0.4, 1.0 - i * 0.15), 2),
                    "reasoning": item.get("snippet", "Google search result"),
                })

            output = {
                "task": "url_reasoning",
                "domain": domain,
                "schema_version": schema_version,
                "result": {"urls": url_entries},
                "confidence": url_entries[0]["confidence"] if url_entries else 0.0,
                "reasoning_trace": f"SerpAPI Google Search: {query}",
            }

        # Validate envelope
        _validate_url_output(output, domain, schema_version)

        # Apply heuristic scoring
        _apply_heuristic_scoring(output)

        return output


# ────────────────────────────────────────────
# Helper: Build search query from intent
# ────────────────────────────────────────────

def _build_search_query(intent: dict, site_domain: str) -> str:
    """
    Build a Google search query from the user's intent.

    Strategy-aware + field-aware query construction:
      - entity_detail_lookup WITH requested_fields:
          Use TITLE + field keywords so Google finds the specific sub-page.
          e.g. "Dhurandhar box office collection site:filmibeat.com"
               → finds /dhurandhar/box-office.html directly
      - entity_detail_lookup WITHOUT requested_fields:
          Use TITLE + "movie" for the general detail page.
          e.g. "Dhurandhar movie site:imdb.com"
      - Other strategies: Use raw_query for broader searches.

    Examples:
        - "Dhurandhar box office collection site:filmibeat.com"  (detail + fields)
        - "Dhurandhar movie site:imdb.com"                       (detail, no fields)
        - "top hindi movies 2024 site:rottentomatoes.com"        (list/search)
    """
    strategy = intent.get("_execution_strategy", {}).get("strategy", "")
    is_detail_lookup = strategy in ("entity_detail_lookup", "detail_lookup")
    requested_fields = intent.get("requested_fields", [])

    if is_detail_lookup and intent.get("title"):
        # For detail lookups, search for the TITLE, not the raw query.
        parts = [intent["title"]]
        if intent.get("year"):
            parts.append(str(intent["year"]))

        if requested_fields:
            # Convert schema field names to Google-friendly search keywords
            field_keywords = _fields_to_search_keywords(requested_fields)
            if field_keywords:
                parts.append(field_keywords)
            else:
                parts.append("movie")
        else:
            # No specific fields requested — general detail page
            parts.append("movie")

        query = " ".join(parts)
    else:
        # For search/list/discovery: use the full raw query
        query = (intent.get("raw_query") or "").strip()

        # Fall back to title + year if raw_query is missing
        if not query:
            parts = []
            if intent.get("title"):
                parts.append(intent["title"])
            if intent.get("year"):
                parts.append(str(intent["year"]))
            query = " ".join(parts)

        # Generic fallback (should rarely happen)
        if not query:
            query = intent.get("domain", "movies")

    # Site restriction — the only thing we add
    if site_domain:
        query += f" site:{site_domain}"

    return query


# Maps extraction schema field names → Google-friendly search terms
# Keep this generic and compact; only add terms where the schema name
# differs significantly from what a user would type into Google.
_FIELD_SEARCH_KEYWORDS = {
    "box_office": "box office collection",
    "budget": "budget",
    "cast": "cast",
    "director": "director",
    "rating": "rating reviews",
    "synopsis": "story synopsis",
    "trailer_url": "trailer",
    "release_date": "release date",
    "runtime_minutes": "runtime duration",
    "critic_score": "reviews critic score",
    "audience_score": "audience score",
    "certification": "certification age rating",
    "poster_url": "poster",
    "genres": "genre",
    "language": "language",
}


def _fields_to_search_keywords(requested_fields: list) -> str:
    """
    Convert a list of extraction schema field names into a compact
    search-friendly string for Google.

    Examples:
        ["box_office"]             → "box office collection"
        ["cast", "director"]       → "cast director"
        ["budget", "box_office"]   → "budget box office collection"
    """
    if not requested_fields:
        return ""

    keywords = []
    seen = set()
    for field in requested_fields[:3]:  # Cap at 3 fields to keep query short
        kw = _FIELD_SEARCH_KEYWORDS.get(field, field.replace("_", " "))
        if kw and kw not in seen:
            keywords.append(kw)
            seen.add(kw)

    return " ".join(keywords)


# ────────────────────────────────────────────
# Helper: Call SerpAPI
# ────────────────────────────────────────────

async def _serpapi_search(
    query: str, num_results: int = 5,
) -> List[Dict[str, str]]:
    """
    Call SerpAPI Google Search and return a list of
    {"url": ..., "title": ..., "snippet": ...} dicts.
    """
    params = {
        "q": query,
        "api_key": settings.serpapi_key,
        "engine": "google",
        "num": str(num_results),
        "no_cache": "true",
    }

    async with httpx.AsyncClient(timeout=settings.timeout_url_agent) as client:
        response = await client.get(SERPAPI_ENDPOINT, params=params)
        response.raise_for_status()
        data = response.json()

    # Check for SerpAPI-level errors
    if "error" in data:
        raise RuntimeError(f"SerpAPI error: {data['error']}")

    organic = data.get("organic_results", [])
    results = []
    for item in organic[:num_results]:
        link = item.get("link", "")
        if link:
            results.append({
                "url": link,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
            })

    return results


# ────────────────────────────────────────────
# Helper: Infer pattern type from URL
# ────────────────────────────────────────────

def _infer_pattern_type(url: str) -> str:
    """Classify a URL as direct, search, browse, or chart based on its path."""
    url_lower = url.lower()

    search_signals = ("/search", "/find", "?q=", "?query=", "/results")
    browse_signals = ("/genre", "/browse", "/discover", "/category", "/list")
    chart_signals = ("/top", "/chart", "/best", "/popular", "/trending")

    if any(s in url_lower for s in search_signals):
        return "search"
    if any(s in url_lower for s in browse_signals):
        return "browse"
    if any(s in url_lower for s in chart_signals):
        return "chart"

    return "direct"


# ────────────────────────────────────────────
# Helper: Extract site domain
# ────────────────────────────────────────────

def _extract_site_domain(site_url: str) -> str:
    """Extract the domain from a site URL."""
    if "://" not in site_url:
        site_url = f"https://{site_url}"
    return urlparse(site_url).netloc.replace("www.", "")


# ────────────────────────────────────────────
# Validation & heuristic scoring (unchanged)
# ────────────────────────────────────────────

def _validate_url_output(
    output: dict, expected_domain: str, expected_schema_version: str,
) -> None:
    """Validate Tier-1 URL generation output envelope."""
    required = ["task", "domain", "schema_version", "result", "confidence"]
    for key in required:
        if key not in output:
            raise RuntimeError(f"URL output missing key: '{key}'")

    result = output.get("result", {})
    if "urls" not in result:
        raise RuntimeError("URL output missing 'result.urls'")


def _apply_heuristic_scoring(output: dict) -> None:
    """
    Apply heuristic scoring adjustments (Section 19, Step 9):
    - Depth penalty
    - Direct match bonus
    """
    urls = output.get("result", {}).get("urls", [])
    for url_entry in urls:
        url = url_entry.get("url", "")
        score = url_entry.get("confidence", 0.5)

        # Depth penalty: deeper URLs get penalized
        depth = url.count("/") - 2  # subtract protocol slashes
        depth_penalty = max(0, (depth - 3) * 0.05)
        score -= depth_penalty

        # Direct match bonus
        if url_entry.get("pattern_type") == "direct":
            score += 0.1

        url_entry["adjusted_score"] = max(0.0, min(1.0, score))





# ────────────────────────────────────────────
# Iterative single-URL generation (SerpAPI)
# ────────────────────────────────────────────

async def generate_single_url(
    intent: dict,
    site_url: str,
    domain: str,
    schema_version: str,
    previous_attempts: list = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Generate a single candidate URL using SerpAPI for iterative discovery.

    For multi-item queries, uses Groq AI query expansion to find better
    list/collection pages. For single-item, uses the original query.

    Args:
        previous_attempts: List of {"url": str, "confidence": int, "summary": str}
            from previous validation failures so we can skip them.

    Returns:
        {"url": str, "reasoning": str, "pattern_type": str}
    """
    site_domain = _extract_site_domain(site_url)

    # Collect previous URLs to skip
    previous_urls = set()
    if previous_attempts:
        previous_urls = {a.get("url", "") for a in previous_attempts if a.get("url")}

    # Detect multi-item query semantically
    query_type = intent.get("query_type", "").lower()
    is_multi_item = query_type in ("list", "search")
    if not is_multi_item and intent.get("limit") and int(intent.get("limit", 0)) > 1:
        is_multi_item = True

    if is_multi_item:
        # Multi-item: use pre-expanded queries from orchestrator if available
        pre_expanded = intent.get("_expanded_queries")
        if pre_expanded and isinstance(pre_expanded, list) and len(pre_expanded) >= 2:
            expanded_queries = pre_expanded
        else:
            from mcp.client.agents.query_expander import expand_query_for_multi_item
            try:
                expanded_queries = await expand_query_for_multi_item(intent, site_domain)
            except Exception:
                expanded_queries = []

        if not expanded_queries:
            expanded_queries = [(intent.get("raw_query") or "")]

        # Send each query to SerpAPI and find the first non-attempted URL
        for eq in expanded_queries:
            search_query = f"{eq} site:{site_domain}" if site_domain else eq
            num_to_fetch = 5 + len(previous_urls)

            try:
                results = await _serpapi_search(search_query, num_results=min(num_to_fetch, 10))
            except Exception as e:
                logger.warning("SerpAPI expanded query failed: %s", e)
                continue

            for item in results:
                if item["url"] not in previous_urls:
                    return {
                        "url": item["url"],
                        "reasoning": f"SerpAPI expanded result: {item.get('snippet', item.get('title', ''))}",
                        "pattern_type": _infer_pattern_type(item["url"]),
                    }

        return {
            "url": "",
            "reasoning": "All expanded SerpAPI results already attempted",
            "pattern_type": "exhausted",
        }

    else:
        # Single-item: existing flow
        query = _build_search_query(intent, site_domain)

        num_to_fetch = 5 + len(previous_urls)

        try:
            results = await _serpapi_search(query, num_results=min(num_to_fetch, 10))
        except Exception as e:
            logger.warning("SerpAPI single URL generation failed: %s", e)
            return {"url": "", "reasoning": f"SerpAPI error: {e}", "pattern_type": "error"}

        # Pick the first result not already attempted
        for item in results:
            if item["url"] not in previous_urls:
                return {
                    "url": item["url"],
                    "reasoning": f"SerpAPI result: {item.get('snippet', item.get('title', ''))}",
                    "pattern_type": _infer_pattern_type(item["url"]),
                }

        # All results were previously tried
        return {
            "url": "",
            "reasoning": "All SerpAPI results already attempted",
            "pattern_type": "exhausted",
        }

