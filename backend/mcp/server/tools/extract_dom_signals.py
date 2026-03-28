"""
MCP Tool — extract_dom_signals (Step 5-DET) — REVISED

For candidate sites: fetch homepage DOM.
Extract structure-only signals (headings, links, schema.org).
Now includes QUERY-AWARE signals using intent context.
Deterministic validation — no AI.
"""

import logging
import re
from typing import Any, Dict
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("umsa.tools.extract_dom_signals")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Extract deterministic DOM signals from a site's homepage, including query awareness."""
    input_data = context.input_data
    site_url = input_data.get("site_url", "")
    domain = input_data.get("domain", "")
    intent = input_data.get("intent", {})

    # Normalize URL
    if not site_url.startswith("http"):
        site_url = f"https://{site_url}"

    signals = {
        "site_url": site_url,
        "accessible": False,
        "has_structured_data": False,
        "has_search": False,
        "has_domain_content": False,
        "headings": [],
        "schema_org_types": [],
        "link_patterns": [],
        "status_code": 0,
        "query_keyword_matches": 0,
        "query_relevance_hint": 0.0,
    }

    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; UMSA/1.0; +https://umsa.dev)"
            },
        ) as client:
            response = await client.get(site_url)
            signals["status_code"] = response.status_code
            signals["accessible"] = 200 <= response.status_code < 400

            if signals["accessible"]:
                html = response.text[:50000]  # Limit parsing

                # Extract headings
                heading_matches = re.findall(r"<h[1-3][^>]*>(.*?)</h[1-3]>", html, re.IGNORECASE | re.DOTALL)
                signals["headings"] = [re.sub(r"<[^>]+>", "", h).strip() for h in heading_matches[:20]]

                # Check for JSON-LD structured data
                jsonld_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>', html, re.IGNORECASE)
                signals["has_structured_data"] = len(jsonld_matches) > 0

                # Extract schema.org types
                schema_types = re.findall(r'"@type"\s*:\s*"([^"]+)"', html)
                signals["schema_org_types"] = list(set(schema_types))

                # Check for search functionality
                signals["has_search"] = bool(
                    re.search(r'(search|find|lookup)', html, re.IGNORECASE)
                    and re.search(r'<(input|form)', html, re.IGNORECASE)
                )

                # ── Search endpoint capability detection ──
                # Detect actual search endpoint links and form inputs
                search_link_patterns = re.findall(
                    r'href="(/(?:search|find|query|discover|browse)[^"]*)"',
                    html, re.IGNORECASE,
                )
                search_form_inputs = re.findall(
                    r'<input[^>]*name=["\']?(q|query|search|s|keyword)["\']?',
                    html, re.IGNORECASE,
                )
                search_forms = re.findall(
                    r'<form[^>]*action="([^"]*(?:search|find|query)[^"]*)"',
                    html, re.IGNORECASE,
                )

                endpoint_patterns = list(set(
                    search_link_patterns[:5] + search_forms[:3]
                ))
                signals["has_search_endpoint"] = bool(
                    endpoint_patterns or search_form_inputs
                )
                signals["search_endpoint_patterns"] = endpoint_patterns
                signals["search_input_names"] = list(set(
                    m.lower() for m in search_form_inputs[:5]
                ))

                # Domain content detection based on schema.org types found
                signals["has_domain_content"] = len(signals["schema_org_types"]) > 0

                # Extract link patterns
                links = re.findall(r'href="(/[^"]{3,60})"', html)
                unique_patterns = set()
                for link in links[:100]:
                    parts = link.strip("/").split("/")
                    if len(parts) >= 1:
                        unique_patterns.add(f"/{parts[0]}/...")
                signals["link_patterns"] = list(unique_patterns)[:20]

                # ── Query-aware signals ──
                keyword_count, relevance_hint = _compute_query_relevance(
                    html, intent, signals["schema_org_types"]
                )
                signals["query_keyword_matches"] = keyword_count
                signals["query_relevance_hint"] = relevance_hint

    except httpx.TimeoutException:
        signals["error"] = "Timeout fetching homepage"
    except Exception as e:
        signals["error"] = str(e)

    return signals


def _compute_query_relevance(
    html: str, intent: dict, schema_types: list
) -> tuple:
    """
    Compute query relevance from intent keywords found in homepage HTML.
    Returns (keyword_match_count, relevance_hint_score).
    """
    if not intent:
        return 0, 0.0

    html_lower = html.lower()
    keywords = []

    # Extract keywords from intent fields
    if intent.get("title"):
        keywords.extend(intent["title"].lower().split())
    if intent.get("language"):
        keywords.append(intent["language"].lower())
    if intent.get("genre"):
        keywords.append(intent["genre"].lower())
    if intent.get("primary_goal"):
        keywords.append(intent["primary_goal"].lower())

    # Remove very short/common words
    keywords = [k for k in keywords if len(k) > 2 and k not in ("the", "and", "for", "top")]

    if not keywords:
        return 0, 0.0

    # Count keyword matches
    match_count = sum(1 for k in keywords if k in html_lower)

    # Relevance hint: keyword fraction + schema type bonus
    keyword_score = match_count / len(keywords) if keywords else 0.0

    # Schema type bonus: if domain-relevant type found (loaded from intent config or defaults)
    target_types = set(intent.get("target_schema_types", ["Movie", "Film", "CreativeWork", "TVSeries"]))
    schema_bonus = 0.2 if any(t in target_types for t in schema_types) else 0.0

    relevance_hint = min(1.0, keyword_score * 0.8 + schema_bonus)

    return match_count, round(relevance_hint, 3)
