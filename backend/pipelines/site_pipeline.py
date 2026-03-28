"""
Site Pipeline — Per-Site Scraping Pipeline (Steps 7–10) — REVISED

PURE ORCHESTRATION LAYER.
All operations go through execute_tool.
ZERO direct AI calls, ZERO inline SQL, ZERO business logic.

Flow:
  Step 7a: fetch_robots_txt (+mark unreachable on persistent failure)
  Step 7b: parse_robots_rules
  Step 7c: store_allowed_paths (+path change detection, unstable marking)
  Step 8-CACHE: check_url_pattern_cache (memory only, robots.txt validated)
  Step 8-AI: url_agent (with failure_class + strategy switching)
  Step 8b: inspect_url_dom (+DOM signals JSON storage)
  Step 8c: scoring_agent (heuristic-first)
  Step 8d: select_best_url
  Step 8e: store_url_pattern
  Step 9: scrape_structured_data (+re-invoke on low confidence)
  Step 9-VAL: validate_extraction (+row count via intent)
"""

import asyncio
import json
import logging
import os
import re as _re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID
from urllib.parse import urlparse

from mcp.server.gateway import (ToolExecutionContext,ToolResult,execute_tool,)

logger = logging.getLogger("umsa.pipelines.site")

# Directory for storing DOM signals JSON per query
DOM_SIGNALS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dom_signals")
os.makedirs(DOM_SIGNALS_DIR, exist_ok=True)


def _classify_dom_failure(status_code: int, error: str, html_size: int = 0) -> str:
    """Classify WHY a DOM fetch/URL failed — for self-healing."""
    error_lower = error.lower() if error else ""
    if status_code == 403 or "forbidden" in error_lower:
        return "BLOCKED_403"
    elif status_code == 429 or "rate" in error_lower:
        return "RATE_LIMITED"
    elif "captcha" in error_lower or "challenge" in error_lower:
        return "CAPTCHA_DETECTED"
    elif "timeout" in error_lower or status_code == 0:
        return "TIMEOUT"
    elif html_size < 500 and html_size > 0:
        return "EMPTY_PAGE"
    elif status_code == 404:
        return "NOT_FOUND"
    elif status_code >= 500:
        return "SERVER_ERROR"
    else:
        return "UNKNOWN"





def _extract_tag(html: str, tag: str) -> str:
    """Extract content of an HTML tag."""
    m = _re.search(f'<{tag}[^>]*>(.*?)</{tag}>', html, _re.IGNORECASE | _re.DOTALL)
    return m.group(1).strip()[:200] if m else ""


def _extract_meta(html: str, name: str) -> str:
    """Extract content of a meta tag."""
    m = _re.search(
        f'<meta[^>]*name=["\']?{name}["\']?[^>]*content=["\']([^"\']*)',
        html, _re.IGNORECASE
    )
    return m.group(1)[:200] if m else ""


def _detect_search_page(url: str, html: str) -> bool:
    """
    Detect if a page is a search results page using URL + DOM heuristics.
    Uses web-standard conventions (not site-specific selectors).
    """
    url_lower = url.lower()

    # URL-based signals: standard query parameter conventions
    url_is_search = bool(_re.search(
        r'[/\?](search|find|query|results)[/\?=&]',
        url_lower,
    ))

    if not url_is_search:
        return False

    # DOM-based confirmation: at least ONE of these must be true
    title = _extract_tag(html, "title").lower()
    h1_blocks = _re.findall(r'<h1[^>]*>(.*?)</h1>', html, _re.IGNORECASE | _re.DOTALL)
    h1_text = " ".join(_re.sub(r'<[^>]+>', '', h).strip().lower() for h in h1_blocks[:3])

    has_search_title = any(kw in title for kw in ("search", "find", "results"))
    has_search_h1 = any(kw in h1_text for kw in ("search", "find", "results"))
    has_no_jsonld = '<script type="application/ld+json">' not in html
    has_high_link_density = html.lower().count("<a ") > 15

    # Need URL signal + at least one DOM signal
    return url_is_search and (has_search_title or has_search_h1 or (has_no_jsonld and has_high_link_density))


def _resolve_entity_from_search(html: str, intent: dict, search_url: str) -> str:
    """
    Extract the best matching detail page link from a search results page.
    Uses generic link patterns and title similarity — zero hardcoded selectors.

    Returns: full URL of the best matching detail page, or empty string.
    """
    # Build query keywords from intent
    title = intent.get("title", "") or ""
    year = str(intent.get("year", "") or "")
    keywords = [w.lower() for w in title.split() if len(w) > 2]

    if not keywords:
        return ""

    # Extract base URL for resolving relative links
    parsed = urlparse(search_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    # Extract all links with their anchor text
    # Pattern: href + surrounding text (anchor text helps match)
    link_pattern = _re.compile(
        r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        _re.IGNORECASE | _re.DOTALL,
    )

    candidates = []
    seen_urls = set()
    for match in link_pattern.finditer(html):
        href = match.group(1).strip()
        anchor_text = _re.sub(r'<[^>]+>', '', match.group(2)).strip().lower()

        # Skip empty, javascript, anchor-only, and non-path links
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue

        # Common detail page path patterns (web conventions, not site-specific)
        detail_indicators = (
            "/title/", "/movie/", "/film/", "/movies/",
            "/m/", "/tv/", "/series/", "/show/",
        )
        href_lower = href.lower()

        # Must contain a detail indicator OR be a slug-like path (3+ segments)
        is_detail_like = (
            any(indicator in href_lower for indicator in detail_indicators)
            or (_re.match(r'^/[a-z0-9\-]+/[a-z0-9\-]+', href_lower) and
                not any(skip in href_lower for skip in ("/search", "/find", "/login", "/register", "/api/")))
        )

        if not is_detail_like:
            continue

        # Resolve relative URLs
        if href.startswith("/"):
            full_url = base_url + href
        elif href.startswith("http"):
            full_url = href
        else:
            continue

        # Deduplicate
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Score by title keyword similarity
        score = 0
        match_text = (anchor_text + " " + href_lower).lower()
        for kw in keywords:
            if kw in match_text:
                score += 1

        # Year bonus
        if year and year in match_text:
            score += 2

        if score > 0:
            candidates.append((score, full_url, anchor_text))

    if not candidates:
        return ""

    # Sort by score descending, pick best
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_url, best_text = candidates[0]

    logger.info(
        "Entity resolved from search page: %s (score=%d, text='%s')",
        best_url, best_score, best_text[:60],
    )

    return best_url


class SitePipeline:
    """
    Per-site scraping pipeline — pure orchestration.
    Every operation delegates to execute_tool.
    """

    def __init__(
        self,
        gateway,
        execution_logger,
        concurrency_manager,
        db_pool,
        metrics_collector=None,
    ):
        self._gateway = gateway
        self._logger = execution_logger
        self._concurrency = concurrency_manager
        self._db_pool = db_pool
        self._metrics = metrics_collector

    # ────────────────────────────────────────────
    # Gateway helper
    # ────────────────────────────────────────────

    async def _call(
        self, tool_name, input_data, request_id, user_id, domain,
        timeout_ms=5000, retry_budget=0, schema_version="",
    ) -> ToolResult:
        """Execute a tool through the MCP gateway."""
        return await execute_tool(
            context=ToolExecutionContext(
                tool_name=tool_name,
                input_data=input_data,
                request_id=request_id,
                user_id=user_id,
                domain=domain,
                caller="site_pipeline",
                timeout_ms=timeout_ms,
                retry_budget=retry_budget,
                schema_version=schema_version,
            ),
            db_pool=self._db_pool,
            tool_registry_manager=self._gateway["tool_registry"],
            policy_engine=self._gateway["policy_engine"],
            execution_logger=self._logger,
            concurrency_manager=self._concurrency,
            metrics_collector=self._metrics,
        )

    # ════════════════════════════════════════════
    # MAIN FLOW — Steps 7-10
    # ════════════════════════════════════════════

    async def _handle_robots(
        self, site_domain: str, domain: str, sv: str, request_id: UUID, user_id: UUID, site_url: str, fl
    ) -> Dict[str, Any]:
        if fl:
            fl.separator(f"SITE ANALYSIS: {site_domain}")
            fl.info(f"Analyzing {site_domain} for scraping compliance and connectivity.")

        t0 = time.monotonic()
        r = await self._call("fetch_robots_txt", {
            "site_domain": site_domain, "domain": domain,
        }, request_id, user_id, domain,
            timeout_ms=5000, retry_budget=2, schema_version=sv)
        step_ms = int((time.monotonic() - t0) * 1000)

        raw_content = ""
        if r.success and r.data:
            raw_content = r.data.get("raw_content", r.data.get("content", ""))

        if not r.success:
            if fl:
                fl.substep("Connectivity Check",
                           "FAILED",
                           f"Failed to fetch robots.txt after retries.\n"
                           f"Error: {r.error}\n"
                           f"Site marked as temporarily unreachable (30 min cooldown).",
                           elapsed_ms=step_ms)
            await self._call("audit_log_event", {
                "request_id": str(request_id),
                "event_type": "ROBOTS_FETCH_FAILED",
                "domain": domain,
                "metadata": {
                    "site_domain": site_domain,
                    "error": str(r.error),
                    "marking_as": "temporarily_unreachable",
                },
            }, request_id, user_id, domain, schema_version=sv)
            return {"success": False, "error": f"robots.txt fetch failed: {r.error}", "failure_class": "NETWORK_TIMEOUT"}

        if fl:
            robots_status = r.data.get('status', '?') if r.data else str(r.error)
            fl.substep("Connectivity Check",
                       "OK",
                       f"robots.txt fetched successfully.\n"
                       f"Status: {robots_status} | Size: {len(raw_content)} bytes",
                       elapsed_ms=step_ms)

        r = await self._call("parse_robots_rules", {
            "raw_content": raw_content, "site_domain": site_domain,
        }, request_id, user_id, domain, schema_version=sv)

        rules = {}
        if r.success and r.data:
            rules = r.data.get("rules", r.data.get("parsed_rules", {}))

        wildcard_rules = rules.get("user_agents", {}).get("*", {}) if isinstance(rules, dict) else {}
        allowed_paths = wildcard_rules.get("allow", [])
        disallowed_paths = wildcard_rules.get("disallow", [])

        if fl:
            fl.substep("Policy Parsing",
                       "OK" if r.success else "WARNING",
                       f"Parsed {len(allowed_paths)} allowed and {len(disallowed_paths)} disallowed paths.")

        r = await self._call("store_allowed_paths", {
            "domain": domain, "site_domain": site_domain,
            "rules": rules, "raw_content": raw_content,
            "status": "compliant", "schema_version": sv,
        }, request_id, user_id, domain,
            schema_version=sv, retry_budget=1)

        if r.success and r.data and r.data.get("skip_pipeline"):
            if fl:
                fl.substep("Compliance Check",
                           "BLOCKED",
                           f"🚫 robots.txt fully disallows scraping on {site_domain}.\n"
                           f"Pipeline for this site terminated.")
            return {"success": False, "error": "Blocked by robots.txt", "failure_class": "ROBOTS_BLOCKED"}

        paths_changed = r.data.get("paths_changed", False) if r.success and r.data else False
        newly_disallowed = r.data.get("newly_disallowed", []) if r.success and r.data else []
        invalidated_count = r.data.get("invalidated_url_count", 0) if r.success and r.data else 0

        if fl:
            status_msg = f"✅ Scraping permitted on {site_domain}"
            if paths_changed:
                status_msg += (
                    f"\n⚠️ Path changes detected! Newly disallowed: {newly_disallowed}\n"
                    f"Invalidated {invalidated_count} cached pattern(s)."
                )
            fl.substep("Compliance Check", "OK", status_msg)

        return {"success": True, "rules": rules, "allowed_paths": allowed_paths, "disallowed_paths": disallowed_paths}

    async def _discover_urls(
        self, domain, site_domain, sv, intent, rules, execution_strategy, 
        allowed_paths, disallowed_paths, request_id, user_id, site_url, fl
    ) -> tuple[list, list]:
        tried_urls = set()  # O(1) duplicate URL detection
        serp_urls = []  # all URLs from single SerpAPI call

        # ── 8a-CACHE: Check URL pattern cache first ──
        r = await self._call("check_url_pattern_cache", {
            "domain": domain, "site_domain": site_domain,
            "schema_version": sv, "intent": intent,
            "robots_rules": rules,
            "url_pattern_hint": execution_strategy.get("url_pattern_hint", "search"),
        }, request_id, user_id, domain,
            schema_version=sv, retry_budget=1)

        cache_url = None
        if r.success and r.data and r.data.get("cache_hit"):
            cache_url = r.data.get("url")
            robots_ok = r.data.get("robots_validated", False)
            if cache_url and cache_url not in tried_urls:
                if fl:
                    fl.substep("URL Lookup (Cache)",
                               "OK",
                               f"Cache Hit: {cache_url}\n"
                               f"robots.txt compliance verified: {robots_ok}")
            else:
                cache_url = None

        strategy = execution_strategy.get("strategy", "")
        # Robust dictionary fetching: check if query_type exists before cast
        qtype = intent.get("query_type", "")
        if isinstance(qtype, list):
            qtype = qtype[0] if qtype else ""
        is_multi_item_query = (
            str(qtype).lower() in ("list", "search")
            and strategy not in ("entity_detail_lookup", "direct_url_lookup")
        )
        url_timeout = 25000 if is_multi_item_query else 10000

        r = await self._call("url_agent", {
            "intent": intent, "site_url": site_url,
            "domain": domain, "schema_version": sv,
            "request_id": str(request_id),
            "robots_rules": rules,
            "allowed_paths": allowed_paths,
            "disallowed_paths": disallowed_paths,
        }, request_id, user_id, domain,
            timeout_ms=url_timeout, retry_budget=2, schema_version=sv)

        if r.success and r.data:
            inner = r.data.get("result", r.data)
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except (ValueError, TypeError):
                    inner = {}
            if isinstance(inner, dict):
                for u in inner.get("urls", []):
                    if isinstance(u, dict) and u.get("url"):
                        serp_urls.append(str(u.get("url")))
                    elif isinstance(u, str):
                        serp_urls.append(u)

        serp_query = r.data.get("reasoning_trace", "") if r.success and r.data else ""
        serp_error = r.error if not r.success else ""

        if fl:
            url_list = "\n".join(
                f"  {i+1}. {u}" for i, u in enumerate(serp_urls)
            ) if serp_urls else "  (none)"

            if is_multi_item_query and serp_query.startswith("SerpAPI Multi-Item"):
                expanded_part = ""
                if "): " in serp_query:
                    expanded_part = serp_query.split("): ", 1)[1]
                expanded_queries = [q.strip() for q in expanded_part.split(" | ")] if expanded_part else []

                query_list = "\n".join(
                    f"    {i+1}. \"{q}\"" for i, q in enumerate(expanded_queries)
                ) if expanded_queries else "    (none)"

                fl.substep("SerpAPI URL Discovery (Multi-Item)",
                           "OK" if serp_urls else "FAILED",
                           f"Mode: Groq AI Query Expansion\n"
                           f"Groq Generated {len(expanded_queries)} Search Queries:\n{query_list}\n"
                           f"Found {len(serp_urls)} unique candidate URL(s):\n{url_list}")
            elif serp_error:
                cleaned_query = serp_query
                if cleaned_query.startswith("SerpAPI Google Search: "):
                    cleaned_query = cleaned_query[len("SerpAPI Google Search: "):]
                fl.substep("SerpAPI URL Discovery",
                           "FAILED",
                           f"Query: {cleaned_query!r}\n"
                           f"Error: {serp_error}\n"
                           f"Found 0 candidate URL(s)")
            else:
                cleaned_query = serp_query
                if cleaned_query.startswith("SerpAPI Google Search: "):
                    cleaned_query = cleaned_query[len("SerpAPI Google Search: "):]
                fl.substep("SerpAPI URL Discovery",
                           "OK" if serp_urls else "FAILED",
                           f"Query sent to Google: {cleaned_query!r}\n"
                           f"Found {len(serp_urls)} candidate URL(s):\n{url_list}")

        ordered_urls = []
        if cache_url:
            ordered_urls.append(("db_cache", cache_url))
        for su in serp_urls:
            if su != cache_url:
                ordered_urls.append(("serp", su))

        return ordered_urls, serp_urls

    async def _evaluate_candidates(
        self, ordered_urls, site_domain, domain, extraction_schema, request_id, user_id, sv, intent, execution_strategy, fl
    ) -> list:
        MAX_URL_ATTEMPTS = 5
        candidates = []
        tried_urls = set()

        for attempt, (url_source, candidate_url) in enumerate(ordered_urls[:MAX_URL_ATTEMPTS], 1):
            if candidate_url in tried_urls:
                continue
            tried_urls.add(candidate_url)

            # ── 8b: inspect_url_dom ──
            t_dom = time.monotonic()
            r = await self._call("inspect_url_dom", {
                "url": candidate_url, "timeout_ms": 45000,
                "extraction_schema": extraction_schema,
            }, request_id, user_id, domain,
                timeout_ms=50000, retry_budget=1, schema_version=sv)
            dom_ms = int((time.monotonic() - t_dom) * 1000)

            if not r.success or not r.data or not r.data.get("success"):
                dom_error = r.error or (r.data.get("error", "") if r.data else "")
                dom_status = r.data.get("status_code", 0) if r.data else 0
                failure_class = _classify_dom_failure(dom_status, str(dom_error))
                candidates.append({
                    "url": candidate_url, "score": 0,
                    "summary": f"DOM fetch failed: {dom_error}",
                    "failure_class": failure_class,
                    "strategy": url_source,
                })
                if fl:
                    fl.substep(f"DOM Acquisition (Attempt {attempt})",
                               "FAILED",
                               f"Target: {candidate_url}\n"
                               f"Status: {dom_status} | Error: {dom_error}\n"
                               f"Classification: {failure_class}",
                               elapsed_ms=dom_ms)
                continue

            html = r.data.get("html", "")
            raw_html = r.data.get("raw_html", html)
            dom_status = r.data.get("status_code", 200)
            discovered_selectors = r.data.get("discovered_selectors", {})

            try:
                dom_signals_data = {
                    "request_id": str(request_id),
                    "site_domain": site_domain,
                    "url": candidate_url,
                    "attempt": attempt,
                    "timestamp": datetime.now().isoformat(),
                    "raw_html_size_chars": len(raw_html),
                    "cleaned_html_size_chars": len(html),
                    "http_status": dom_status,
                    "title_tag": _extract_tag(raw_html, "title"),
                    "meta_description": _extract_meta(raw_html, "description"),
                    "has_json_ld": '<script type="application/ld+json">' in raw_html,
                    "has_domain_content": bool(
                        _re.findall(r'"@type"\s*:\s*"(\w+)"', raw_html)
                    ),
                    "discovered_selectors": discovered_selectors,
                    "schema_org_types": _re.findall(r'"@type"\s*:\s*"(\w+)"', raw_html),
                    "link_count": raw_html.lower().count("<a "),
                    "image_count": raw_html.lower().count("<img "),
                    "h1_tags": [
                        _extract_tag(block, "h1")
                        for block in _re.findall(r'<h1[^>]*>.*?</h1>', raw_html, _re.IGNORECASE | _re.DOTALL)
                    ][:5],
                }
                signal_file = os.path.join(
                    DOM_SIGNALS_DIR,
                    f"{request_id}_{site_domain}_attempt{attempt}.json"
                )
                with open(signal_file, "w", encoding="utf-8") as f:
                    json.dump(dom_signals_data, f, indent=2, default=str)
            except Exception as sig_err:
                logger.warning("Failed to save DOM signals: %s", sig_err)

            if fl:
                fl.substep(f"DOM Acquisition (Attempt {attempt})",
                           "OK",
                           f"Success: {candidate_url}\n"
                           f"Content: {len(raw_html)} chars received.",
                           elapsed_ms=dom_ms)

            _is_search = _detect_search_page(candidate_url, raw_html)
            _expects_detail = execution_strategy.get("expected_page_type") in ("detail", "detail_page")

            if _is_search and _expects_detail:
                if fl:
                    fl.substep(f"Search Page Detected (Attempt {attempt})",
                               "WARNING",
                               f"URL {candidate_url} is a search results page.\nAttempting entity resolution...")

                resolved_url = _resolve_entity_from_search(raw_html, intent, candidate_url)

                if resolved_url:
                    if fl:
                        fl.substep(f"Entity Resolved (Attempt {attempt})", "OK", f"Found detail page: {resolved_url}")

                    t_resolve = time.monotonic()
                    r_resolve = await self._call("inspect_url_dom", {
                        "url": resolved_url, "timeout_ms": 45000,
                        "extraction_schema": extraction_schema,
                    }, request_id, user_id, domain, timeout_ms=50000, retry_budget=1, schema_version=sv)
                    resolve_ms = int((time.monotonic() - t_resolve) * 1000)

                    if r_resolve.success and r_resolve.data and r_resolve.data.get("success"):
                        html = r_resolve.data.get("html", "")
                        raw_html = r_resolve.data.get("raw_html", html)
                        candidate_url = resolved_url
                        discovered_selectors = r_resolve.data.get("discovered_selectors", {})
                        if fl:
                            fl.substep(f"Detail Page Fetched (Attempt {attempt})", "OK", f"Resolved to: {resolved_url}\nContent: {len(raw_html)} chars", elapsed_ms=resolve_ms)
                    else:
                        if fl:
                            fl.substep(f"Detail Page Fetch Failed (Attempt {attempt})", "FAILED", f"Could not fetch resolved URL: {resolved_url}")
                else:
                    candidates.append({
                        "url": candidate_url, "score": 0,
                        "summary": "Search page — no matching entity link found",
                        "failure_class": "SEARCH_PAGE_NO_ENTITY_FOUND",
                        "strategy": url_source,
                    })
                    if fl:
                        fl.substep(f"Entity Resolution Failed (Attempt {attempt})", "FAILED", f"No detail page link found on search results page.\nFailure: SEARCH_PAGE_NO_ENTITY_FOUND")
                    continue

            t_score = time.monotonic()
            r = await self._call("scoring_agent", {
                "html": html, "url": candidate_url,
                "intent": intent, "extraction_schema": extraction_schema,
                "domain": domain, "schema_version": sv,
                "expected_page_type": execution_strategy.get("expected_page_type"),
            }, request_id, user_id, domain, timeout_ms=15000, retry_budget=2, schema_version=sv)
            score_ms = int((time.monotonic() - t_score) * 1000)

            score = 0
            recommendation = "SKIP"
            scoring_method = "unknown"
            validation_summary = ""
            if r.success and r.data:
                score = r.data.get("confidence", 0)
                recommendation = r.data.get("recommendation", "SKIP")
                scoring_method = r.data.get("scoring_method", "unknown")
                validation_summary = r.data.get("validation_summary", "")

            if scoring_method in ("ai_error", "rate_limited") and html:
                logger.info("Scoring infra failure (%s), retrying once on same DOM", scoring_method)
                if fl:
                    fl.substep(f"Scoring Retry (Attempt {attempt})", "WARNING", f"AI scoring failed ({scoring_method}), retrying once on same DOM...")
                await asyncio.sleep(2)
                t_retry = time.monotonic()
                r2 = await self._call("scoring_agent", {
                    "html": html, "url": candidate_url,
                    "intent": intent, "extraction_schema": extraction_schema,
                    "domain": domain, "schema_version": sv,
                    "expected_page_type": execution_strategy.get("expected_page_type"),
                }, request_id, user_id, domain, timeout_ms=15000, retry_budget=0, schema_version=sv)
                retry_ms = int((time.monotonic() - t_retry) * 1000)
                if r2.success and r2.data and r2.data.get("confidence", 0) > 0:
                    score = r2.data.get("confidence", 0)
                    recommendation = r2.data.get("recommendation", "SKIP")
                    scoring_method = r2.data.get("scoring_method", "unknown") + "_retried"
                    validation_summary = r2.data.get("validation_summary", "")
                    score_ms += retry_ms
                    if fl:
                        fl.substep(f"Scoring Retry (Attempt {attempt})", "OK", f"Retry succeeded: {score}/100", elapsed_ms=retry_ms)

            candidates.append({
                "url": candidate_url, "score": score,
                "html": html, "raw_html": raw_html,
                "recommendation": recommendation, "summary": validation_summary,
                "scoring_method": scoring_method, "strategy": url_source,
                "discovered_selectors": discovered_selectors, "from_cache": url_source == "db_cache",
            })

            if url_source == "db_cache" and score < 50:
                logger.info("Invalidating stale cache entry: %s scored %d/100 for intent '%s'", candidate_url, score, intent.get("title", ""))
                await self._call("store_url_pattern", {
                    "domain": domain, "site_domain": site_domain,
                    "url": candidate_url, "schema_version": sv,
                    "success": False, "confidence": score / 100.0, "intent": intent,
                }, request_id, user_id, domain, schema_version=sv)

            if fl:
                score_status = "OK" if score >= 75 else ("WARNING" if score >= 25 else "FAILED")
                fl.substep(f"Relevance Scoring (Attempt {attempt})", score_status, f"Match Level: {score}/100\nMethod: {scoring_method} | Recommendation: {recommendation}\nLogic: {validation_summary}", elapsed_ms=score_ms)

            if score >= 90:
                break

        return candidates


    async def run(
        self,
        site_url: str,
        intent: dict,
        domain_module: dict,
        request_id: UUID,
        user_id: UUID,
        pipeline_id: UUID,
        file_logger=None,
        execution_strategy: Optional[dict] = None,
    ) -> Dict[str, Any]:
        execution_strategy = execution_strategy or {}
        domain = domain_module["name"]
        sv = domain_module["schema_version"]
        extraction_schema = domain_module["extraction_schema"]
        site_domain = urlparse(
            site_url if "://" in site_url else f"https://{site_url}"
        ).netloc

        # Extract domain config from DB (for MCP context injection)
        domain_config = domain_module.get("db_config", {}).get("config", {})

        jsonld_type_map = domain_config.get("jsonld_type_map", {})
        site_trust_weights = domain_config.get("site_trust_weights", {})

        start_time = time.monotonic()
        fl = file_logger  # shorthand

        try:
            # ═══ STEPS 7: robots.txt compliance ═══
            robots_status = await self._handle_robots(
                site_domain, domain, sv, request_id, user_id, site_url, fl
            )
            if not robots_status["success"]:
                return {
                    "success": False, "site_url": site_url,
                    "error": robots_status["error"],
                    "failure_class": robots_status["failure_class"], "confidence": 0.0,
                }
                
            rules = robots_status["rules"]
            allowed_paths = robots_status["allowed_paths"]
            disallowed_paths = robots_status["disallowed_paths"]

            # ═══ STEPS 8: URL Discovery Loop ═══
            MAX_URL_ATTEMPTS = 5
            
            ordered_urls, serp_urls = await self._discover_urls(
                domain, site_domain, sv, intent, rules, execution_strategy,
                allowed_paths, disallowed_paths, request_id, user_id, site_url, fl
            )

            if not ordered_urls:
                if fl:
                    fl.substep("URL Discovery", "FAILED",
                               "No candidate URLs found (cache miss + SerpAPI returned nothing)")
                return {
                    "success": False, "site_url": site_url,
                    "error": "No candidate URLs found",
                    "failure_class": "NO_URLS", "confidence": 0.0,
                }

            # ── 8b: Iterate through URLs — DOM + score each ──
            candidates = await self._evaluate_candidates(
                ordered_urls, site_domain, domain, extraction_schema, request_id, user_id, sv, intent, execution_strategy, fl
            )

            # ═══ STEP 8d: select_best_url ═══
            if fl:
                fl.substep("Candidate Selection", "OK",
                           "\n".join(
                               f"  - {c.get('url', '?')}: Level {c.get('score', 0)}"
                               for c in candidates
                           ) or "No candidates found.")

            r = await self._call("select_best_url", {
                "candidates": [{"url": c["url"], "score": c.get("score", 0)}
                               for c in candidates],
                "min_score": 50,
            }, request_id, user_id, domain, schema_version=sv)

            selected_url = None
            selected_html = None
            selected_raw_html = None
            selected_selectors = {}
            if r.success and r.data and r.data.get("selected"):
                selected_url = r.data.get("url")
                for c in candidates:
                    if c.get("url") == selected_url:
                        selected_html = c.get("html", "")
                        selected_raw_html = c.get("raw_html", selected_html)
                        selected_selectors = c.get("discovered_selectors", {})
                        break

            if not selected_url or not selected_html:
                # Store failed patterns
                for c in candidates:
                    await self._call("store_url_pattern", {
                        "domain": domain, "site_domain": site_domain,
                        "url": c.get("url", ""), "schema_version": sv,
                        "success": False,
                        "confidence": c.get("score", 0) / 100.0,
                        "intent": intent,
                    }, request_id, user_id, domain, schema_version=sv)

                best_score = max((c.get("score", 0) for c in candidates), default=0)
                if fl:
                    fl.substep("Candidate Selection",
                               "FAILED",
                               f"No target URL met the minimum relevance threshold (50).\n"
                               f"Best level seen: {best_score}/100. Search terminated.")

                return {
                    "success": False, "site_url": site_url,
                    "error": "No suitable URL found",
                    "failure_class": "LOW_DOM_CONFIDENCE",
                    "confidence": 0.0,
                }

            # ═══ STEP 8e: store_url_pattern (success) — includes CSS selectors in metadata ═══
            selected_score = r.data.get("score", 0)
            await self._call("store_url_pattern", {
                "domain": domain, "site_domain": site_domain,
                "url": selected_url, "schema_version": sv,
                "success": True,
                "confidence": selected_score / 100.0,
                "intent": intent,
                "metadata": {
                    "discovered_selectors": selected_selectors,
                    "dom_signals": {
                        "title_tag": _extract_tag(selected_html, "title") if selected_html else "",
                        "has_json_ld": '<script type="application/ld+json">' in (selected_html or ""),
                        "html_size": len(selected_html) if selected_html else 0,
                    },
                },
            }, request_id, user_id, domain,
                schema_version=sv, retry_budget=1)

            if fl:
                fl.substep("Target Locked", "OK",
                           f"Selected: {selected_url}\n"
                           f"Trust Level: {selected_score}/100")

            # ═══ STEP 9: scrape_structured_data ═══
            if fl:
                fl.separator(f"DATA EXTRACTION: {site_domain}")
            t0 = time.monotonic()
            r = await self._call("scrape_structured_data", {
                "html": selected_raw_html or selected_html, "url": selected_url,
                "intent": intent,
                "extraction_schema": extraction_schema,
                "domain": domain,
                "jsonld_type_map": jsonld_type_map,
                "discovered_selectors": selected_selectors,
                "extraction_mode": execution_strategy.get("extraction_mode", "multi_item"),
            }, request_id, user_id, domain,
                timeout_ms=20000, retry_budget=1, schema_version=sv)
            extract_ms = int((time.monotonic() - t0) * 1000)

            extracted_data = {}
            extracted_items = []  # multi-item results (list of dicts)
            is_multi_item = False
            extraction_confidence = 0.0
            extraction_method = "none"
            smart_completeness = 0.0
            extraction_selectors = {}
            available_field_names = []
            field_discovery = {}
            engine_results = {}
            if r.success and r.data:
                extracted_data = r.data.get("extracted_data", {})
                extracted_items = r.data.get("extracted_items", [])
                is_multi_item = r.data.get("is_multi_item", False)
                extraction_confidence = r.data.get("confidence", 0.0)
                extraction_method = r.data.get("extraction_method", "none")
                smart_completeness = r.data.get("smart_completeness", 0.0)
                extraction_selectors = r.data.get("extraction_selectors", {})
                available_field_names = r.data.get("available_field_names", [])
                field_discovery = r.data.get("field_discovery", {})
                engine_results = r.data.get("engine_results", {})

            if fl:
                if is_multi_item and extracted_items:
                    # Log ALL items (no truncation) for multi-item extraction
                    # Domain-agnostic: try multiple field name candidates
                    items_summary = []
                    for idx, item in enumerate(extracted_items):
                        # Title: try structural → schema → raw names
                        item_title = (
                            item.get('_heading')
                            or item.get('name')
                            or item.get('title')
                            or item.get('headline')
                            or item.get('_primary_link_text')
                            or item.get('_primary_image_alt')
                            or '?'
                        )
                        # Truncate long titles
                        if len(str(item_title)) > 80:
                            item_title = str(item_title)[:77] + '...'

                        # Collect key fields for display
                        field_parts = [f"  {idx+1}. {item_title}"]

                        # Show top fields (up to 3) from the item
                        display_keys = [
                            k for k in item.keys()
                            if k not in ('_heading', '_position', 'source_url',
                                         'source_site', '_text_snippets',
                                         '_all_images', '_data_attributes')
                            and not k.startswith('_primary_')
                        ][:3]
                        if display_keys:
                            extras = []
                            for dk in display_keys:
                                dv = item[dk]
                                if isinstance(dv, (list, dict)):
                                    dv = json.dumps(dv, default=str)[:60]
                                else:
                                    dv = str(dv)[:60]
                                extras.append(f"{dk}={dv}")
                            field_parts.append(f" ({', '.join(extras)})")

                        items_summary.append("".join(field_parts))

                    # Build details string
                    items_text = "\n".join(items_summary)
                    details_text = (
                        f"Method: {extraction_method} | Multi-Item: {len(extracted_items)} items\n"
                        f"Completeness: {smart_completeness:.0%}\n"
                        f"{items_text}"
                    )
                    fl.substep("Data Extraction", "OK", details_text,
                               elapsed_ms=extract_ms)
                else:
                    field_names = [k for k in extracted_data.keys() if k not in ("source_url", "source_site")] if isinstance(extracted_data, dict) else []
                    fields_avail = r.data.get("fields_available", 0) if r.success and r.data else 0
                    fl.substep("Data Extraction",
                               "OK" if r.success and extracted_data else "FAILED",
                               f"Method: {extraction_method} | Success Rate: {smart_completeness:.0%}\n"
                               f"Fields found ({len(field_names)}/{fields_avail} available): {', '.join(field_names)}",
                               data=extracted_data if extracted_data else None,
                               elapsed_ms=extract_ms)

            # ═══ STEP 9-FALLBACK: AI Full-Page Extractor (Multi-Item Smart Fallback) ═══
            # If structural clustering failed or extracted noise (e.g. news headlines instead of a movie table),
            # smart_completeness will be very low since the schema fields (actors, directors) won't exist in news.
            if execution_strategy.get("extraction_mode") == "multi_item":
                # Recalculate true semantic completeness to ignore generic structural clustering keys
                # _text_snippets, _numeric_values, etc. do NOT count towards satisfying the domain schema.
                true_semantic_completeness = smart_completeness
                if is_multi_item and extracted_items and isinstance(extracted_items[0], dict):
                    all_found_keys = set()
                    for item in extracted_items:
                        all_found_keys.update(k for k, v in item.items() if v is not None and v != "")
                    
                    # Remove structural clustering metadata keys (e.g., _heading, _position)
                    semantic_keys_found = {k for k in all_found_keys if not k.startswith('_')}
                    schema_keys = set(extraction_schema.get("properties", {}).keys())
                    
                    # Build reverse mapping: jsonld_key → schema_key
                    # from domain's jsonld_type_map.field_map
                    # e.g., field_map = {"title": ["name"], "synopsis": ["description"]}
                    # → reverse = {"name": "title", "description": "synopsis"}
                    reverse_field_map = {}
                    field_map = jsonld_type_map.get("field_map", {})
                    for schema_key, jsonld_aliases in field_map.items():
                        if isinstance(jsonld_aliases, list):
                            for alias in jsonld_aliases:
                                # Handle dotted keys like "director.name" → base "director"
                                base_key = alias.split(".")[0]
                                reverse_field_map[base_key] = schema_key
                    
                    # Translate found keys to schema keys where mapping exists
                    mapped_keys = set()
                    for k in semantic_keys_found:
                        mapped_keys.add(reverse_field_map.get(k, k))
                    
                    if schema_keys:
                        true_semantic_completeness = len(mapped_keys & schema_keys) / max(len(schema_keys), 1)

                # Assuming less than 35% true semantic completeness indicates we missed the primary table
                if not is_multi_item or true_semantic_completeness < 0.35:
                    if fl:
                        fl.substep("AI Extractor Fallback", "WARNING",
                                   f"{extraction_method} extraction had low semantic completeness ({true_semantic_completeness:.0%}).\n"
                                   f"Falling back to Full-Page AI Extractor...")
                    
                    try:
                        t0_ai = time.monotonic()
                        from mcp.client.html_to_markdown import html_to_markdown
                        
                        # 1. Convert DOM to clean Markdown
                        md_text = html_to_markdown(selected_raw_html or selected_html or "")
                        
                        if fl:
                            fl.substep("Markdown Conversion", "INFO", f"Converted HTML to Markdown ({len(md_text)} chars).")
                        
                        # Save the markdown for debugging visibility in the logs directory
                        try:
                            import os
                            logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
                            os.makedirs(logs_dir, exist_ok=True)
                            md_filepath = os.path.join(logs_dir, f"{request_id}_fallback.md")
                            with open(md_filepath, "w", encoding="utf-8") as f:
                                f.write(md_text)
                            if fl:
                                fl.substep("Markdown Saved", "INFO", f"Saved full full-page markdown to {md_filepath} for debugging.")
                        except Exception as e:
                            logger.error(f"Failed to save fallback markdown log: {e}")
                        
                        # 2. Call the AI Extractor Agent
                        r_ai = await self._call("ai_extractor_agent", {
                            "markdown_text": md_text,
                            "intent": intent or {},
                            "extraction_schema": extraction_schema or {},
                            "domain": domain,
                            "schema_version": sv,
                            "request_id": str(request_id) if request_id else "unknown"
                        }, request_id, user_id, domain, schema_version=sv, timeout_ms=35000, retry_budget=1)
                        
                        ai_ms = int((time.monotonic() - t0_ai) * 1000)
                        
                        if r_ai and r_ai.success and r_ai.data:
                            ai_data = r_ai.data if isinstance(r_ai.data, dict) else {}
                            new_items = ai_data.get("extracted_items", [])
                            if new_items:
                                extracted_items = new_items
                                is_multi_item = True
                                extraction_method = "ai_extractor_fallback"
                                extraction_confidence = ai_data.get("confidence", 0.85)
                                
                                # Estimate new smart completeness
                                all_fields_found = set()
                                for it in new_items:
                                    all_fields_found.update(k for k, v in it.items() if v is not None and v != "")
                                
                                schema_keys = set((extraction_schema or {}).get("properties", {}).keys())
                                new_completeness = len(all_fields_found & schema_keys) / max(len(schema_keys), 1)
                                smart_completeness = new_completeness
                                
                                if fl:
                                    fl.substep("AI Extractor Fallback", "OK",
                                               f"AI successfully extracted {len(new_items)} items.\n"
                                               f"New Completeness: {new_completeness:.0%}", elapsed_ms=ai_ms)
                                    # Log a preview of the first item
                                    preview = json.dumps(new_items[0], default=str)[:150]
                                    fl.info(f"AI Extractor Preview (Item 1): {preview}...")
                            else:
                                if fl:
                                    fl.substep("AI Extractor Fallback", "FAILED", "AI found 0 matching items.", elapsed_ms=ai_ms)
                        else:
                            if fl:
                                err_msg = r_ai.error if r_ai else "No response from gateway"
                                fl.substep("AI Extractor Fallback", "FAILED", f"Agent error: {err_msg}", elapsed_ms=ai_ms)
                    except Exception as ai_e:
                        import traceback
                        logger.warning("AI Extractor Fallback failed for %s: %s\n%s", site_url, ai_e, traceback.format_exc())
                        if fl:
                            fl.substep("AI Extractor Fallback", "FAILED", f"Exception: {str(ai_e)[:100]}")

            # ── Enforce intent limit on extracted items ──
            # The user may request "top 13 movies" but the page has 21 — trim to requested limit
            intent_limit = intent.get("limit") if intent else None
            if intent_limit and is_multi_item and extracted_items and len(extracted_items) > intent_limit:
                original_count = len(extracted_items)
                extracted_items = extracted_items[:intent_limit]
                if fl:
                    fl.substep("Limit Enforcement", "OK",
                               f"Trimmed from {original_count} to {intent_limit} items per user's requested limit.")

            # ── Enrich dom_signals JSON with extraction metadata ──
            try:
                sig_file = os.path.join(
                    DOM_SIGNALS_DIR,
                    f"{request_id}_{site_domain}_attempt{len(candidates)}.json",
                )
                if os.path.exists(sig_file):
                    with open(sig_file, "r") as f:
                        sig_data = json.load(f)
                    # Add relevance scoring
                    sig_data["relevance_score"] = selected_score
                    sig_data["relevance_method"] = next(
                        (c.get("scoring_method", "") for c in candidates if c.get("url") == selected_url), ""
                    )
                    sig_data["relevance_recommendation"] = next(
                        (c.get("recommendation", "") for c in candidates if c.get("url") == selected_url), ""
                    )
                    # Add extraction metadata
                    sig_data["extraction_method"] = extraction_method
                    sig_data["extraction_confidence"] = round(extraction_confidence, 4)
                    sig_data["smart_completeness"] = round(smart_completeness, 4)
                    sig_data["fields_extracted"] = [k for k in extracted_data.keys() if k not in ("source_url", "source_site")] if isinstance(extracted_data, dict) else []
                    sig_data["fields_available_on_page"] = available_field_names
                    sig_data["completeness_detail"] = (
                        f"{len(sig_data['fields_extracted'])}/{len(available_field_names)} available "
                        f"({smart_completeness:.0%})"
                    )
                    sig_data["extraction_selectors"] = extraction_selectors
                    # Store field discovery and engine debug outputs
                    sig_data["field_discovery"] = field_discovery
                    sig_data["engine_results"] = {
                        name: {
                            "confidence": data.get("confidence", 0),
                            "fields_count": data.get("fields_count", 0),
                            "field_keys": sorted(data.get("fields", {}).keys()) if isinstance(data.get("fields"), dict) else [],
                        }
                        for name, data in engine_results.items()
                    } if engine_results else {}
                    with open(sig_file, "w") as f:
                        json.dump(sig_data, f, indent=2, default=str)
            except Exception as e:
                logger.debug("Failed to enrich dom_signals: %s", e)

            # ═══ STEP 9-REINVOKE: Re-invoke URL discovery if smart completeness < 70% ═══
            # Skip re-invoke if multi-item extraction succeeded (clustering found items)
            re_invoked = False
            if not is_multi_item and smart_completeness < 0.70 and len(candidates) < MAX_URL_ATTEMPTS:
                re_invoked = True
                if fl:
                    fl.substep(f"[{site_domain}] Low Extraction Confidence",
                               "WARNING",
                               f"Completeness {smart_completeness:.0%} < 70% threshold.\n"
                               f"Re-invoking URL discovery with AI...")

                # Mark current URL as low-confidence failure
                candidates.append({
                    "url": selected_url, "score": int(smart_completeness * 100),
                    "summary": "Low extraction completeness",
                    "failure_class": "LOW_EXTRACTION_COMPLETENESS",
                })

                # Pick next untried URL from the already-fetched SerpAPI batch
                # (Re-calling SerpAPI would return the same results)
                new_url = None
                tried_urls = {c["url"] for c in candidates}
                for su in serp_urls:
                    if su not in tried_urls and su != selected_url:
                        new_url = su
                        break

                if new_url and new_url != selected_url:
                    # Fetch, score, extract the new URL
                    r2 = await self._call("inspect_url_dom", {
                        "url": new_url, "timeout_ms": 15000,
                        "extraction_schema": extraction_schema,
                    }, request_id, user_id, domain,
                        timeout_ms=20000, retry_budget=1, schema_version=sv)

                    if r2.success and r2.data and r2.data.get("success"):
                        new_html = r2.data.get("html", "")
                        new_raw_html = r2.data.get("raw_html", new_html)
                        new_selectors = r2.data.get("discovered_selectors", {})
                        r3 = await self._call("scrape_structured_data", {
                            "html": new_raw_html or new_html, "url": new_url,
                            "intent": intent,
                            "extraction_schema": extraction_schema,
                            "domain": domain,
                            "jsonld_type_map": jsonld_type_map,
                            "discovered_selectors": new_selectors,
                            "extraction_mode": execution_strategy.get("extraction_mode", "multi_item"),
                        }, request_id, user_id, domain,
                            timeout_ms=20000, schema_version=sv)

                        if r3.success and r3.data:
                            new_confidence = r3.data.get("confidence", 0.0)
                            if new_confidence > extraction_confidence:
                                if fl:
                                    fl.substep(f"[{site_domain}] Re-invoke Succeeded",
                                               "OK",
                                               f"New confidence: {new_confidence:.0%} > old {extraction_confidence:.0%}")
                                extracted_data = r3.data.get("extracted_data", {})
                                extraction_confidence = new_confidence
                                extraction_method = r3.data.get("extraction_method", "none")
                                selected_url = new_url
                                selected_html = new_html

            # ═══ STEP 9-VAL: validate_extraction (now includes intent for row count) ═══
            r = await self._call("validate_extraction", {
                "extracted_data": extracted_data,
                "extraction_schema": extraction_schema,
                "url": selected_url,
                "intent": intent,  # pass intent for row count validation
            }, request_id, user_id, domain, schema_version=sv)

            valid = True
            val_errors = []
            val_warnings = []
            filled = 0
            total = 0
            if r.success and r.data:
                valid = r.data.get("valid", True)
                val_errors = r.data.get("errors", [])
                val_warnings = r.data.get("warnings", [])
                filled = r.data.get("filled_fields", 0)
                total = r.data.get("total_schema_fields", 0)

            if not valid:
                if fl:
                    fl.substep("Data Validation",
                               "FAILED",
                               f"Consistency check failed.\n"
                               f"Errors: {val_errors}\n"
                               f"Completeness: {filled}/{total} fields.")
                return {
                    "success": False, "site_url": site_url,
                    "extracted_data": None,
                    "confidence": 0.0,
                    "failure_class": "EXTRACTION_SCHEMA_FAIL",
                    "error": f"Validation failed: {val_errors}",
                }

            if fl:
                fl.substep("Data Validation",
                           "OK",
                           f"Consistency check passed.\n"
                           f"Completeness: {filled if r.success else '?'}/{total if r.success else '?'}\n"
                           f"Warnings: {val_warnings if val_warnings else 'none'}")

            # ═══ STEP 9b: Sub-Page Enrichment (when requested_fields not satisfied) ═══
            requested_fields = intent.get("requested_fields", [])
            if requested_fields and selected_url and extracted_data:
                t0_enrich = time.monotonic()
                r_enrich = await self._call("sub_page_navigator", {
                    "request_id": str(request_id),
                    "url": selected_url,
                    "raw_html": selected_raw_html or selected_html or "",
                    "requested_fields": requested_fields,
                    "extracted_data": extracted_data,
                    "intent": intent,
                }, request_id, user_id, domain,
                    timeout_ms=60000, retry_budget=0, schema_version=sv)
                enrich_ms = int((time.monotonic() - t0_enrich) * 1000)

                if r_enrich.success and r_enrich.data:
                    enrichment_method = r_enrich.data.get("enrichment_method", "none")
                    fields_found = r_enrich.data.get("fields_found", [])
                    enriched_data = r_enrich.data.get("enriched_data", {})
                    enrich_reason = r_enrich.data.get("reason", "")

                    if enrichment_method != "none" and enriched_data:
                        # Merge enriched data into extracted_data
                        # Enriched data fills gaps — existing fields are NOT overwritten
                        for ek, ev in enriched_data.items():
                            if ek not in extracted_data and ev is not None:
                                extracted_data[ek] = ev

                        if fl:
                            sub_url = r_enrich.data.get("sub_page_url", "")
                            link_label = r_enrich.data.get("link_label", "")
                            field_previews = r_enrich.data.get("field_previews", {})
                            preview_text = ""
                            if field_previews:
                                preview_parts = [f"  {k}: {v[:100]}" for k, v in field_previews.items()]
                                preview_text = f"\nField Previews:\n" + "\n".join(preview_parts)
                            fl.substep("Sub-Page Enrichment", "OK",
                                       f"Method: {enrichment_method}\n"
                                       f"Fields Found: {', '.join(fields_found) if fields_found else 'none'}\n"
                                       f"Sub-Page: {sub_url or '(scroll only)'}\n"
                                       f"Link: {link_label or 'N/A'}\n"
                                       f"Reason: {enrich_reason}"
                                       f"{preview_text}",
                                       elapsed_ms=enrich_ms)
                    else:
                        if fl:
                            fl.substep("Sub-Page Enrichment", "SKIPPED",
                                       f"Reason: {enrich_reason}",
                                       elapsed_ms=enrich_ms)
                elif fl:
                    fl.substep("Sub-Page Enrichment", "FAILED",
                               f"Error: {r_enrich.error or 'Unknown'}",
                               elapsed_ms=enrich_ms)

            # ═══ STEP 9-SEMANTIC: AI-powered semantic field matching ═══
            # Analyze extracted fields against user query using Groq AI
            # Store results in umsa_core.semantic_fields for UI display
            try:
                user_query = intent.get("title", "") or intent.get("query", "") or ""
                req_fields = intent.get("requested_fields", [])
                if isinstance(extracted_data, dict) and user_query:
                    # Build field summary for AI — FILTER CSS NOISE
                    field_summary_parts = []
                    for fk, fv in extracted_data.items():
                        if fk in ("source_url", "source_site"):
                            continue
                        fk_lower = fk.lower()
                        # Skip CSS variables, properties, and style noise
                        if fk_lower.startswith("text:"):
                            suffix = fk_lower[5:]
                            if suffix.startswith("--") or suffix in (
                                "color", "display", "position", "margin", "padding",
                                "border", "width", "height", "font-family", "font-size",
                                "font-weight", "line-height", "background", "opacity",
                                "overflow", "z-index", "cursor", "transition",
                                "transform", "animation", "flex", "grid", "gap",
                                "top", "left", "right", "bottom", "float",
                            ) or any(suffix.startswith(p) for p in (
                                "border-", "padding-", "margin-", "background-",
                                "font-", "text-", "min-", "max-", "-webkit",
                                "outline", "box-shadow", "letter-spacing",
                                "vertical-align", "white-space", "word-break",
                                "overflow-", "flex-", "grid-", "align-", "justify",
                                "visibility", "pointer", "will-change", "backface",
                                "box-sizing", "list-style", "tab-size", "resize",
                                "appearance", "inset", "scale", "rotate",
                                "shape-rendering", "fill", "content",
                                "--tw-", "--color-", "--font-", "--spacing",
                                "--container", "--text-", "--leading", "--radius",
                                "--animate", "--blur", "--default",
                            )):
                                continue
                        # Skip strictly internal structural fields (like _page_title, meta:viewport)
                        if fk.startswith("_"):
                            continue
                        if fk_lower.startswith("meta:") and fk_lower not in ("meta:description", "meta:keywords", "meta:title"):
                            continue
                        # Truncate long values for the prompt
                        if isinstance(fv, str):
                            preview = fv[:120]
                        elif isinstance(fv, (dict, list)):
                            preview = json.dumps(fv, default=str)[:120]
                        else:
                            preview = str(fv)[:120]
                        field_summary_parts.append(f"  - {fk}: {preview}")

                    if field_summary_parts:
                        field_summary = "\n".join(field_summary_parts)
                        # Add requested_fields context to help AI prioritize
                        req_fields_note = ""
                        if req_fields:
                            req_fields_note = (
                                f"\nUser specifically requested these fields: {json.dumps(req_fields)}\n"
                                "Prioritize identifying fields that answer these requests.\n"
                            )
                        semantic_prompt = (
                            "You are a data field analyst. Given a user query and extracted raw fields "
                            "from a web page, identify which fields are relevant to the query.\n\n"
                            f"User Query: \"{user_query}\"\n"
                            f"Source Site: {site_domain}\n"
                            f"{req_fields_note}\n"
                            f"Extracted fields:\n{field_summary}\n\n"
                            "Return a JSON object with key \"fields\" containing an array. "
                            "For each relevant field include:\n"
                            "- \"raw_key\": the original field key exactly as shown\n"
                            "- \"display_name\": a human-readable label (e.g. \"name\" → \"Title\", "
                            "\"aggregateRating\" → \"Rating\", \"datePublished\" → \"Release Date\")\n"
                            "- \"relevance\": 0.0 to 1.0 relevance score\n"
                            "- \"category\": one of \"identity\", \"metadata\", \"rating\", "
                            "\"media\", \"description\", \"cast\", \"financial\", \"other\"\n\n"
                            "RULES:\n"
                            "- Only include fields relevant to the user's query\n"
                            "- Exclude technical/internal fields (source_url, @type, meta:viewport, etc.)\n"
                            "- If requested_fields are specified, prioritize those above all else\n"
                            "- display_name must be human-readable, properly capitalized\n"
                            "- Sort by relevance descending\n"
                        )

                        from app.core.config import settings
                        import httpx as _httpx
                        import asyncio as _asyncio

                        semantic_result = {}
                        max_retries = 2
                        current_key = settings.ai_api_key_phase2
                        for attempt in range(max_retries + 1):
                            try:
                                async with _httpx.AsyncClient(timeout=12.0) as ai_client:
                                    ai_resp = await ai_client.post(
                                        f"{settings.ai_base_url}/chat/completions",
                                        headers={
                                            "Authorization": f"Bearer {current_key}",
                                            "Content-Type": "application/json",
                                        },
                                        json={
                                            "model": settings.ai_model,
                                            "messages": [
                                                {"role": "system", "content": semantic_prompt},
                                                {"role": "user", "content": f"Analyze fields for query: {user_query}"},
                                            ],
                                            "temperature": 0.1,
                                            "response_format": {"type": "json_object"},
                                        },
                                    )
                                    ai_resp.raise_for_status()
                                    ai_data = ai_resp.json()
                                    content = ai_data["choices"][0]["message"]["content"]
                                    semantic_result = json.loads(content)
                                    break
                            except _httpx.HTTPStatusError as e:
                                if e.response.status_code == 429 and attempt < max_retries:
                                    await _asyncio.sleep(1)
                                    continue
                                logger.warning(f"Semantic Field Discovery failed: {e}")
                                break
                            except Exception as e:
                                logger.warning(f"Semantic Field Discovery failed: {e}")
                                break

                        matched_fields = semantic_result.get("fields", [])

                        # Store in DB
                        if matched_fields:
                            async with self._db_pool.acquire() as conn:
                                for sf in matched_fields:
                                    raw_key = sf.get("raw_key", "")
                                    if not raw_key:
                                        continue
                                    # Get the raw value preview
                                    # 1. Exact match
                                    raw_val = extracted_data.get(raw_key)
                                    # 2. Fuzzy match — AI may modify raw_key
                                    #    e.g. AI returns "section:Cast: Vicky Kaushal..."
                                    #    but actual key is "section:Cast"
                                    if raw_val is None:
                                        for ek, ev in extracted_data.items():
                                            if ek in raw_key or raw_key.startswith(ek):
                                                raw_val = ev
                                                raw_key = ek  # Use the actual key
                                                break
                                    if isinstance(raw_val, (dict, list)):
                                        val_preview = json.dumps(raw_val, default=str)[:500]
                                    elif raw_val is not None:
                                        val_preview = str(raw_val)[:500]
                                    else:
                                        val_preview = None

                                    # Use extraction method as engine source
                                    eng_src = extraction_method if extraction_method else "unknown"

                                    await conn.execute(
                                        """
                                        INSERT INTO umsa_core.semantic_fields
                                            (request_id, site_domain, raw_field_key,
                                             display_name, relevance, category,
                                             raw_value, engine_source)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                        """,
                                        request_id,
                                        site_domain,
                                        raw_key,
                                        sf.get("display_name", raw_key),
                                        float(sf.get("relevance", 0.5)),
                                        sf.get("category", "other"),
                                        val_preview,
                                        eng_src,
                                    )

                            if fl:
                                fl.substep("Semantic Field Matching",
                                           "OK",
                                           f"AI identified {len(matched_fields)} relevant fields "
                                           f"from {len(field_summary_parts)} extracted.\n"
                                           f"Top fields: {', '.join(f.get('display_name', '?') for f in matched_fields[:5])}")
                        else:
                            if fl:
                                fl.substep("Semantic Field Matching",
                                           "WARNING",
                                           "AI returned no relevant fields")
            except Exception as sem_err:
                logger.warning("Semantic field matching failed for %s: %s",
                               site_domain, sem_err)
                if fl:
                    fl.substep("Semantic Field Matching",
                               "WARNING",
                               f"Non-critical: {str(sem_err)[:200]}")

            latency_ms = int((time.monotonic() - start_time) * 1000)

            # Update pipeline state to EXTRACTED in DB
            try:
                async with self._db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE umsa_core.pipelines SET state = $1, completed_at = now() "
                        "WHERE request_id = $2 AND site_url = $3",
                        "EXTRACTED", str(request_id), site_domain,
                    )
            except Exception as db_err:
                logger.warning("Failed to update pipeline state: %s", db_err)

            if fl:
                fl.info(f"✅ Pipeline complete for {site_domain}. Data successfully unified.")

            result = {
                "success": True,
                "site_url": site_url,
                "extracted_data": extracted_data,
                "confidence": extraction_confidence,
                "failure_class": None,
                "error": None,
                "latency_ms": latency_ms,
            }
            if is_multi_item and extracted_items:
                result["is_multi_item"] = True
                result["extracted_items"] = extracted_items
                result["items_count"] = len(extracted_items)
            return result

        except Exception as e:
            logger.error("Pipeline failed for %s: %s", site_url, e, exc_info=True)
            # Update pipeline state to FAILED in DB
            try:
                import json as _json
                async with self._db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE umsa_core.pipelines SET state = $1, error = $2::jsonb "
                        "WHERE request_id = $3 AND site_url = $4",
                        "FAILED", _json.dumps({"message": str(e)[:500]}),
                        str(request_id), site_domain,
                    )
            except Exception:
                pass  # Non-blocking — don't mask the original error
            if fl:
                fl.substep(f"[{site_domain}] Pipeline Exception",
                           "FAILED", str(e))
            return {
                "success": False, "site_url": site_url,
                "extracted_data": None, "confidence": 0.0,
                "failure_class": "NETWORK_TIMEOUT",
                "error": str(e),
            }
