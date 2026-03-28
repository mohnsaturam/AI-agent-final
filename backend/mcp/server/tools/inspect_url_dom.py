"""
MCP Tool — inspect_url_dom (Step 8b) — REVISED

Fetch DOM via Playwright with 2-mode rendering and escalation.
CSS selector discovery DEPRECATED to diagnostic-only fallback.

Returns:
  - html: cleaned HTML for AI scoring
  - raw_html: raw HTML for extraction engines
  - discovered_selectors: diagnostic only (empty by default)
  - escalation_used: whether Mode B was triggered
"""

import json
import logging
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

logger = logging.getLogger("umsa.tools.inspect_url_dom")


async def execute(context, db_pool) -> Dict[str, Any]:
    """
    Fetch DOM with 2-mode rendering and JS escalation.

    Mode A (light): default — allows JS, blocks images/fonts/media.
    Mode B (full): escalation — allows everything, waits for networkidle.

    Escalation triggers (if ANY met after Mode A):
      - HTML < 5000 chars
      - No JSON-LD AND HTML < 50000 chars
      - SPA shell detected (<div id="root"> or <div id="__next"> with minimal text)
      - React/Vue hydration placeholder with < 1000 chars body text
      - Empty body (< 200 chars) with > 5 script tags
    """
    input_data = context.input_data
    url = input_data.get("url", "")
    timeout_ms = input_data.get("timeout_ms", 15000)
    extraction_schema = input_data.get("extraction_schema", {})

    if not url:
        return {
            "success": False, "url": url, "error": "No URL provided",
            "html": "", "raw_html": "", "status_code": 0, "latency_ms": 0,
            "discovered_selectors": {}, "escalation_used": False,
        }

    from mcp.client.agents.dom_validation_agent import fetch_dom

    # ── Mode A: Light fetch (default) ──
    result = await fetch_dom(url, timeout_ms=timeout_ms, mode="light")

    if not result.get("success"):
        return {
            "success": False, "url": url,
            "html": "", "raw_html": "",
            "status_code": result.get("status_code", 0),
            "error": result.get("error"),
            "latency_ms": result.get("latency_ms", 0),
            "discovered_selectors": {}, "escalation_used": False,
        }

    raw_html = result.get("raw_html", "")
    cleaned_html = result.get("html", "")
    escalation_used = False

    # ── Check escalation triggers ──
    needs_escalation = _check_escalation_triggers(raw_html)

    if needs_escalation:
        logger.info("Escalation triggered for %s — re-fetching in Mode B (full)", url)
        full_result = await fetch_dom(url, timeout_ms=30000, mode="full")

        if full_result.get("success"):
            raw_html = full_result.get("raw_html", raw_html)
            cleaned_html = full_result.get("html", cleaned_html)
            escalation_used = True
            logger.info(
                "Mode B yielded %d chars raw HTML (was %d in Mode A)",
                len(raw_html), len(result.get("raw_html", ""))
            )

    return {
        "success": True,
        "url": url,
        "html": cleaned_html,
        "raw_html": raw_html,
        "status_code": result.get("status_code", 200),
        "error": None,
        "latency_ms": result.get("latency_ms", 0),
        "discovered_selectors": {},  # DEPRECATED — diagnostic only
        "escalation_used": escalation_used,
    }


def _check_escalation_triggers(raw_html: str) -> bool:
    """
    Check if Mode A result needs escalation to Mode B.
    Returns True if any trigger fires.
    """
    html_size = len(raw_html)

    # Trigger 1: HTML too small
    if html_size < 5000:
        logger.debug("Escalation trigger: HTML size %d < 5000", html_size)
        return True

    # Trigger 2: No JSON-LD on a page that should have it
    has_jsonld = 'application/ld+json' in raw_html
    if not has_jsonld and html_size < 50000:
        logger.debug("Escalation trigger: No JSON-LD and HTML < 50K")
        return True

    # Trigger 3: SPA shell detected
    if re.search(r'<div\s+id="(root|__next|app)"', raw_html, re.I):
        # Check if the SPA shell has minimal text content
        text_only = re.sub(r'<[^>]+>', '', raw_html)
        text_only = re.sub(r'\s+', ' ', text_only).strip()
        if len(text_only) < 500:
            logger.debug("Escalation trigger: SPA shell with %d chars text", len(text_only))
            return True

    # Trigger 4: React/Vue hydration placeholder
    if ('data-reactroot' in raw_html or 'data-server-rendered' in raw_html):
        text_only = re.sub(r'<[^>]+>', '', raw_html)
        text_only = re.sub(r'\s+', ' ', text_only).strip()
        if len(text_only) < 1000:
            logger.debug("Escalation trigger: Hydration placeholder with %d chars", len(text_only))
            return True

    # Trigger 5: Empty body + heavy JS
    body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.I)
    if body_match:
        body_text = re.sub(r'<[^>]+>', '', body_match.group(1))
        body_text = re.sub(r'\s+', ' ', body_text).strip()
        script_count = raw_html.lower().count('<script')
        if len(body_text) < 200 and script_count > 5:
            logger.debug("Escalation trigger: Empty body (%d chars) + %d scripts", len(body_text), script_count)
            return True

    return False


def _discover_css_selectors(
    raw_html: str,
    extraction_schema: dict,
    url: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Dynamically discover CSS selectors from the live DOM structure.

    Analyzes the page's HTML to find the best CSS paths for each
    extraction schema field. Works with ANY site — no hardcoded
    selectors.

    Strategy:
      1. Parse DOM with BeautifulSoup
      2. For each schema field, use semantic heuristics to find
         candidate elements (tag types, attributes, content patterns)
      3. Generate a precise CSS selector path for each match
      4. Score candidates by confidence and return the best

    Returns:
        {
            "title": {"selector": "h1.movie-title", "sample_text": "Inception", "confidence": 0.9},
            "rating": {"selector": "span.rating-value", "sample_text": "8.8", "confidence": 0.8},
            ...
        }
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("BeautifulSoup not installed — selector discovery skipped")
        return {}

    soup = BeautifulSoup(raw_html, "html.parser")
    properties = extraction_schema.get("properties", {})
    discovered = {}

    for field_name, field_def in properties.items():
        # Skip metadata fields
        if field_name in ("source_url", "source_site"):
            continue

        candidates = _find_candidates_for_field(soup, field_name, field_def)
        if candidates:
            # Pick the best candidate
            best = max(candidates, key=lambda c: c["confidence"])
            discovered[field_name] = best

    if discovered:
        logger.info(
            "Discovered %d CSS selectors for %s: %s",
            len(discovered),
            urlparse(url).netloc,
            {k: v["selector"] for k, v in discovered.items()},
        )

    return discovered


def _find_candidates_for_field(
    soup, field_name: str, field_def: dict
) -> List[Dict[str, Any]]:
    """
    Find candidate DOM elements for a given extraction schema field.

    Uses semantic heuristics:
      - Tag types (h1/h2 → title, time → date, img → poster)
      - Attribute patterns (data-testid, aria-label, class names, itemprop)
      - Content patterns (numeric → rating/year, URL → poster)
    """
    candidates = []
    field_type = field_def.get("type", "string")
    field_desc = field_def.get("description", "").lower()

    # ── Strategy 1: Attribute-based discovery ──
    # Look for elements with class/id/data-testid/itemprop matching field name
    attr_patterns = _get_attribute_patterns(field_name)

    for pattern in attr_patterns:
        # Search by class name containing the pattern
        for el in soup.find_all(attrs={"class": re.compile(pattern, re.I)}):
            sel = _build_css_selector(el)
            text = el.get_text(strip=True)[:200]
            if text and sel:
                conf = _score_candidate(el, text, field_name, field_type, "class")
                candidates.append({
                    "selector": sel,
                    "sample_text": text,
                    "confidence": conf,
                    "match_type": "class_attr",
                })

        # Search by data-testid
        for el in soup.find_all(attrs={"data-testid": re.compile(pattern, re.I)}):
            sel = _build_css_selector(el)
            text = el.get_text(strip=True)[:200]
            if text and sel:
                conf = _score_candidate(el, text, field_name, field_type, "data-testid")
                candidates.append({
                    "selector": sel,
                    "sample_text": text,
                    "confidence": conf,
                    "match_type": "data_testid",
                })

        # Search by itemprop (schema.org microdata)
        for el in soup.find_all(attrs={"itemprop": re.compile(pattern, re.I)}):
            sel = _build_css_selector(el)
            text = el.get_text(strip=True)[:200]
            if text and sel:
                # itemprop matches get high confidence
                candidates.append({
                    "selector": sel,
                    "sample_text": text,
                    "confidence": 0.9,
                    "match_type": "itemprop",
                })

        # Search by aria-label
        for el in soup.find_all(attrs={"aria-label": re.compile(pattern, re.I)}):
            sel = _build_css_selector(el)
            text = el.get_text(strip=True)[:200]
            if text and sel:
                conf = _score_candidate(el, text, field_name, field_type, "aria-label")
                candidates.append({
                    "selector": sel,
                    "sample_text": text,
                    "confidence": conf,
                    "match_type": "aria_label",
                })

    # ── Strategy 2: Tag-based semantic discovery ──
    tag_candidates = _find_by_semantic_tags(soup, field_name, field_type)
    candidates.extend(tag_candidates)

    # Deduplicate by selector
    seen_selectors = set()
    unique = []
    for c in candidates:
        if c["selector"] not in seen_selectors:
            seen_selectors.add(c["selector"])
            unique.append(c)

    # Return top 3 candidates sorted by confidence
    unique.sort(key=lambda x: x["confidence"], reverse=True)
    return unique[:3]


def _get_attribute_patterns(field_name: str) -> List[str]:
    """
    Generate regex patterns to match CSS class/id/data-testid attributes
    related to a schema field name.
    """
    patterns = [field_name]

    # Common field-to-attribute mappings
    synonyms = {
        "title": ["title", "name", "heading", "headline"],
        "year": ["year", "release.*date", "date", "release"],
        "rating": ["rating", "score", "vote", "aggregate"],
        "director": ["director", "filmmaker", "directed"],
        "genre": ["genre", "category", "type"],
        "synopsis": ["synopsis", "description", "overview", "summary", "plot"],
        "poster_url": ["poster", "image", "thumbnail", "cover", "artwork"],
        "cast": ["cast", "actor", "star", "credit"],
        "runtime_minutes": ["runtime", "duration", "length", "time"],
    }

    if field_name in synonyms:
        patterns = synonyms[field_name]

    return patterns


def _find_by_semantic_tags(
    soup, field_name: str, field_type: str
) -> List[Dict[str, Any]]:
    """
    Find elements by semantic HTML tag patterns.
    """
    candidates = []

    if field_name == "title":
        # Title is usually the main h1
        for h1 in soup.find_all("h1"):
            text = h1.get_text(strip=True)[:200]
            if text and len(text) > 1:
                sel = _build_css_selector(h1)
                if sel:
                    candidates.append({
                        "selector": sel,
                        "sample_text": text,
                        "confidence": 0.85,
                        "match_type": "semantic_h1",
                    })
                    break  # Only take the first h1 for title

    elif field_name == "year":
        # Year often in <time> tags or links with year patterns
        for time_el in soup.find_all("time"):
            text = time_el.get_text(strip=True)
            dt = time_el.get("datetime", "")
            year_text = dt[:4] if dt else text
            if re.match(r"^(19|20)\d{2}$", year_text.strip()):
                sel = _build_css_selector(time_el)
                if sel:
                    candidates.append({
                        "selector": sel,
                        "sample_text": year_text.strip(),
                        "confidence": 0.85,
                        "match_type": "semantic_time",
                    })
                    break

    elif field_name == "rating":
        # Ratings often in spans with numeric content like "8.8" or "88%"
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            if re.match(r"^\d\.?\d?\s*/\s*10$", text) or re.match(r"^\d\.\d$", text):
                sel = _build_css_selector(span)
                if sel:
                    candidates.append({
                        "selector": sel,
                        "sample_text": text,
                        "confidence": 0.75,
                        "match_type": "semantic_rating_pattern",
                    })

    elif field_name in ("poster_url", "image"):
        # Images with relevant alt text
        for img in soup.find_all("img"):
            alt = img.get("alt", "").lower()
            src = img.get("src", "")
            if src and ("poster" in alt or "cover" in alt or "movie" in alt
                        or img.get("class") and any("poster" in c.lower()
                            for c in (img.get("class") or []))):
                sel = _build_css_selector(img)
                if sel:
                    candidates.append({
                        "selector": sel,
                        "sample_text": src[:200],
                        "confidence": 0.7,
                        "match_type": "semantic_img",
                    })
                    break

    return candidates


def _build_css_selector(el) -> str:
    """
    Build a unique CSS selector path for an element.

    Priority order for building selector:
      1. data-testid attribute (most stable)
      2. itemprop attribute
      3. id attribute
      4. tag + specific class combination
      5. Tag with parent context
    """
    tag = el.name
    if not tag:
        return ""

    # 1. data-testid (most reliable for modern sites)
    testid = el.get("data-testid")
    if testid:
        return f"{tag}[data-testid='{testid}']"

    # 2. itemprop (schema.org microdata)
    itemprop = el.get("itemprop")
    if itemprop:
        return f"{tag}[itemprop='{itemprop}']"

    # 3. id attribute
    elem_id = el.get("id")
    if elem_id and not re.match(r"^(ember|react|__next)", elem_id):
        return f"#{elem_id}"

    # 4. Tag + first meaningful class
    classes = el.get("class", [])
    if classes:
        # Filter out generic classes
        meaningful = [
            c for c in classes
            if len(c) > 2
            and not re.match(r"^(col|row|container|wrapper|flex|grid|d-|p-|m-|w-|h-)", c)
            and not re.match(r"^[a-f0-9]{6,}$", c)  # skip hash classes
        ]
        if meaningful:
            return f"{tag}.{meaningful[0]}"

    # 5. Tag + parent context
    parent = el.parent
    if parent and parent.name:
        parent_classes = parent.get("class", [])
        if parent_classes:
            meaningful_parent = [c for c in parent_classes if len(c) > 2]
            if meaningful_parent:
                return f"{parent.name}.{meaningful_parent[0]} > {tag}"

    # 6. Just the tag name as last resort
    return tag


def _score_candidate(
    el, text: str, field_name: str, field_type: str, match_source: str
) -> float:
    """
    Score how likely a candidate element is to contain the target field.
    """
    score = 0.5  # Base confidence

    # Boost for specific match types
    if match_source == "data-testid":
        score += 0.3
    elif match_source == "itemprop":
        score += 0.35
    elif match_source == "class":
        score += 0.15
    elif match_source == "aria-label":
        score += 0.1

    # Boost for matching tag semantics
    tag = el.name
    if field_name == "title" and tag in ("h1", "h2"):
        score += 0.15
    if field_name in ("year", "date") and tag == "time":
        score += 0.2
    if field_name == "rating" and tag == "span" and re.match(r"^\d", text):
        score += 0.1
    if field_name == "poster_url" and tag == "img":
        score += 0.15

    # Penalize very short or very long text
    if len(text) < 1:
        score -= 0.3
    elif len(text) > 500:
        score -= 0.2

    return min(max(score, 0.0), 1.0)
