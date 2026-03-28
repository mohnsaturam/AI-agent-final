"""
UMSA MCP Tool — Sub Page Navigator (Step 9b)
Production-grade post-extraction enrichment.

Architecture:
  Initial extraction → Semantic field detection (embeddings)
  → Scroll page → DOM block extraction
  → Discover navigation links → AI semantic ranking
  → Navigate best sub-page → Re-extract → Return enriched data

Improvements applied:
  1.  Lazy-loaded SentenceTransformer (no startup cost)
  2.  Batch embedding computation
  3.  Registrable-domain navigation (subdomain-safe)
  4.  Button/tab filtering (no false navigation)
  5.  (reserved)
  6.  Navigation candidate cap (max 25)
  7.  URL deduplication
  8.  Timeout protection on all network calls
  9.  Input validation
  10. Smart extraction merging (prefer richer values)
  11. Structured logging
  12. Consistent output format
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("umsa.tools.sub_page_navigator")

# ═══════════════════════════════════════════════════════════
# DIAGNOSTIC LOG WRITER
# ═══════════════════════════════════════════════════════════

_LOG_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "logs", "sub_page_navigator",
)


class _DiagLog:
    """Collects diagnostic data throughout the execute flow and writes a JSON log."""

    def __init__(self, request_id: str, site_domain: str, url: str):
        self._request_id = request_id
        self._site_domain = site_domain
        self._data = {
            "request_id": request_id,
            "site_domain": site_domain,
            "url": url,
            "timestamp": datetime.now().isoformat(),
            "steps": [],
        }

    def step(self, name: str, detail: dict):
        """Record a named step with its diagnostic detail."""
        self._data["steps"].append({
            "step": name,
            "time": datetime.now().isoformat(),
            **detail,
        })

    def set_result(self, result: dict):
        """Attach the final result summary."""
        self._data["result"] = {
            "success": result.get("success"),
            "enrichment_method": result.get("enrichment_method"),
            "fields_found": result.get("fields_found", []),
            "reason": result.get("reason", ""),
        }

    def flush(self):
        """Write the diagnostic log to disk."""
        try:
            os.makedirs(_LOG_DIR, exist_ok=True)
            fname = f"{self._request_id}_{self._site_domain}.json"
            path = os.path.join(_LOG_DIR, fname)
            with open(path, "w") as f:
                json.dump(self._data, f, indent=2, default=str)
            logger.info("Sub-page navigator log saved: %s", path)
        except Exception as e:
            logger.warning("Failed to write sub-page navigator log: %s", e)

# ═══════════════════════════════════════════════════════════
# 1. LAZY-LOADED EMBEDDING MODEL
# ═══════════════════════════════════════════════════════════

_embedding_model = None


def _get_embedding_model():
    """
    Lazy-load SentenceTransformer — only on first use.
    Avoids startup cost, memory pressure, and import crashes
    in worker/fork environments.
    """
    global _embedding_model
    if _embedding_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            _embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("SentenceTransformer model loaded (all-MiniLM-L6-v2)")
        except ImportError:
            logger.warning(
                "sentence-transformers not installed — "
                "falling back to keyword-based field detection"
            )
    return _embedding_model


# ═══════════════════════════════════════════════════════════
# 2. SEMANTIC FIELD DETECTOR (BATCH EMBEDDINGS)
# ═══════════════════════════════════════════════════════════

_SIMILARITY_THRESHOLD = 0.65

# Key prefixes that are noise — never contain actual field data
_NOISE_KEY_PREFIXES = (
    "a:Go to ", "a:Learn more", "a:Set your", "a:View title page for",
    "label:", "button:", "nav:", "aside",
    "li:", "li[role=",
    "script:", "style:", "text:",
    "div:suggestion", "div:announcement",
    "div[role=presentation]", "div[role=group]",
    "span:nav-", "span[role=presentation]",
    "testid:nav-", "testid:drawer", "testid:panel",
    "testid:category-", "testid:list-container",
    "testid:grouped-link",
    "ul[role=", "meta:", "og:",
)


def _is_meaningful_value(value) -> bool:
    """Check if a value contains actual data, not just a label or link text."""
    if value is None:
        return False
    if isinstance(value, (dict, list)):
        return bool(value)  # Non-empty dicts/lists are meaningful
    s = str(value).strip()
    if len(s) < 15:
        return False  # Too short to be real data (link labels, booleans, etc.)
    # Reject values that look like menu/navigation text
    if s.count(" ") > 10 and len(s) < 100:
        # Short text with lots of spaces = likely navigation menu concatenation
        return False
    return True


def detect_missing_fields(
    data: dict, requested_fields: List[str]
) -> List[str]:
    """
    Detect which requested_fields are NOT present in the extracted data
    using cosine similarity of embeddings.

    Filters out noise keys (navigation, ads, CSS) and validates that
    matched values contain actual data (not just link labels).

    Falls back to keyword matching if the model is unavailable.
    """
    if not data or not requested_fields:
        return list(requested_fields) if requested_fields else []

    # Filter out noise keys before comparison
    filtered_items = {}
    for k, v in data.items():
        if any(k.startswith(p) for p in _NOISE_KEY_PREFIXES):
            continue
        filtered_items[k] = v

    keys = list(filtered_items.keys())
    if not keys:
        return list(requested_fields)

    model = _get_embedding_model()
    if model is None:
        return _detect_missing_fields_keyword(filtered_items, requested_fields)

    try:
        from sklearn.metrics.pairwise import cosine_similarity

        # Batch encode — one call for all keys, one for all fields
        key_vectors = model.encode(keys, batch_size=64, show_progress_bar=False)
        field_vectors = model.encode(
            requested_fields, batch_size=64, show_progress_bar=False
        )

        # Compute full similarity matrix: (n_fields × n_keys)
        sim_matrix = cosine_similarity(field_vectors, key_vectors)

        missing = []
        for i, field in enumerate(requested_fields):
            best_idx = int(sim_matrix[i].argmax())
            best_score = float(sim_matrix[i].max())

            if best_score < _SIMILARITY_THRESHOLD:
                missing.append(field)
                continue

            # Validate that the matched key's VALUE is meaningful
            matched_key = keys[best_idx]
            matched_value = filtered_items[matched_key]
            if not _is_meaningful_value(matched_value):
                logger.info(
                    "Field '%s' matched key '%s' (score=%.2f) but value is not meaningful — treating as missing",
                    field, matched_key, best_score,
                )
                missing.append(field)

        logger.info(
            "Semantic field detection: %d requested → %d missing (threshold=%.2f, %d keys after noise filter)",
            len(requested_fields), len(missing), _SIMILARITY_THRESHOLD, len(keys),
        )
        return missing

    except Exception as e:
        logger.warning("Embedding field detection failed: %s — using keyword fallback", e)
        return _detect_missing_fields_keyword(filtered_items, requested_fields)


def _detect_missing_fields_keyword(
    data: dict, requested_fields: List[str]
) -> List[str]:
    """Keyword-based fallback when embeddings are unavailable."""
    keys_lower = {k.lower() for k in data.keys()}
    missing = []
    for field in requested_fields:
        field_lower = field.lower().replace("_", " ")
        found = any(
            field_lower in k or field.lower() in k
            for k in keys_lower
        )
        if not found:
            missing.append(field)
    return missing


# ═══════════════════════════════════════════════════════════
# DOM BLOCK EXTRACTION
# ═══════════════════════════════════════════════════════════

_MAX_HTML_PARSE = 1_000_000  # Cap HTML size for parsing


def extract_dom_blocks(html: str) -> dict:
    """
    Extract structured content blocks from page HTML.
    Captures:
      - heading→content sections (h1-h4)
      - table rows
      - definition lists (dl/dt/dd)
      - labeled list items (li with span label/value pairs)
      - data-testid sections (React apps like IMDb)
    """
    soup = BeautifulSoup(html[:_MAX_HTML_PARSE], "html.parser")
    blocks = {}

    # 1. Heading → sibling content sections
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        title = h.get_text(strip=True)
        if not title or len(title) > 120:
            continue

        content = []
        for sib in h.find_next_siblings():
            if sib.name in ["h1", "h2", "h3", "h4"]:
                break
            text = sib.get_text(" ", strip=True)
            if text and len(text) > 2:
                content.append(text)

        if content:
            blocks[f"section:{title}"] = "\n".join(content[:15])

    # 2. Table rows
    for i, table in enumerate(soup.find_all("table")[:10]):
        rows = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if cells:
                rows.append(" | ".join(cells))
        if rows:
            blocks[f"table:{i}"] = "\n".join(rows[:50])

    # 3. Definition lists (dl/dt/dd) — common for movie details
    for i, dl in enumerate(soup.find_all("dl")[:10]):
        pairs = []
        dts = dl.find_all("dt")
        for dt in dts:
            dd = dt.find_next_sibling("dd")
            if dd:
                label = dt.get_text(strip=True)
                value = dd.get_text(" ", strip=True)
                if label and value:
                    pairs.append(f"{label}: {value}")
        if pairs:
            blocks[f"deflist:{i}"] = "\n".join(pairs)

    # 4. Labeled list items (ul/li with label→value children)
    #    IMDb box office uses: <li><span>Budget</span><span>$160M</span></li>
    for ul in soup.find_all(["ul", "ol"])[:20]:
        items = []
        for li in ul.find_all("li", recursive=False):
            spans = li.find_all("span", recursive=True)
            if len(spans) >= 2:
                label = spans[0].get_text(strip=True)
                value = spans[-1].get_text(strip=True)
                if label and value and label != value:
                    items.append(f"{label}: {value}")
            elif len(spans) == 1:
                # Single span might be a label with adjacent text
                label = spans[0].get_text(strip=True)
                full_text = li.get_text(" ", strip=True)
                value = full_text.replace(label, "", 1).strip()
                if label and value and len(value) > 1:
                    items.append(f"{label}: {value}")
        if items:
            # Use first item's label as a group name
            group_name = items[0].split(":")[0].strip()[:40]
            blocks[f"list:{group_name}"] = "\n".join(items)

    # 5. data-testid sections (React apps like IMDb)
    for el in soup.find_all(attrs={"data-testid": True})[:30]:
        testid = el.get("data-testid", "")
        if not testid:
            continue
        # Only capture sections with meaningful content
        text = el.get_text(" ", strip=True)
        if text and 5 < len(text) < 500:
            # Avoid duplicating headings already captured
            key = f"testid:{testid}"
            if key not in blocks:
                blocks[key] = text

    # 6. CATCH-ALL: extract from all semantic / identifiable containers
    #    Scans section, article, aside, main, nav, footer, plus
    #    div/span elements with id, class, role, or aria-label.
    #    This ensures we capture data regardless of tag choice.
    _SEMANTIC_TAGS = {"section", "article", "aside", "main", "nav", "footer"}
    seen_texts = set(blocks.values())  # avoid duplicating content

    for el in soup.find_all(True):
        tag_name = el.name or ""

        # Determine if this element is worth extracting
        el_id = el.get("id", "")
        el_class = " ".join(el.get("class", []))
        el_role = el.get("role", "")
        el_aria = el.get("aria-label", "")

        # Accept semantic tags always, or generic tags with identifying attributes
        is_semantic = tag_name in _SEMANTIC_TAGS
        has_identity = bool(el_id or el_role or el_aria)

        if not is_semantic and not has_identity:
            continue

        # Build a key from the most descriptive attribute
        if el_aria:
            block_key = f"{tag_name}:{el_aria}"
        elif el_id:
            block_key = f"{tag_name}:{el_id}"
        elif el_role:
            block_key = f"{tag_name}[role={el_role}]"
        else:
            block_key = f"{tag_name}"

        # Skip if key already exists (from earlier extractors)
        block_key = block_key[:80]
        if block_key in blocks:
            continue

        text = el.get_text(" ", strip=True)
        # Filter: too short (noise), too long (whole-page), or already seen
        if not text or len(text) < 10 or len(text) > 2000:
            continue
        if text in seen_texts:
            continue

        blocks[block_key] = text[:500]
        seen_texts.add(text)

        # Safety cap — don't produce a massive dict
        if len(blocks) >= 200:
            break

    return blocks


# ═══════════════════════════════════════════════════════════
# 3. REGISTRABLE DOMAIN MATCHING
# ═══════════════════════════════════════════════════════════

def _registrable_domain(netloc: str) -> str:
    """
    Extract the registrable domain from a netloc.
    e.g., 'm.imdb.com' → 'imdb.com', 'data.example.co.uk' → 'example.co.uk'
    """
    parts = netloc.lower().split(".")
    # Handle common two-part TLDs (co.uk, com.au, etc.)
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "ac"):
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return netloc.lower()


def _same_site(url_a: str, url_b: str) -> bool:
    """Check if two URLs belong to the same registrable domain."""
    domain_a = _registrable_domain(urlparse(url_a).netloc)
    domain_b = _registrable_domain(urlparse(url_b).netloc)
    return domain_a == domain_b


# ═══════════════════════════════════════════════════════════
# 4 + 6 + 7. NAVIGATION DISCOVERY
#   - Buttons/tabs filtered (no false navigation)
#   - Candidate cap (max 25)
#   - URL deduplication
# ═══════════════════════════════════════════════════════════

_MAX_NAVIGATION_CANDIDATES = 25

# Links that are clearly non-content navigation
_SKIP_LABELS = frozenset({
    "home", "menu", "search", "login", "sign in", "register",
    "about", "contact", "privacy", "terms", "help", "faq",
    "settings", "close", "back", "next", "more",
})


def discover_navigation(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Discover navigable links from the page.

    Filters:
      - Only <a href> with real URLs (no buttons/tabs)
      - Same registrable domain only
      - SKIP links inside <nav>, <header>, <footer> (site-wide menus)
      - SKIP site-wide URL patterns (charts, calendars, news, etc.)
      - PRIORITISE links sharing the base URL path prefix
      - Deduplicated by URL
      - Capped at _MAX_NAVIGATION_CANDIDATES
    """
    soup = BeautifulSoup(html[:400_000], "html.parser")
    base_domain = _registrable_domain(urlparse(base_url).netloc)
    base_path = urlparse(base_url).path.rstrip("/")

    # Site-wide URL path segments that indicate generic navigation
    _SITEWIDE_PATH_SEGMENTS = frozenset({
        "/chart/", "/calendar/", "/news/", "/showtimes/",
        "/interest/", "/trailers/", "/originals/", "/whats-on-tv/",
        "/what-to-watch/", "/awards/", "/event/", "/poll/",
        "/registration/", "/ap/", "/preferences/", "/search/",
    })

    # Parent tags that indicate site-wide navigation containers
    _NAV_PARENT_TAGS = frozenset({"nav", "header", "footer"})

    prefixed = []    # links sharing base URL path (high priority)
    other = []       # other same-site links (lower priority)
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        label = a.get_text(strip=True)

        # Skip empty, anchor-only, or javascript links
        if not label or not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        # Skip obvious non-content labels
        if label.lower().strip() in _SKIP_LABELS:
            continue

        # ── NEW: Skip links inside nav/header/footer containers ──
        in_nav = False
        for parent in a.parents:
            if parent.name in _NAV_PARENT_TAGS:
                in_nav = True
                break
            # Also check role="navigation" on any parent
            if parent.get("role") == "navigation":
                in_nav = True
                break
        if in_nav:
            continue

        full_url = urljoin(base_url, href)
        link_domain = _registrable_domain(urlparse(full_url).netloc)

        # Same-site check (subdomain-safe)
        if link_domain != base_domain:
            continue

        link_path = urlparse(full_url).path

        # ── NEW: Skip site-wide URL patterns ──
        if any(seg in link_path for seg in _SITEWIDE_PATH_SEGMENTS):
            continue

        # URL deduplication
        normalized = full_url.rstrip("/")
        if normalized in seen_urls:
            continue
        # Skip self-links
        if normalized == base_url.rstrip("/"):
            continue
        seen_urls.add(normalized)

        entry = {
            "url": full_url,
            "label": label[:60],
        }

        # Prioritise links that share the base URL path prefix
        # e.g., /title/tt9477520/fullcredits shares /title/tt9477520/
        if base_path and link_path.startswith(base_path):
            prefixed.append(entry)
        else:
            other.append(entry)

    # Return prioritised links first, then others, capped
    candidates = (prefixed + other)[:_MAX_NAVIGATION_CANDIDATES]

    logger.info(
        "Navigation discovery: %d links (%d page-specific, %d other) from %s",
        len(candidates), len(prefixed), min(len(other), _MAX_NAVIGATION_CANDIDATES - len(prefixed)),
        base_url,
    )
    return candidates


# ═══════════════════════════════════════════════════════════
# AI LINK RANKING (with timeout + error handling)
# ═══════════════════════════════════════════════════════════

async def rank_navigation_ai(
    candidates: List[dict], missing_fields: List[str], intent: dict
) -> List[dict]:
    """
    Use AI to semantically rank navigation candidates by relevance
    to the missing fields. Falls back to unranked list on failure.
    """
    if not candidates:
        return []

    try:
        from app.core.config import settings

        payload = [
            {"id": i, "label": c["label"]}
            for i, c in enumerate(candidates)
        ]

        prompt = (
            f"User query: {intent.get('query', '')}\n\n"
            f"Missing data fields: {json.dumps(missing_fields)}\n\n"
            f"Navigation links found on the page:\n{json.dumps(payload)}\n\n"
            "Score each link 0.0–1.0 by how likely it leads to a page "
            "containing the missing fields.\n\n"
            'Return JSON: {"ranked":[{"id":0,"score":0.9}]}'
        )

        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.post(
                f"{settings.ai_base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.ai_api_key_phase3}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.ai_model_tier2,
                    "messages": [
                        {"role": "system", "content": "Rank navigation links by data relevance."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()

        result = resp.json()
        ranked = json.loads(
            result["choices"][0]["message"]["content"]
        ).get("ranked", [])

        for entry in ranked:
            idx = entry.get("id")
            score = entry.get("score", 0)
            if isinstance(idx, int) and 0 <= idx < len(candidates):
                candidates[idx]["score"] = score

        candidates.sort(key=lambda x: x.get("score", 0), reverse=True)

        logger.info(
            "AI link ranking: top=%s (score=%.2f), total=%d",
            candidates[0]["label"] if candidates else "none",
            candidates[0].get("score", 0) if candidates else 0,
            len(candidates),
        )

    except Exception as e:
        logger.warning("AI link ranking failed — returning unranked: %s", e)

    return candidates


# ═══════════════════════════════════════════════════════════
# 8. PAGE FETCHING (with timeouts)
# ═══════════════════════════════════════════════════════════

async def fetch_page(url: str) -> Optional[str]:
    """Fetch a page's raw HTML via DOM agent. Returns None on failure."""
    try:
        from mcp.client.agents.dom_validation_agent import fetch_dom

        result = await fetch_dom(url, timeout_ms=15000, mode="light")
        if not result.get("success") or not result.get("raw_html"):
            logger.warning(
                "fetch_page failed for %s: %s",
                url, result.get("error", "no HTML"),
            )
            return None

        return result["raw_html"]

    except Exception as e:
        logger.warning("fetch_page exception for %s: %s", url, e)
        return None


# ═══════════════════════════════════════════════════════════
# 10. EXTRACTION ENGINE (smart merging)
# ═══════════════════════════════════════════════════════════

def run_extractors(html: str) -> dict:
    """
    Run all extraction engines + DOM block extractor.
    Merging strategy: prefer the longer/richer value on key conflicts.
    """
    from mcp.server.tools.scrape_structured_data import (
        extract_jsonld,
        extract_microdata,
        extract_opengraph,
        extract_twitter_cards,
        extract_heuristic_patterns,
    )

    data = {}

    for extractor in [
        extract_jsonld,
        extract_microdata,
        extract_opengraph,
        extract_twitter_cards,
        extract_heuristic_patterns,
    ]:
        try:
            result = extractor(html)
            if result:
                for k, v in result.items():
                    # Prefer richer (longer) values on conflict
                    if k not in data or len(str(v)) > len(str(data.get(k, ""))):
                        data[k] = v
        except Exception as e:
            logger.debug("Extractor %s failed: %s", extractor.__name__, e)

    # DOM block extraction (tables, sections)
    try:
        blocks = extract_dom_blocks(html)
        data.update(blocks)  # blocks use unique prefixed keys, no conflict
    except Exception as e:
        logger.debug("DOM block extraction failed: %s", e)

    return data


# ═══════════════════════════════════════════════════════════
# 9 + 11 + 12. MAIN EXECUTE
#   - Input validation
#   - Structured logging at every stage
#   - Consistent output format
# ═══════════════════════════════════════════════════════════

def _ok_result(
    method: str,
    enriched_data: dict,
    fields_found: List[str],
    reason: str,
    sub_page_url: str = "",
    link_label: str = "",
) -> Dict[str, Any]:
    """Build a successful return dict in the format site_pipeline expects."""
    return {
        "success": True,
        "enrichment_method": method,
        "enriched_data": enriched_data,
        "fields_found": fields_found,
        "sub_page_url": sub_page_url,
        "link_label": link_label,
        "reason": reason,
    }


def _fail_result(reason: str) -> Dict[str, Any]:
    """Build a failure return dict."""
    return {
        "success": False,
        "enrichment_method": "none",
        "enriched_data": {},
        "fields_found": [],
        "reason": reason,
    }


async def execute(context, db_pool) -> Dict[str, Any]:
    """
    Sub-page navigation enrichment (Step 9b).

    Input (via context.input_data):
      - url: the selected page URL
      - raw_html: the raw HTML of the selected page
      - requested_fields: list of field names the user wants
      - extracted_data: already extracted data from Step 9
      - intent: the parsed intent

    Returns consistent dict with: success, enrichment_method,
    enriched_data, fields_found, sub_page_url, reason.
    """
    t0 = time.monotonic()

    # ── 9. Input Validation ──────────────────────────────────
    input_data = context.input_data if hasattr(context, "input_data") else {}
    url = input_data.get("url") or ""
    raw_html = input_data.get("raw_html") or ""
    requested_fields = input_data.get("requested_fields") or []
    extracted_data = input_data.get("extracted_data") or {}
    intent = input_data.get("intent") or {}

    if not isinstance(requested_fields, list):
        requested_fields = list(requested_fields) if requested_fields else []

    # Extract request_id and site_domain for log file naming
    request_id = input_data.get("request_id") or intent.get("request_id") or "unknown"
    site_domain = urlparse(url).netloc.replace("www.", "") if url else "unknown"
    diag = _DiagLog(request_id, site_domain, url)

    if not url or not requested_fields:
        result = _fail_result("No URL or requested_fields provided")
        diag.step("input_validation", {"status": "FAIL", "reason": "No URL or requested_fields"})
        diag.set_result(result)
        diag.flush()
        return result

    logger.info(
        "Sub-page navigator START: url=%s, requested_fields=%s",
        url, requested_fields,
    )

    # Log initial state
    initial_keys = sorted(extracted_data.keys()) if isinstance(extracted_data, dict) else []
    diag.step("input", {
        "url": url,
        "requested_fields": requested_fields,
        "initial_extracted_keys": initial_keys,
        "initial_extracted_count": len(initial_keys),
        "raw_html_size": len(raw_html),
        "raw_html_has_links": raw_html.lower().count("<a "),
        "intent_title": intent.get("title", ""),
    })

    # ── Detect missing fields ────────────────────────────────
    missing = detect_missing_fields(extracted_data, requested_fields)

    diag.step("missing_field_detection", {
        "requested_fields": requested_fields,
        "missing_fields": missing,
        "satisfied_fields": [f for f in requested_fields if f not in missing],
        "method": "embedding" if _embedding_model else "keyword",
    })

    if not missing:
        logger.info("All requested fields already satisfied")
        result = _ok_result(
            method="none",
            enriched_data={},
            fields_found=[],
            reason="All requested_fields already satisfied",
        )
        diag.set_result(result)
        diag.flush()
        return result

    logger.info("Missing fields after initial extraction: %s", missing)

    visited_urls = {url.rstrip("/")}
    scroll_html_saved = None  # Track scrolled HTML for navigation discovery

    # ── STEP 1: Scroll & Re-extract ──────────────────────────
    t1 = time.monotonic()
    try:
        from mcp.client.agents.dom_validation_agent import fetch_dom_with_scroll

        scroll_result = await fetch_dom_with_scroll(url, timeout_ms=55000)
        scroll_ms = int((time.monotonic() - t1) * 1000)

        if scroll_result.get("success") and scroll_result.get("raw_html"):
            scroll_html = scroll_result["raw_html"]
            scroll_html_saved = scroll_html  # Save for navigation discovery
            scroll_data = run_extractors(scroll_html)

            # Build preview of extracted data for diagnostics
            scroll_previews = {}
            for k, v in scroll_data.items():
                if isinstance(v, str):
                    scroll_previews[k] = v[:150]
                elif isinstance(v, (dict, list)):
                    scroll_previews[k] = json.dumps(v, default=str)[:150]
                else:
                    scroll_previews[k] = str(v)[:150]

            still_missing = detect_missing_fields(scroll_data, missing)
            found_fields = [f for f in missing if f not in still_missing]

            diag.step("scroll", {
                "status": "OK",
                "scroll_html_size": len(scroll_html),
                "scroll_html_has_links": scroll_html.lower().count("<a "),
                "scroll_extracted_keys": sorted(scroll_data.keys()),
                "scroll_extracted_count": len(scroll_data),
                "scroll_data_previews": scroll_previews,
                "still_missing_after_scroll": still_missing,
                "found_by_scroll": found_fields,
                "duration_ms": scroll_ms,
                "scroll_stats": scroll_result.get("scroll_stats", {}),
            })

            logger.info(
                "Scroll enrichment: %d fields found (%s), %dms",
                len(found_fields), found_fields, scroll_ms,
            )

            if found_fields:
                result = _ok_result(
                    method="scroll",
                    enriched_data=scroll_data,
                    fields_found=found_fields,
                    reason=f"Scroll revealed {len(found_fields)} field(s) in {scroll_ms}ms",
                )
                diag.set_result(result)
                diag.flush()
                return result
        else:
            scroll_ms = int((time.monotonic() - t1) * 1000)
            diag.step("scroll", {
                "status": "FAILED",
                "error": scroll_result.get("error", "unknown"),
                "duration_ms": scroll_ms,
            })
            logger.info(
                "Scroll fetch failed: %s (%dms)",
                scroll_result.get("error", "unknown"), scroll_ms,
            )
    except Exception as e:
        scroll_ms = int((time.monotonic() - t1) * 1000)
        diag.step("scroll", {
            "status": "ERROR",
            "error": str(e),
            "duration_ms": scroll_ms,
        })
        logger.warning("Scroll step failed: %s", e)

    # ── Time budget check ────────────────────────────────────
    elapsed = time.monotonic() - t0
    if elapsed > 70:
        result = _fail_result(
            f"Time budget exceeded after scroll ({elapsed:.0f}s)"
        )
        diag.set_result(result)
        diag.flush()
        return result

    # ── STEP 2: Discover navigation links ────────────────────
    # Use scrolled HTML for discovery if available (it has rendered links)
    # Original raw_html often has 0 links on SPA sites like IMDb
    nav_html = scroll_html_saved or raw_html
    nav_candidates = discover_navigation(nav_html, url)

    diag.step("navigation_discovery", {
        "candidates_found": len(nav_candidates),
        "candidates": [
            {"url": c["url"], "label": c["label"]}
            for c in nav_candidates[:15]
        ],
        "html_source": "scroll" if nav_html != raw_html else "original",
        "html_size": len(nav_html),
    })

    if not nav_candidates:
        logger.info("No navigation links found for missing fields %s", missing)
        result = _fail_result("No navigation links discovered")
        diag.set_result(result)
        diag.flush()
        return result

    # ── STEP 3: AI semantic ranking ──────────────────────────
    ranked = await rank_navigation_ai(nav_candidates, missing, intent)

    diag.step("ai_ranking", {
        "ranked_top_5": [
            {"url": c["url"], "label": c["label"], "score": c.get("score", 0)}
            for c in ranked[:5]
        ],
    })

    # ── STEP 4: Navigate top candidates → extract ────────────
    for item in ranked[:3]:
        sub_url = item["url"]
        normalized = sub_url.rstrip("/")

        # Skip already-visited
        if normalized in visited_urls:
            diag.step("sub_page_skip", {
                "url": sub_url,
                "reason": "already visited",
            })
            continue
        visited_urls.add(normalized)

        logger.info(
            "Navigating sub-page: %s (score=%.2f, label=%s)",
            sub_url, item.get("score", 0), item.get("label", ""),
        )

        sub_html = await fetch_page(sub_url)
        if not sub_html:
            diag.step("sub_page_fetch", {
                "url": sub_url,
                "status": "FAILED",
                "label": item.get("label", ""),
            })
            continue

        sub_data = run_extractors(sub_html)

        # Build preview
        sub_previews = {}
        for k, v in sub_data.items():
            if isinstance(v, str):
                sub_previews[k] = v[:150]
            elif isinstance(v, (dict, list)):
                sub_previews[k] = json.dumps(v, default=str)[:150]
            else:
                sub_previews[k] = str(v)[:150]

        still_missing = detect_missing_fields(sub_data, missing)
        found_fields = [f for f in missing if f not in still_missing]

        diag.step("sub_page_extract", {
            "url": sub_url,
            "label": item.get("label", ""),
            "score": item.get("score", 0),
            "html_size": len(sub_html),
            "extracted_keys": sorted(sub_data.keys()),
            "extracted_count": len(sub_data),
            "data_previews": sub_previews,
            "still_missing": still_missing,
            "found_fields": found_fields,
        })

        logger.info(
            "Sub-page %s: %d fields found (%s)",
            sub_url, len(found_fields), found_fields,
        )

        if found_fields:
            result = _ok_result(
                method="sub_page",
                enriched_data=sub_data,
                fields_found=found_fields,
                sub_page_url=sub_url,
                link_label=item.get("label", ""),
                reason=f"Sub-page {sub_url} has {len(found_fields)} field(s)",
            )
            diag.set_result(result)
            diag.flush()
            return result

    total_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "Sub-page navigator DONE: no enrichment found (%dms, visited %d pages)",
        total_ms, len(visited_urls),
    )
    result = _fail_result(
        f"Navigated {len(visited_urls) - 1} sub-pages but no matching fields found"
    )
    diag.set_result(result)
    diag.flush()
    return result