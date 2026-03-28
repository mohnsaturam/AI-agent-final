"""
MCP Tool — scrape_structured_data (Step 9) — DOMAIN-AGNOSTIC

Fully generic, layout-agnostic, domain-agnostic semantic extraction.
Zero hardcoded selectors. Zero domain assumptions. Zero field mappings.

Extraction engines (all deterministic, zero AI):
  1) JSON-LD         — raw key-value extraction from ld+json scripts
  2) Microdata       — raw itemprop/itemscope extraction
  3) OpenGraph       — raw og:* meta tag extraction
  4) Twitter Cards   — raw twitter:* meta tag extraction
  5) Heuristic       — generic label:value text pattern extraction

All engines run. Each returns raw discovered fields.
Results merged by engine priority (JSON-LD > Microdata > OG > Twitter > Heuristic).
Nested structures preserved. No field-level weights.
"""

import json
import logging
import os
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

logger = logging.getLogger("umsa.tools.scrape_structured_data")

# Directory for storing extraction debug JSON
DOM_SIGNALS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "dom_signals",
)

# ═══════════════════════════════════════════
# Engine Reliability (generic, no field weights)
# ═══════════════════════════════════════════

ENGINE_PRIORITY = ["json_ld", "microdata", "opengraph", "twitter_cards", "heuristic"]

ENGINE_CONFIDENCE = {
    "json_ld": 0.95,
    "microdata": 0.85,
    "opengraph": 0.75,
    "twitter_cards": 0.70,
    "heuristic": 0.50,
}


# ═══════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════

async def execute(context, db_pool) -> Dict[str, Any]:
    """
    Extract structured data using generic, domain-agnostic engines.

    Runs ALL extraction engines, each returning raw key-value pairs.
    Results merged by engine priority. No domain-specific mapping.
    """
    input_data = context.input_data
    html = input_data.get("html", "")
    url = input_data.get("url", "")
    extraction_mode = input_data.get("extraction_mode", "single_item")

    # Accept but do not require these legacy params (backward compatibility)
    _intent = input_data.get("intent", {})
    _extraction_schema = input_data.get("extraction_schema", {})
    _request_id = input_data.get("request_id", "")
    _domain = input_data.get("domain", "")

    if not html:
        return {
            "success": False,
            "url": url,
            "error": "No HTML content provided",
            "extracted_data": {},
            "extraction_method": "none",
            "confidence": 0.0,
            "fields_extracted": 0,
            "field_discovery": {},
            "engine_results": {},
        }

    # ── Step 1: Field Discovery — scan the DOM for available signals ──
    field_discovery = _discover_fields(html)
    total_discoverable = _count_discoverable(field_discovery)

    # ── Step 2: Run all engines ──
    engine_outputs = {}

    # Engine 1: JSON-LD
    jsonld_fields = extract_jsonld(html)
    if jsonld_fields:
        density = len(jsonld_fields) / max(total_discoverable, 1)
        engine_outputs["json_ld"] = {
            "fields": jsonld_fields,
            "confidence": round(ENGINE_CONFIDENCE["json_ld"] * min(density, 1.0), 4),
            "fields_count": len(jsonld_fields),
        }

    # Engine 2: Microdata
    microdata_fields = extract_microdata(html)
    if microdata_fields:
        density = len(microdata_fields) / max(total_discoverable, 1)
        engine_outputs["microdata"] = {
            "fields": microdata_fields,
            "confidence": round(ENGINE_CONFIDENCE["microdata"] * min(density, 1.0), 4),
            "fields_count": len(microdata_fields),
        }

    # Engine 3: OpenGraph
    og_fields = extract_opengraph(html)
    if og_fields:
        density = len(og_fields) / max(total_discoverable, 1)
        engine_outputs["opengraph"] = {
            "fields": og_fields,
            "confidence": round(ENGINE_CONFIDENCE["opengraph"] * min(density, 1.0), 4),
            "fields_count": len(og_fields),
        }

    # Engine 4: Twitter Cards
    twitter_fields = extract_twitter_cards(html)
    if twitter_fields:
        density = len(twitter_fields) / max(total_discoverable, 1)
        engine_outputs["twitter_cards"] = {
            "fields": twitter_fields,
            "confidence": round(ENGINE_CONFIDENCE["twitter_cards"] * min(density, 1.0), 4),
            "fields_count": len(twitter_fields),
        }

    # Engine 5: Heuristic
    heuristic_fields = extract_heuristic_patterns(html)
    if heuristic_fields:
        density = len(heuristic_fields) / max(total_discoverable, 1)
        engine_outputs["heuristic"] = {
            "fields": heuristic_fields,
            "confidence": round(ENGINE_CONFIDENCE["heuristic"] * min(density, 1.0), 4),
            "fields_count": len(heuristic_fields),
        }

    # ── Multi-item extraction mode ──
    if extraction_mode == "multi_item":
        multi_result = _attempt_multi_item_extraction(html, engine_outputs, field_discovery, _intent)
        if multi_result:
            multi_result["field_discovery"] = field_discovery
            multi_result["engine_results"] = engine_outputs
            _save_extraction_debug(
                input_data, url, field_discovery, engine_outputs,
                multi_result.get("extracted_data", {}),
            )
            return multi_result

    # ── Step 3: Merge engine results (priority order) ──
    merged_data, methods_used, field_sources = _merge_engine_results(engine_outputs)

    if not merged_data:
        return {
            "success": False,
            "url": url,
            "error": "No extraction engine produced results",
            "extracted_data": {"source_url": url, "source_site": urlparse(url).netloc if url else ""},
            "extraction_method": "none",
            "confidence": 0.0,
            "fields_extracted": 0,
            "field_discovery": field_discovery,
            "engine_results": engine_outputs,
        }

    # Add source metadata
    merged_data["source_url"] = url
    merged_data["source_site"] = urlparse(url).netloc if url else ""

    # ── Step 4: Compute overall confidence ──
    # Best engine's confidence is the primary signal
    best_engine = methods_used[0] if methods_used else "none"
    best_confidence = engine_outputs.get(best_engine, {}).get("confidence", 0.0)

    extracted_field_names = [
        k for k, v in merged_data.items()
        if v is not None and k not in ("source_url", "source_site")
    ]

    result = {
        "success": True,
        "url": url,
        "extracted_data": merged_data,
        "extraction_method": "+".join(methods_used) if len(methods_used) > 1 else best_engine,
        "confidence": round(best_confidence, 4),
        "fields_extracted": len(extracted_field_names),
        "field_discovery": field_discovery,
        "engine_results": engine_outputs,
        "extraction_selectors": field_sources,
        # Backward compatibility fields
        "smart_completeness": round(
            len(extracted_field_names) / max(total_discoverable, 1), 4
        ),
        "fields_available": total_discoverable,
        "available_field_names": sorted(
            set(field_discovery.get("jsonld_keys", []))
            | set(field_discovery.get("microdata_keys", []))
            | set(field_discovery.get("meta_keys", []))
        ),
        "missing_fields": [],
        "low_confidence": best_confidence < 0.40,
        "engines_evaluated": len(engine_outputs),
        "all_engine_scores": {
            name: data["confidence"] for name, data in engine_outputs.items()
        },
    }

    # ── Save extraction debug ──
    _save_extraction_debug(input_data, url, field_discovery, engine_outputs, merged_data)

    return result


# ═══════════════════════════════════════════
# Field Discovery
# ═══════════════════════════════════════════

def _discover_fields(html: str) -> dict:
    """
    Scan HTML to discover what structured data signals exist.
    Returns raw key inventories — no domain mapping, no assumptions.
    """
    discovery = {
        "jsonld_keys": [],
        "microdata_keys": [],
        "meta_keys": [],
        "text_patterns": [],
    }

    # JSON-LD keys
    jsonld_items = _parse_jsonld_blocks(html)
    jsonld_keys = set()
    for item in jsonld_items:
        _collect_keys_recursive(item, jsonld_keys, prefix="")
    discovery["jsonld_keys"] = sorted(jsonld_keys)

    # Microdata itemprop values
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        itemprops = set()
        for el in soup.find_all(attrs={"itemprop": True}):
            itemprops.add(el.get("itemprop"))
        discovery["microdata_keys"] = sorted(itemprops)
    except ImportError:
        pass

    # Meta tags (og:*, twitter:*, standard)
    meta_keys = set()
    # OG tags
    for m in re.finditer(r'<meta[^>]*property="(og:[^"]+)"', html, re.IGNORECASE):
        meta_keys.add(m.group(1))
    # Twitter tags
    for m in re.finditer(r'<meta[^>]*name="(twitter:[^"]+)"', html, re.IGNORECASE):
        meta_keys.add(m.group(1))
    # Standard meta name tags
    for m in re.finditer(r'<meta[^>]*name="([^"]+)"[^>]*content="[^"]+"', html, re.IGNORECASE):
        name = m.group(1)
        if not name.startswith("twitter:"):
            meta_keys.add(name)
    discovery["meta_keys"] = sorted(meta_keys)

    # Text patterns (generic label:value)
    text_pattern = re.compile(r'^\s*([A-Za-z0-9 _-]{2,40})\s*:\s*(.+)$', re.MULTILINE)
    # Extract visible text from first 50K chars
    text_only = re.sub(r'<[^>]+>', ' ', html[:50000])
    patterns = []
    for m in text_pattern.finditer(text_only):
        label = m.group(1).strip()
        value = m.group(2).strip()[:200]
        if label and value and len(value) > 1:
            patterns.append(f"{label}: {value[:80]}")
    # Deduplicate and cap
    seen = set()
    unique_patterns = []
    for p in patterns:
        key = p.split(":")[0].strip().lower()
        if key not in seen:
            seen.add(key)
            unique_patterns.append(p)
    discovery["text_patterns"] = unique_patterns[:50]

    return discovery


def _collect_keys_recursive(obj, keys: set, prefix: str = ""):
    """Collect only TOP-LEVEL keys from a JSON-LD object.
    
    Does NOT recurse into nested objects — matches what the engines
    actually extract (top-level key-value pairs with nested values preserved).
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.startswith("@"):
                continue
            # Only add the top-level key, do NOT recurse into nested objects
            keys.add(k)
    elif isinstance(obj, list):
        for item in obj:
            _collect_keys_recursive(item, keys, prefix)


def _count_discoverable(discovery: dict) -> int:
    """Count total unique discoverable fields across all sources."""
    all_keys = set()
    all_keys.update(discovery.get("jsonld_keys", []))
    all_keys.update(discovery.get("microdata_keys", []))
    all_keys.update(discovery.get("meta_keys", []))
    all_keys.update(
        p.split(":")[0].strip().lower()
        for p in discovery.get("text_patterns", [])
    )
    return max(len(all_keys), 1)


# ═══════════════════════════════════════════
# Engine 1: JSON-LD (Generic)
# ═══════════════════════════════════════════

def extract_jsonld(html: str) -> dict:
    """
    Extract ALL key-value pairs from JSON-LD scripts.
    No type filtering, no alias mapping — raw extraction.
    Returns a flat dict of all discovered fields.
    """
    items = _parse_jsonld_blocks(html)
    if not items:
        return {}

    # Find the richest JSON-LD block (most keys)
    best = max(items, key=lambda item: _count_fields(item))

    # Flatten to top-level key-value pairs, preserving nested structures
    result = {}
    for key, value in best.items():
        if key.startswith("@"):
            # Preserve @type for context
            if key == "@type":
                result["@type"] = value
            continue
        result[key] = value

    return result


def _parse_jsonld_blocks(html: str) -> list:
    """Parse all JSON-LD script blocks from HTML. Handles @graph wrappers."""
    results = []
    pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)

    for match in matches:
        try:
            data = json.loads(match.strip())
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                if "@graph" in data:
                    graph = data["@graph"]
                    if isinstance(graph, list):
                        results.extend(graph)
                    else:
                        results.append(graph)
                else:
                    results.append(data)
        except json.JSONDecodeError:
            continue

    return results


def _count_fields(item: dict) -> int:
    """Count non-@ fields in a JSON-LD item."""
    if not isinstance(item, dict):
        return 0
    return sum(1 for k, v in item.items() if not k.startswith("@") and v is not None)


# ═══════════════════════════════════════════
# Engine 2: Microdata (Generic)
# ═══════════════════════════════════════════

def extract_microdata(html: str) -> dict:
    """
    Extract ALL itemprop key-value pairs from HTML Microdata.
    No type filtering, no field mapping — raw extraction.
    Groups by itemscope when available.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    result = {}

    # Find all elements with itemprop
    for el in soup.find_all(attrs={"itemprop": True}):
        prop_name = el.get("itemprop")
        if not prop_name:
            continue

        # Extract value based on tag type
        value = _extract_element_value(el)
        if value is None:
            continue

        # Handle multiple values for the same prop (e.g., multiple actors)
        if prop_name in result:
            existing = result[prop_name]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[prop_name] = [existing, value]
        else:
            result[prop_name] = value

    return result


def _extract_element_value(el) -> Any:
    """Extract the value from an HTML element based on its tag type."""
    if el.name == "meta":
        return el.get("content", "").strip() or None
    elif el.name == "img":
        return el.get("src", el.get("data-src", "")).strip() or None
    elif el.name == "time":
        return el.get("datetime", el.get_text(strip=True)) or None
    elif el.name == "link":
        return el.get("href", "").strip() or None
    elif el.name == "data":
        return el.get("value", el.get_text(strip=True)) or None
    else:
        text = el.get_text(strip=True)
        return text if text else None


# ═══════════════════════════════════════════
# Engine 3: OpenGraph (Generic)
# ═══════════════════════════════════════════

def extract_opengraph(html: str) -> dict:
    """
    Extract ALL og:* meta tag values.
    No field mapping — returns raw property:content pairs.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    result = {}

    for meta in soup.find_all("meta", attrs={"property": True}):
        prop = meta.get("property", "")
        content = meta.get("content", "").strip()
        if prop.startswith("og:") and content:
            result[prop] = content

    return result


# ═══════════════════════════════════════════
# Engine 4: Twitter Cards (Generic)
# ═══════════════════════════════════════════

def extract_twitter_cards(html: str) -> dict:
    """
    Extract ALL twitter:* meta tag values.
    Includes labeled pairs (twitter:label1/data1 through label9/data9).
    No field mapping — returns raw name:content pairs.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    result = {}

    # Direct twitter: meta tags
    for meta in soup.find_all("meta", attrs={"name": True}):
        name = meta.get("name", "")
        content = meta.get("content", "").strip()
        if name.startswith("twitter:") and content:
            result[name] = content

    # Labeled pairs: twitter:label1/data1 through label9/data9
    labeled_pairs = {}
    for i in range(1, 10):
        label_key = f"twitter:label{i}"
        data_key = f"twitter:data{i}"
        if label_key in result and data_key in result:
            label = result[label_key]
            data_value = result[data_key]
            labeled_pairs[label] = data_value

    if labeled_pairs:
        result["_labeled_pairs"] = labeled_pairs

    return result


# ═══════════════════════════════════════════
# Engine 5: Heuristic Patterns (Generic)
# ═══════════════════════════════════════════

def extract_heuristic_patterns(html: str) -> dict:
    """
    Extract data using generic text patterns.
    Fully universal — zero domain keywords.

    Extracts:
    - <title> tag content
    - All <meta name="..." content="..."> pairs
    - Generic label:value text patterns from visible content
    - Section-based key-value data (e.g., "Box office", "Details" sections)
    """
    result = {}

    # Title tag
    title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    if title_match:
        raw_title = title_match.group(1).strip()
        if raw_title:
            result["_page_title"] = raw_title

    # All meta name/content pairs (non-OG, non-Twitter — those have their own engines)
    for m in re.finditer(
        r'<meta[^>]*name="([^"]+)"[^>]*content="([^"]*)"',
        html[:30000], re.IGNORECASE,
    ):
        name = m.group(1).strip()
        content = m.group(2).strip()
        if content and not name.startswith(("og:", "twitter:")):
            result[f"meta:{name}"] = content

    # Also handle reverse attribute order: content="..." name="..."
    for m in re.finditer(
        r'<meta[^>]*content="([^"]*)"[^>]*name="([^"]+)"',
        html[:30000], re.IGNORECASE,
    ):
        content = m.group(1).strip()
        name = m.group(2).strip()
        key = f"meta:{name}"
        if content and not name.startswith(("og:", "twitter:")) and key not in result:
            result[key] = content

    # Generic label:value patterns from visible text
    # Scan a larger portion of the page (200K) because important data like
    # box office, budget, production info is often near the bottom of the page
    text_only = re.sub(r'<[^>]+>', '\n', html[:200000])
    label_value_pattern = re.compile(
        r'^\s*([A-Za-z0-9 _-]{2,40})\s*:\s*(.+)$',
        re.MULTILINE,
    )

    seen_labels = set()
    for m in label_value_pattern.finditer(text_only):
        label = m.group(1).strip()
        value = m.group(2).strip()[:500]
        label_lower = label.lower()

        # Skip if already seen or too short value
        if label_lower in seen_labels or len(value) < 2:
            continue
        seen_labels.add(label_lower)

        result[f"text:{label}"] = value

    # ── Section-based extraction: find labeled data blocks ──
    # Looks for heading + value patterns common in movie detail pages
    # e.g., <h3>Box office</h3> followed by label/value pairs
    # This is fully generic — no domain keywords hardcoded
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Find all data lists (dl/dt/dd patterns)
        for dl in soup.find_all("dl"):
            current_label = None
            for child in dl.children:
                if hasattr(child, 'name'):
                    if child.name == "dt":
                        current_label = child.get_text(strip=True)
                    elif child.name == "dd" and current_label:
                        value = child.get_text(strip=True)
                        if current_label and value:
                            key = f"text:{current_label}"
                            if key not in result:
                                result[key] = value
                        current_label = None

        # Find section-header + list-item patterns
        # e.g., <h3>Box office</h3> <ul><li>Gross US: $19M</li></ul>
        for heading in soup.find_all(["h2", "h3", "h4"]):
            section_name = heading.get_text(strip=True)
            if not section_name or len(section_name) > 50:
                continue

            # Look at siblings immediately after the heading
            sibling = heading.find_next_sibling()
            items_found = 0
            while sibling and items_found < 20:
                if sibling.name in ("h1", "h2", "h3", "h4"):
                    break  # Next section started

                # Extract from list items within this section
                if sibling.name in ("ul", "ol", "div", "dl"):
                    for li in sibling.find_all(["li", "div", "span", "dd"], recursive=True):
                        text = li.get_text(strip=True)
                        if text and len(text) > 3:
                            # Try to split into label:value
                            if ":" in text:
                                parts = text.split(":", 1)
                                lbl = parts[0].strip()
                                val = parts[1].strip()
                                if lbl and val:
                                    key = f"text:{section_name} - {lbl}"
                                    if key not in result:
                                        result[key] = val
                                        items_found += 1

                sibling = sibling.find_next_sibling()

    except ImportError:
        pass
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════
# Engine Merging
# ═══════════════════════════════════════════

def _merge_engine_results(engine_outputs: dict) -> tuple:
    """
    Merge all engine outputs by priority order.
    Higher-priority engines win on key conflicts.
    Nested structures are preserved.

    Returns: (merged_data, methods_used, field_sources)
    """
    merged = {}
    methods_used = []
    field_sources = {}

    # Iterate in priority order
    for engine_name in ENGINE_PRIORITY:
        if engine_name not in engine_outputs:
            continue

        engine_data = engine_outputs[engine_name].get("fields", {})
        if not engine_data:
            continue

        added_fields = False
        for key, value in engine_data.items():
            if value is None:
                continue
            # Higher-priority engine's value is never overwritten
            if key not in merged:
                merged[key] = value
                field_sources[key] = {
                    "engine": engine_name,
                    "source": engine_name,
                }
                added_fields = True

        if added_fields:
            methods_used.append(engine_name)

    return merged, methods_used, field_sources


# ═══════════════════════════════════════════
# Multi-Item Extraction
# ═══════════════════════════════════════════

def _attempt_multi_item_extraction(
    html: str, engine_outputs: dict, field_discovery: dict,
    intent: dict = None,
) -> dict:
    """
    Attempt multi-item extraction for list/discovery pages.
    Uses JSON-LD ItemList, multiple JSON-LD blocks, and structural clustering.
    Returns None if no multi-item result found.
    """
    multi_results = []

    # Approach 1: JSON-LD multi-item (ItemList or multiple typed objects)
    try:
        items = _parse_jsonld_blocks(html)
        jsonld_multi = _extract_jsonld_multi_items(items)
        if jsonld_multi and len(jsonld_multi) >= 2:
            density = len(jsonld_multi) / max(_count_discoverable(field_discovery), 1)
            conf = ENGINE_CONFIDENCE["json_ld"] * min(density, 1.0)
            multi_results.append(("json_ld_multi", jsonld_multi, conf))
            logger.info("JSON-LD multi-item: %d items (conf=%.2f)", len(jsonld_multi), conf)
    except Exception as e:
        logger.debug("JSON-LD multi-item extraction failed: %s", e)

    # Approach 2: Microdata multi-item (multiple itemscope blocks)
    try:
        microdata_items = _extract_microdata_multi(html)
        if microdata_items and len(microdata_items) >= 2:
            density = len(microdata_items) / max(_count_discoverable(field_discovery), 1)
            conf = ENGINE_CONFIDENCE["microdata"] * min(density, 1.0)
            multi_results.append(("microdata_multi", microdata_items, conf))
            logger.info("Microdata multi-item: %d items (conf=%.2f)", len(microdata_items), conf)
    except Exception as e:
        logger.debug("Microdata multi-item extraction failed: %s", e)

    # Approach 3: Structural clustering + cluster field extractor
    #   Evaluate ALL cluster groups and pick the best by quality score
    try:
        from mcp.server.tools.structural_clustering import find_repeating_dom_groups
        from mcp.server.tools.cluster_field_extractor import extract_and_score_best_cluster

        dom_clusters = find_repeating_dom_groups(html, max_clusters=5)
        logger.info("Structural clustering: found %d cluster group(s)", len(dom_clusters))

        # Determine result limit from intent (if available)
        result_limit = None
        if intent:
            result_limit = intent.get("limit") or intent.get("result", {}).get("limit")

        if dom_clusters:
            cluster_items, quality_score = extract_and_score_best_cluster(
                dom_clusters,
                max_items=result_limit,
            )

            if len(cluster_items) >= 2 and quality_score > 0.05:
                density = len(cluster_items) / max(_count_discoverable(field_discovery), 1)
                conf = 0.70 * min(density, 1.0)
                multi_results.append(("structural_clustering", cluster_items, conf))
                logger.info(
                    "Structural clustering: best group has %d items "
                    "(quality=%.3f, conf=%.2f)",
                    len(cluster_items), quality_score, conf,
                )
    except ImportError:
        logger.debug("structural_clustering not available")
    except Exception as e:
        logger.warning("Structural clustering failed: %s", e)

    if not multi_results:
        return None

    # Pick best  by confidence then item count
    multi_results.sort(key=lambda r: (r[2], len(r[1])), reverse=True)
    best_method, best_items, best_conf = multi_results[0]

    # Add source metadata
    for it in best_items:
        it["source_url"] = ""
        it["source_site"] = ""

    all_fields_in_items = set()
    for it in best_items:
        all_fields_in_items.update(
            k for k, v in it.items()
            if v is not None and k not in ("source_url", "source_site")
        )

    return {
        "success": True,
        "url": "",
        "is_multi_item": True,
        "extracted_items": best_items,
        "extracted_data": best_items[0] if best_items else {},
        "items_count": len(best_items),
        "extraction_method": best_method,
        "confidence": round(best_conf, 4),
        "smart_completeness": round(
            len(all_fields_in_items) / max(_count_discoverable(field_discovery), 1), 4
        ),
        "fields_extracted": len(all_fields_in_items),
        "fields_available": _count_discoverable(field_discovery),
        "available_field_names": sorted(all_fields_in_items),
        "missing_fields": [],
        "low_confidence": best_conf < 0.40,
        "engines_evaluated": len(multi_results),
        "all_engine_scores": {r[0]: round(r[2], 4) for r in multi_results},
    }


def _extract_jsonld_multi_items(jsonld_items: list) -> List[Dict[str, Any]]:
    """
    Extract multiple items from JSON-LD data.
    Handles ItemList with listElement and multiple top-level typed objects.
    Generic — no type filtering.
    """
    if not jsonld_items:
        return []

    extracted = []

    # Path 1: ItemList with listElement
    for item in jsonld_items:
        if isinstance(item, dict) and item.get("@type") == "ItemList":
            list_elements = item.get("itemListElement", [])
            for le in list_elements:
                nested = le.get("item", le) if isinstance(le, dict) else le
                if isinstance(nested, dict):
                    clean = {
                        k: v for k, v in nested.items()
                        if not k.startswith("@") and v is not None
                    }
                    if clean:
                        pos = le.get("position") if isinstance(le, dict) else None
                        if pos:
                            clean["_position"] = int(pos)
                        extracted.append(clean)

    if len(extracted) >= 2:
        return extracted

    # Path 2: Multiple objects with same @type at top level
    type_groups = {}
    for item in jsonld_items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("@type", "")
        if isinstance(item_type, list):
            item_type = item_type[0] if item_type else ""
        if item_type:
            if item_type not in type_groups:
                type_groups[item_type] = []
            type_groups[item_type].append(item)

    # Find the largest group with >= 2 items
    for type_name, items in sorted(type_groups.items(), key=lambda x: len(x[1]), reverse=True):
        if len(items) >= 2:
            for idx, item in enumerate(items):
                clean = {
                    k: v for k, v in item.items()
                    if not k.startswith("@") and v is not None
                }
                if clean:
                    clean["_position"] = idx + 1
                    extracted.append(clean)
            break

    return extracted


def _extract_microdata_multi(html: str) -> List[Dict[str, Any]]:
    """
    Extract multiple items from Microdata — finds all itemscope blocks
    with the same itemtype and extracts their itemprop fields.
    Generic — no type filtering.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Find all top-level itemscope elements
    scopes = soup.find_all(attrs={"itemscope": True, "itemtype": True})
    if len(scopes) < 2:
        return []

    # Group by itemtype
    type_groups = {}
    for scope in scopes:
        item_type = scope.get("itemtype", "")
        if item_type not in type_groups:
            type_groups[item_type] = []
        type_groups[item_type].append(scope)

    # Use the largest group
    items = []
    for type_name, group_scopes in sorted(type_groups.items(), key=lambda x: len(x[1]), reverse=True):
        if len(group_scopes) < 2:
            continue

        for idx, scope in enumerate(group_scopes):
            item_data = {}
            for el in scope.find_all(attrs={"itemprop": True}):
                prop = el.get("itemprop")
                value = _extract_element_value(el)
                if prop and value:
                    item_data[prop] = value

            if item_data:
                item_data["_position"] = idx + 1
                items.append(item_data)
        break

    return items


def _extract_from_fragment(fragment_html: str) -> dict:
    """
    Extract fields from a single HTML fragment (one cluster item).
    Runs all generic engines on the fragment.
    """
    if not fragment_html or len(fragment_html.strip()) < 10:
        return {}

    result = {}

    # Run engines on fragment
    heuristic = extract_heuristic_patterns(fragment_html)
    if heuristic:
        result.update(heuristic)

    microdata = extract_microdata(fragment_html)
    if microdata:
        for k, v in microdata.items():
            if k not in result and v is not None:
                result[k] = v

    og = extract_opengraph(fragment_html)
    if og:
        for k, v in og.items():
            if k not in result and v is not None:
                result[k] = v

    jsonld = _parse_jsonld_blocks(fragment_html)
    if jsonld:
        for item in jsonld:
            if isinstance(item, dict):
                for k, v in item.items():
                    if not k.startswith("@") and k not in result and v is not None:
                        result[k] = v

    # Try text-based title extraction as fallback
    if not result.get("_page_title") and not result.get("name"):
        try:
            from bs4 import BeautifulSoup
            frag_soup = BeautifulSoup(fragment_html, "html.parser")
            for tag in ["h2", "h3", "h1", "h4", "a"]:
                el = frag_soup.find(tag)
                if el:
                    txt = el.get_text(strip=True)
                    if txt and 2 < len(txt) < 200:
                        result["_fragment_title"] = txt
                        break
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════
# Debug Output Storage
# ═══════════════════════════════════════════

def _save_extraction_debug(
    input_data: dict, url: str,
    field_discovery: dict, engine_outputs: dict,
    final_fields: dict,
):
    """
    Save extraction debug JSON to dom_signals/ folder.
    Includes field_discovery, engine_outputs, and final_fields.
    """
    try:
        request_id = input_data.get("request_id", "")
        domain = input_data.get("domain", "unknown")
        site_domain = urlparse(url).netloc if url else "unknown"

        if not request_id:
            return

        # Sanitize engine outputs for JSON storage (remove large HTML)
        sanitized_outputs = {}
        for engine_name, data in engine_outputs.items():
            sanitized_outputs[engine_name] = {
                "confidence": data.get("confidence", 0),
                "fields_count": data.get("fields_count", 0),
                "field_keys": sorted(data.get("fields", {}).keys()),
            }

        debug_data = {
            "request_id": request_id,
            "site_domain": site_domain,
            "url": url,
            "field_discovery": field_discovery,
            "engine_outputs": sanitized_outputs,
            "final_fields": {
                k: (v if not isinstance(v, str) or len(v) <= 200 else v[:200] + "...")
                for k, v in final_fields.items()
            } if isinstance(final_fields, dict) else {},
        }

        os.makedirs(DOM_SIGNALS_DIR, exist_ok=True)
        debug_file = os.path.join(
            DOM_SIGNALS_DIR,
            f"{request_id}_{site_domain}_extraction.json",
        )
        with open(debug_file, "w", encoding="utf-8") as f:
            json.dump(debug_data, f, indent=2, default=str)

    except Exception as e:
        logger.debug("Failed to save extraction debug: %s", e)
