"""
MCP Tool — cluster_field_extractor

Domain-agnostic field extraction from HTML cluster fragments.

Structural clustering detects repeating DOM blocks (cards, rows, tiles),
but the fragments lack metadata (<script type="application/ld+json">,
<meta property="og:...">) so the standard engines return nothing.

This module reads the VISIBLE DOM elements inside each fragment:
  - Headings (h1-h6)
  - Links (<a> text + href)
  - Images (<img> src + alt)
  - Text spans with numbers
  - Inline itemprop attributes
  - Label:Value text patterns

ALL field names are structural (_heading, _primary_link, _numeric_values).
Zero domain keywords. Zero hardcoded field names.
The semantic layer (Groq AI) maps these to domain names later.
"""

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger("umsa.tools.cluster_field_extractor")


def extract_fields_from_cluster(
    fragment_html_list: List[str],
    max_items: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Extract structured fields from a list of HTML cluster fragments.

    Args:
        fragment_html_list: List of HTML strings, one per cluster item.
        max_items: Optional limit on number of items to return.

    Returns:
        List of dictionaries, each containing domain-agnostic fields
        extracted from one fragment. Empty/unresolvable fragments
        are filtered out.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("BeautifulSoup not installed — cluster extraction skipped")
        return []

    items = []
    for idx, frag_html in enumerate(fragment_html_list):
        if not frag_html or len(frag_html.strip()) < 10:
            continue

        fields = _extract_fragment_fields(frag_html, idx + 1)

        # Filter: must have at least a heading or a link text
        if not fields:
            continue
        if not fields.get("_heading") and not fields.get("_primary_link_text"):
            continue

        items.append(fields)

    logger.info(
        "Cluster field extractor: %d/%d fragments yielded fields",
        len(items), len(fragment_html_list),
    )

    # Limit results if requested
    if max_items and max_items > 0 and len(items) > max_items:
        items = items[:max_items]
        logger.info("Truncated to %d items (intent limit)", max_items)

    return items


def score_cluster_quality(items: List[Dict[str, Any]]) -> float:
    """
    Score the quality of an extracted cluster group (0.0 – 1.0).

    Domain-agnostic criteria:
      1. Fragment count — more items = more likely real content
      2. Field richness — real cards have headings + images + numbers
      3. Content diversity — nav bars have short, similar text
      4. Average text length — content cards are longer than nav links
    """
    if not items:
        return 0.0

    n = len(items)

    # ── 1. Fragment count score (0-0.25) ──
    # 2 items → 0.05, 5 → 0.125, 10+ → 0.25
    count_score = min(n / 10.0, 1.0) * 0.25

    # ── 2. Field richness (0-0.35) ──
    # Count how many structural fields each item has beyond _position
    richness_keys = (
        "_heading", "_primary_link_text", "_primary_image_src",
        "_numeric_values", "_labeled_pairs", "_itemprop_fields",
    )
    total_richness = 0
    for item in items:
        item_richness = sum(1 for k in richness_keys if item.get(k))
        total_richness += item_richness
    avg_richness = total_richness / n
    # 1 field → 0.06, 3 fields → 0.175, 6 fields → 0.35
    richness_score = min(avg_richness / 6.0, 1.0) * 0.35

    # ── 3. Content diversity (0-0.20) ──
    # If all headings are the same or very short, it's likely a nav bar
    headings = [
        (item.get("_heading") or item.get("_primary_link_text") or "")
        for item in items
    ]
    unique_headings = set(h.strip().lower() for h in headings if h.strip())
    if n > 0 and len(unique_headings) > 0:
        diversity_ratio = len(unique_headings) / n
    else:
        diversity_ratio = 0.0
    diversity_score = diversity_ratio * 0.20

    # ── 4. Average heading length (0-0.20) ──
    # Nav links: ~10 chars. Content cards: 20-80 chars.
    heading_lengths = [len(h) for h in headings if h]
    avg_heading_len = sum(heading_lengths) / max(len(heading_lengths), 1)
    # <8 chars → likely nav; 30+ chars → real content
    length_score = min(max(avg_heading_len - 5, 0) / 30.0, 1.0) * 0.20

    total = count_score + richness_score + diversity_score + length_score

    logger.debug(
        "Cluster quality: n=%d count=%.3f richness=%.3f diversity=%.3f "
        "length=%.3f total=%.3f",
        n, count_score, richness_score, diversity_score, length_score, total,
    )

    return round(total, 4)


def extract_and_score_best_cluster(
    all_cluster_groups: List[List[str]],
    max_items: Optional[int] = None,
) -> tuple:
    """
    Extract fields from ALL cluster groups, score each, return the best.

    Returns:
        (items, quality_score) — best cluster's items and score.
        Returns ([], 0.0) if no valid cluster found.
    """
    best_items: List[Dict[str, Any]] = []
    best_score = 0.0

    for ci, cluster_html_list in enumerate(all_cluster_groups):
        items = extract_fields_from_cluster(cluster_html_list, max_items=None)

        if len(items) < 2:
            continue

        quality = score_cluster_quality(items)
        logger.info(
            "Cluster group %d: %d items, quality=%.3f", ci, len(items), quality,
        )

        if quality > best_score:
            best_score = quality
            best_items = items

    # Apply limit after selecting best cluster
    if max_items and max_items > 0 and len(best_items) > max_items:
        best_items = best_items[:max_items]

    return best_items, best_score


def _extract_fragment_fields(html: str, position: int) -> Dict[str, Any]:
    """
    Extract domain-agnostic fields from a single HTML fragment.

    Returns a dict with structural field names:
      _position      : ordinal position in the cluster
      _heading       : first heading text (h1-h6)
      _primary_link_text : text of first meaningful <a>
      _primary_link_href : href of first meaningful <a>
      _primary_image_src : src of first <img>
      _primary_image_alt : alt text of first <img>
      _all_images    : list of {src, alt} for all images
      _numeric_values: list of standalone numbers found
      _text_snippets : list of non-trivial text nodes
      _labeled_pairs : dict of Label→Value from "Label: Value" patterns
      _itemprop_fields : dict of itemprop→value from inline microdata
    """
    from bs4 import BeautifulSoup, Tag

    soup = BeautifulSoup(html, "html.parser")
    fields: Dict[str, Any] = {"_position": position}

    # ── 1. Heading extraction (first h1-h6) ──
    for tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        el = soup.find(tag_name)
        if el:
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 300:
                fields["_heading"] = text
                break

    # ── 2. Primary link extraction ──
    links = soup.find_all("a", href=True)
    for link in links:
        text = link.get_text(strip=True)
        href = link.get("href", "")
        if text and len(text) > 2 and href and not href.startswith("#"):
            fields["_primary_link_text"] = text
            fields["_primary_link_href"] = href
            break

    # ── 3. If no heading found, try link text or img alt as fallback ──
    if "_heading" not in fields and "_primary_link_text" in fields:
        fields["_heading"] = fields["_primary_link_text"]

    # ── 4. Image extraction ──
    images = soup.find_all("img")
    all_images = []
    for img in images:
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        alt = img.get("alt", "").strip()
        if src and len(src) > 5:
            img_entry = {"src": src}
            if alt:
                img_entry["alt"] = alt
            all_images.append(img_entry)

    if all_images:
        fields["_primary_image_src"] = all_images[0]["src"]
        if "alt" in all_images[0]:
            fields["_primary_image_alt"] = all_images[0]["alt"]
        fields["_all_images"] = all_images

    # If still no heading, try img alt
    if "_heading" not in fields and all_images:
        for img_entry in all_images:
            alt = img_entry.get("alt", "")
            if alt and len(alt) > 2:
                fields["_heading"] = alt
                break

    # ── 5. Collect all visible text nodes ──
    visible_text = soup.get_text(separator="\n", strip=True)
    text_lines = [
        line.strip() for line in visible_text.split("\n")
        if line.strip() and len(line.strip()) > 1
    ]
    if text_lines:
        fields["_text_snippets"] = text_lines[:20]  # cap at 20

    # ── 6. Numeric value extraction ──
    # Find all standalone numbers (integers and decimals)
    # These could be ratings, years, prices, counts — domain-agnostic
    numeric_pattern = re.compile(r'\b(\d{1,2}\.\d{1,2})\b|\b(\d{4})\b|\b(\d{1,6})\b')
    numeric_values = []
    for line in text_lines:
        for match in numeric_pattern.finditer(line):
            # decimal (e.g. 8.8, 7.5)
            if match.group(1):
                numeric_values.append(float(match.group(1)))
            # 4-digit (e.g. 2025, 1994)
            elif match.group(2):
                numeric_values.append(int(match.group(2)))
            # other integers
            elif match.group(3):
                val = int(match.group(3))
                if val > 0:  # skip zero
                    numeric_values.append(val)

    # Deduplicate while preserving order
    seen_nums = set()
    unique_nums = []
    for n in numeric_values:
        if n not in seen_nums:
            seen_nums.add(n)
            unique_nums.append(n)
    if unique_nums:
        fields["_numeric_values"] = unique_nums[:15]  # cap at 15

    # ── 7. Label:Value text patterns ──
    label_value_re = re.compile(
        r'^\s*([A-Za-z0-9 _-]{2,40})\s*:\s*(.+)$', re.MULTILINE
    )
    labeled_pairs = {}
    for m in label_value_re.finditer(visible_text):
        label = m.group(1).strip()
        value = m.group(2).strip()[:200]
        if label and value and len(value) > 1:
            key = label.lower().replace(" ", "_")
            if key not in labeled_pairs:
                labeled_pairs[key] = value
    if labeled_pairs:
        fields["_labeled_pairs"] = labeled_pairs

    # ── 8. Inline itemprop attributes ──
    itemprop_fields = {}
    for el in soup.find_all(attrs={"itemprop": True}):
        prop = el.get("itemprop")
        if not prop:
            continue
        # Extract value by tag type
        if el.name == "meta":
            val = el.get("content", "").strip()
        elif el.name == "img":
            val = el.get("src") or el.get("data-src") or ""
        elif el.name == "time":
            val = el.get("datetime") or el.get_text(strip=True)
        elif el.name == "link":
            val = el.get("href", "").strip()
        elif el.name == "data":
            val = el.get("value") or el.get_text(strip=True)
        else:
            val = el.get_text(strip=True)

        if val and prop not in itemprop_fields:
            itemprop_fields[prop] = val

    if itemprop_fields:
        fields["_itemprop_fields"] = itemprop_fields

    # ── 9. Data attributes (data-*) ──
    data_attrs = {}
    for el in soup.find_all(True):
        for attr_name, attr_val in el.attrs.items():
            if isinstance(attr_name, str) and attr_name.startswith("data-"):
                clean_name = attr_name[5:]  # strip "data-" prefix
                if (
                    isinstance(attr_val, str)
                    and attr_val
                    and len(attr_val) < 200
                    and clean_name not in data_attrs
                    and clean_name not in (
                        "reactid", "testid", "qa", "tracking",
                        "gtm", "analytics", "component",
                    )
                ):
                    data_attrs[clean_name] = attr_val
        if len(data_attrs) >= 10:  # cap collection
            break
    if data_attrs:
        fields["_data_attributes"] = data_attrs

    return fields
