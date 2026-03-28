"""
Multi-Item Unifier — Cross-site Deduplication

Domain-agnostic title-based deduplication of items returned by
multiple sites during multi-item extraction.

Logic:
  1. Collect all items from all sites into a flat list
  2. Normalize each item's "title" using the field cascade
  3. Fuzzy-match titles using normalized lowercase + stripped punctuation
  4. For duplicates, merge fields (keep the richest version)
  5. Return a single deduplicated list with source_sites metadata

Zero AI calls — purely deterministic string matching.
"""

import logging
import re
import unicodedata
from typing import Any, Dict, List

logger = logging.getLogger("umsa.client.multi_item_unifier")

# ── Title field cascade (domain-agnostic) ──
TITLE_KEYS = ("_heading", "name", "title", "headline", "_primary_link_text")


def _get_title(item: Dict[str, Any]) -> str:
    """Extract the best title from an item using the field cascade."""
    for key in TITLE_KEYS:
        val = item.get(key)
        if val and isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _normalize_title(title: str) -> str:
    """
    Normalize a title for fuzzy comparison.

    Steps:
      - Unicode normalize (NFKD)
      - Lowercase
      - Strip HTML entities (&amp; etc)
      - Remove punctuation except spaces
      - Collapse whitespace
      - Strip leading/trailing whitespace
    """
    if not title:
        return ""

    # Unicode normalize
    t = unicodedata.normalize("NFKD", title)
    # Decode common HTML entities
    t = t.replace("&amp;", "&").replace("&apos;", "'").replace("&quot;", '"')
    t = re.sub(r"&#?\w+;", "", t)
    # Lowercase
    t = t.lower()
    # Remove parenthesized year suffixes like "(2010)" — these are metadata, not title
    t = re.sub(r"\(\d{4}\)\s*$", "", t)
    # Remove punctuation (keep letters, digits, spaces)
    t = re.sub(r"[^\w\s]", "", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    return t


def _count_fields(item: Dict[str, Any]) -> int:
    """Count meaningful (non-None, non-empty) fields in an item."""
    skip_keys = {"source_url", "source_site", "_source_sites", "_position"}
    count = 0
    for k, v in item.items():
        if k in skip_keys:
            continue
        if v is not None and v != "" and v != []:
            count += 1
    return count


def unify_multi_items(
    sites_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Unify and deduplicate items across all sites.

    Args:
        sites_data: Dict of site_domain → {"items": [...], "items_count": int, ...}

    Returns:
        List of deduplicated item dicts, each with _source_sites metadata.
    """
    # ── Step 1: Collect all items with source site annotation ──
    all_items: List[Dict[str, Any]] = []

    for site, site_info in sites_data.items():
        items = site_info.get("items", [])
        for item in items:
            item_copy = dict(item)
            item_copy["_source_sites"] = [site]
            all_items.append(item_copy)

    if not all_items:
        return []

    logger.info("Multi-item unifier: %d total items from %d sites",
                len(all_items), len(sites_data))

    # ── Step 2: Group by normalized title ──
    title_groups: Dict[str, List[Dict[str, Any]]] = {}

    for item in all_items:
        title = _get_title(item)
        norm = _normalize_title(title)

        if not norm:
            # No title — keep as unique (won't match anything)
            unique_key = f"__notitle_{id(item)}"
            title_groups[unique_key] = [item]
            continue

        if norm in title_groups:
            title_groups[norm].append(item)
        else:
            title_groups[norm] = [item]

    # ── Step 3: Merge duplicates — keep richest version ──
    deduplicated: List[Dict[str, Any]] = []

    for norm_title, group in title_groups.items():
        if len(group) == 1:
            deduplicated.append(group[0])
            continue

        # Multiple items with the same title — merge
        # Pick the richest item as base
        group.sort(key=lambda x: _count_fields(x), reverse=True)
        merged = dict(group[0])  # richest version

        # Collect all source sites
        all_sources = set()
        for item in group:
            for s in item.get("_source_sites", []):
                all_sources.add(s)
        merged["_source_sites"] = sorted(all_sources)

        # Fill in any fields from other items that the base is missing
        for item in group[1:]:
            for k, v in item.items():
                if k in ("_source_sites", "_position"):
                    continue
                if k not in merged or merged[k] is None or merged[k] == "":
                    merged[k] = v

        deduplicated.append(merged)

    removed = len(all_items) - len(deduplicated)
    logger.info(
        "Multi-item unifier: %d items → %d unique (%d duplicates removed)",
        len(all_items), len(deduplicated), removed,
    )

    return deduplicated

