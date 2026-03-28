"""
Deterministic pre-normalization before AI unification (Step 12a).

Normalizes values across candidates, resolves obvious conflicts
deterministically using site trust weights, and only passes
remaining genuine conflicts to the AI unification agent.

This reduces AI workload and saves AI calls when sources agree.
"""

import re
import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("umsa.pre_unification")

# Default weight for sites not in domain config
DEFAULT_WEIGHT = 0.50


def pre_normalize(
    candidates: List[Dict[str, Any]],
    site_trust_weights: Dict[str, float] = None,
) -> List[Dict[str, Any]]:
    """
    Normalize field values across all candidates before unification.
    - Converts string years to int
    - Extracts numeric ratings from strings
    - Strips whitespace from titles
    - Assigns site trust weights (from DB config via coordinator)
    """
    weights = site_trust_weights or {}

    for c in candidates:
        fields = c.get("extracted_data", c.get("extracted_fields", {}))
        if not isinstance(fields, dict):
            continue

        # Normalize year: "2010" → 2010
        if "year" in fields and isinstance(fields["year"], str):
            try:
                fields["year"] = int(fields["year"])
            except (ValueError, TypeError):
                pass

        # Normalize rating: "8.8/10" → 8.8, "93%" → leave (different scale)
        if "rating" in fields and isinstance(fields["rating"], str):
            rating_str = fields["rating"]
            # Only convert X/10 format
            m = re.match(r"^(\d+\.?\d*)\s*/\s*10$", rating_str.strip())
            if m:
                fields["rating"] = float(m.group(1))
            else:
                # Try simple numeric extraction (not percentage)
                m2 = re.match(r"^(\d+\.?\d*)$", rating_str.strip())
                if m2:
                    val = float(m2.group(1))
                    if val <= 10:
                        fields["rating"] = val

        # Normalize title: strip whitespace, fix inconsistent casing
        if "title" in fields and isinstance(fields["title"], str):
            fields["title"] = fields["title"].strip()

        # Normalize director
        if "director" in fields and isinstance(fields["director"], str):
            fields["director"] = fields["director"].strip()

        # Apply trust weight from DB config (not hardcoded)
        site = c.get("source_site", c.get("site_url", ""))
        clean_site = site.lower().replace("www.", "").replace("https://", "").replace("http://", "").rstrip("/")
        c["source_weight"] = weights.get(clean_site, DEFAULT_WEIGHT)

        # Ensure we have a consistent key for extracted data
        if "extracted_fields" not in c and "extracted_data" in c:
            c["extracted_fields"] = c["extracted_data"]

    return candidates


def deterministic_dedup(
    candidates: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Resolve obvious duplicates/conflicts without AI.

    Returns:
        unified: dict of field → value (best guess for all fields)
        conflicts: list of field names that have genuine conflicts
                   (need AI to resolve)
    """
    unified = {}
    conflicts = []

    # Collect all field values across candidates
    field_values: Dict[str, List[Tuple[Any, float, str]]] = {}
    for c in candidates:
        fields = c.get("extracted_fields", c.get("extracted_data", {}))
        if not isinstance(fields, dict):
            continue
        source = c.get("source_site", c.get("site_url", "unknown"))
        weight = c.get("source_weight", DEFAULT_WEIGHT)

        for field, value in fields.items():
            if value is not None and field not in ("source_url", "source_site"):
                field_values.setdefault(field, []).append(
                    (value, weight, source)
                )

    for field, values in field_values.items():
        if len(values) == 1:
            # Only one source — use it directly
            unified[field] = values[0][0]
        elif _all_equal(v[0] for v in values):
            # All sources agree — no conflict
            unified[field] = values[0][0]
        elif _all_equal_normalized(v[0] for v in values):
            # All sources agree after normalization
            unified[field] = values[0][0]
        else:
            # Real conflict — try to resolve by trust weight
            values.sort(key=lambda x: x[1], reverse=True)
            weight_gap = values[0][1] - values[1][1]

            if weight_gap >= 0.1:
                # Clear winner by trust weight — resolve deterministically
                unified[field] = values[0][0]
                logger.info(
                    "Field '%s' resolved by trust weight: %s (%.2f) > %s (%.2f)",
                    field, values[0][2], values[0][1],
                    values[1][2], values[1][1],
                )
            else:
                # Genuine conflict — needs AI
                conflicts.append(field)
                # Use highest-weight value as temporary placeholder
                unified[field] = values[0][0]
                logger.info(
                    "Field '%s' has GENUINE CONFLICT: %s",
                    field,
                    [(v[0], v[2]) for v in values[:3]],
                )

    return unified, conflicts


def _all_equal(iterable) -> bool:
    """Check if all items in iterable are equal."""
    items = list(iterable)
    if not items:
        return True
    first = items[0]
    return all(item == first for item in items[1:])


def _all_equal_normalized(iterable) -> bool:
    """Check if all items are equal after normalization."""
    items = list(iterable)
    if not items:
        return True

    def normalize(val):
        if isinstance(val, str):
            return val.lower().strip()
        if isinstance(val, (int, float)):
            return float(val)
        return val

    first = normalize(items[0])
    return all(normalize(item) == first for item in items[1:])
