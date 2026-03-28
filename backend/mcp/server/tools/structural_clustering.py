"""
MCP Tool — structural_clustering (Phase 3)

DOM-based repeating pattern detection for list/discovery pages.
Zero selectors. Zero class names. Zero field extraction.

ONLY job: find repeating sibling groups in the DOM and return their HTML.
Field extraction is handled by the caller (scrape_structured_data.py)
using the same engines used for single-item extraction.

Algorithm:
  1. Parse DOM tree
  2. Detect repeating sibling groups via 2-level subtree similarity
  3. Score clusters by: item count × average text length (richest data wins)
  4. Return top clusters as lists of HTML strings
"""

import logging
from typing import List
from statistics import mean

logger = logging.getLogger("umsa.tools.structural_clustering")


def find_repeating_dom_groups(html: str, max_clusters: int = 3) -> List[List[str]]:
    """
    Find repeating DOM patterns and return their HTML fragments.

    Returns a list of clusters (up to max_clusters).
    Each cluster is a list of HTML strings (one per repeating item).
    The caller is responsible for field extraction using its engines.

    Clusters are sorted by quality: deepest DOM level, then by richness
    (item count × average text length).
    """
    try:
        from bs4 import BeautifulSoup, Tag
    except ImportError:
        logger.warning("BeautifulSoup not installed — clustering skipped")
        return []

    soup = BeautifulSoup(html, "html.parser")
    body = soup.find("body") or soup

    # Step 1: Find candidate clusters (repeating sibling groups)
    candidate_clusters = _find_repeating_groups(body)

    if not candidate_clusters:
        logger.debug("No repeating DOM groups found")
        return []

    logger.debug("Found %d raw candidate clusters", len(candidate_clusters))

    # Step 2: Score and rank clusters by data richness
    scored = []
    for cluster in candidate_clusters:
        text_lengths = [len(el.get_text(strip=True)) for el in cluster]
        avg_text = mean(text_lengths) if text_lengths else 0

        # Skip clusters where items have very little text (< 10 chars avg)
        if avg_text < 10:
            continue

        # Quality score: depth (closer to content) × count × avg text
        depth = _avg_depth(cluster)
        quality = depth * len(cluster) * avg_text

        scored.append((quality, cluster))

    if not scored:
        logger.debug("No clusters with sufficient text content")
        return []

    # Sort by quality descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # Step 3: Convert top clusters to HTML strings
    result = []
    for _, cluster in scored[:max_clusters]:
        html_fragments = [str(el) for el in cluster]
        result.append(html_fragments)
        logger.debug(
            "Cluster: %d items, avg_text=%d chars, sample='%s'",
            len(html_fragments),
            int(mean(len(el.get_text(strip=True)) for el in cluster)),
            cluster[0].get_text(strip=True)[:60],
        )

    logger.info(
        "Returning %d cluster(s), largest has %d items",
        len(result), len(result[0]) if result else 0,
    )

    return result


def _find_repeating_groups(root) -> List[list]:
    """
    Walk DOM tree and find parent nodes whose children have similar subtree structure.
    Similarity is measured by tag distribution at 2 levels deep.
    """
    from bs4 import Tag

    clusters = []

    for parent in root.find_all(True):
        children = [c for c in parent.children if isinstance(c, Tag)]

        if len(children) < 3:
            continue

        # Skip non-content containers
        if parent.name in ("head", "script", "style", "noscript"):
            continue

        # Compare children subtree signatures (2-level deep)
        signatures = [_subtree_signature(c) for c in children]

        # Filter out empty/trivial signatures
        valid_pairs = [(sig, child) for sig, child in zip(signatures, children) if sig]

        if len(valid_pairs) < 3:
            continue

        # Find groups of similar signatures
        groups = _group_by_similarity(
            [p[0] for p in valid_pairs],
            [p[1] for p in valid_pairs],
        )

        for group in groups:
            if len(group) >= 3:
                clusters.append(group)

    # Prefer deepest clusters (closest to actual content), then by size
    if clusters:
        clusters.sort(key=lambda c: (_avg_depth(c), len(c)), reverse=True)

    return clusters


def _subtree_signature(el) -> str:
    """
    Generate a structural signature for a DOM subtree.
    Captures tag hierarchy at 2 LEVELS deep for better discrimination.
    Ignores content, classes, and attributes — purely structural.
    """
    from bs4 import Tag

    if not isinstance(el, Tag):
        return ""

    child_sigs = []
    for child in el.children:
        if isinstance(child, Tag):
            grandchild_tags = sorted(
                gc.name for gc in child.children if isinstance(gc, Tag)
            )
            gc_str = ",".join(grandchild_tags[:5])
            child_sigs.append(f"{child.name}[{gc_str}]")

    return f"{el.name}({','.join(sorted(child_sigs))})"


def _group_by_similarity(signatures: List[str], elements: list) -> List[list]:
    """Group elements by exact signature match. Returns lists with ≥3 members."""
    sig_groups = {}
    for sig, el in zip(signatures, elements):
        if sig not in sig_groups:
            sig_groups[sig] = []
        sig_groups[sig].append(el)

    return [group for group in sig_groups.values() if len(group) >= 3]


def _avg_depth(cluster: list) -> int:
    """Average depth of cluster elements in the DOM tree."""
    depths = []
    for el in cluster:
        depth = 0
        parent = el.parent
        while parent:
            depth += 1
            parent = parent.parent
        depths.append(depth)
    return int(mean(depths)) if depths else 0
