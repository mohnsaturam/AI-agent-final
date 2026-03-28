"""
MCP Tool — check_url_pattern_cache (Step 8-CACHE) — REVISED

MEMORY-ONLY. No hardcoded deterministic templates.
Checks DB cache for previously successful URL patterns.
Validates cached URLs against CURRENT robots.txt allowed paths.
If cached URL path is now disallowed → mark as "unstable" and return MISS.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlparse

logger = logging.getLogger("umsa.tools.check_url_pattern_cache")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Check URL pattern cache (memory only) with robots.txt validation."""
    input_data = context.input_data
    domain = input_data.get("domain", "")
    site_domain = input_data.get("site_domain", "")
    schema_version = input_data.get("schema_version", "")
    intent = input_data.get("intent", {})
    robots_rules = input_data.get("robots_rules", {})  # current rules from Step 7

    # Normalize
    clean_domain = site_domain.lower().replace("www.", "")

    # Normalize intent title for entity-aware cache key
    intent_title = _normalize_title(intent.get("title", ""))

    # Current allowed and disallowed paths from fresh robots.txt
    allowed_paths = robots_rules.get("allowed", [])
    disallowed_paths = robots_rules.get("disallowed", [])

    # ═══ Step 1: Check DB for cached URL patterns (entity-aware key) ═══
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, pattern, pattern_type, confidence, metadata
                FROM umsa_core.url_patterns
                WHERE domain = $1
                  AND site_domain = $2
                  AND schema_version = $3
                  AND status = 'valid'
                  AND expires_at > now()
                  AND confidence > 0.5
                  AND (metadata->>'entity_title') = $4
                ORDER BY confidence DESC
                LIMIT 5
                """,
                domain,
                clean_domain,
                schema_version,
                intent_title,
            )
    except Exception as e:
        logger.warning("Failed to query url_patterns cache: %s", e)
        rows = []

    # ═══ Step 2: Validate each cached URL against current robots.txt ═══
    for row in rows:
        cached_url = row["pattern"]
        url_path = urlparse(cached_url).path
        cached_meta = row["metadata"] if isinstance(row["metadata"], dict) else {}

        # Check if this URL path is DISALLOWED by current robots.txt
        is_disallowed = False
        blocking_path = ""
        for blocked_path in disallowed_paths:
            if blocked_path and url_path.startswith(blocked_path):
                is_disallowed = True
                blocking_path = blocked_path
                break

        if is_disallowed:
            # ═══ Path was allowed before but is NOW disallowed ═══
            logger.warning(
                "Cached URL %s uses path '%s' which is NOW DISALLOWED by '%s'. "
                "Marking as unstable.",
                cached_url, url_path, blocking_path,
            )
            # Mark as unstable/invalidated in DB
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE umsa_core.url_patterns
                        SET status = 'invalidated',
                            metadata = metadata || $1::jsonb
                        WHERE id = $2
                        """,
                        json.dumps({
                            "invalidation_reason": "path_now_disallowed",
                            "disallowed_path": blocking_path,
                            "url_path": url_path,
                            "detected_at": datetime.now().isoformat(),
                        }),
                        row["id"],
                    )
            except Exception as e:
                logger.warning("Failed to mark URL pattern as unstable: %s", e)

            continue  # Skip this URL, try next cached one

        # ═══ Path is still allowed — validate intent relevance ═══
        # Check that cached URL matches current intent's key parameters
        # (title, year, language — cached URL for Se7en should NOT match Interstellar)
        if not _url_matches_intent(cached_url, intent, cached_meta):
            logger.info(
                "Cached URL %s doesn't match current intent — skipping",
                cached_url,
            )
            continue

        logger.info(
            "URL pattern cache HIT for %s: %s (path validated against robots.txt)",
            clean_domain, cached_url,
        )
        return {
            "cache_hit": True,
            "url": cached_url,
            "pattern_type": row["pattern_type"],
            "confidence": float(row["confidence"]),
            "source": "db_cache",
            "robots_validated": True,
        }

    # ═══ Step 3: Cache MISS — AI URL generation needed ═══
    logger.info(
        "URL pattern cache MISS for %s (no valid cached URLs or all invalidated)",
        clean_domain,
    )
    return {
        "cache_hit": False,
        "site_domain": clean_domain,
        "allowed_paths": allowed_paths,
        "disallowed_paths": disallowed_paths,
    }


def _normalize_title(title: str) -> str:
    """Normalize a title for cache key: lowercase, trim, strip punctuation."""
    import re
    t = (title or "").strip().lower()
    # Strip non-alphanumeric except spaces
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return t.strip()


def _url_matches_intent(url: str, intent: dict, cached_meta: dict = None) -> bool:
    """
    Check if a cached URL is relevant to the current intent.
    Validates: title (primary), year, language.
    Returns True if compatible, False if the URL was for a different query.
    """
    import re
    from urllib.parse import parse_qs

    url_lower = url.lower()
    parsed = urlparse(url)
    params = parse_qs(parsed.query.lower())
    cached_meta = cached_meta or {}

    # ── Check title relevance (PRIMARY — prevents stale cross-entity cache hits) ──
    intent_title = intent.get("title")
    if intent_title:
        cached_entity_title = cached_meta.get("entity_title", "")
        if cached_entity_title:
            # Both titles exist → compare normalized forms
            if _normalize_title(intent_title) != _normalize_title(cached_entity_title):
                logger.debug(
                    "Title mismatch: intent='%s' vs cached='%s' — rejecting %s",
                    intent_title, cached_entity_title, url,
                )
                return False
        # If no entity_title in metadata (legacy entry), fall through to other checks

    # ── Check year relevance ──
    intent_year = intent.get("year")
    if intent_year:
        year_str = str(intent_year)
        if year_str not in url_lower:
            # Cached URL doesn't mention the intent year at all
            return False

    # ── Check language relevance ──
    intent_lang = (intent.get("language") or "").lower()
    filters = intent.get("filters") or {}
    filter_lang = (filters.get("language") or "").lower()
    lang = intent_lang or filter_lang

    if lang:
        # Map common language names to ISO codes used in URLs
        lang_code_map = {
            "hindi": "hi", "english": "en", "tamil": "ta", "telugu": "te",
            "malayalam": "ml", "kannada": "kn", "bengali": "bn", "marathi": "mr",
            "french": "fr", "spanish": "es", "german": "de", "japanese": "ja",
            "korean": "ko", "chinese": "zh",
        }
        lang_code = lang_code_map.get(lang, lang)

        # Check if the URL contains the language code OR the language name
        if lang not in url_lower and lang_code not in url_lower:
            return False

    return True
