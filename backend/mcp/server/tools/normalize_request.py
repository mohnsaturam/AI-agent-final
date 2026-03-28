"""
MCP Tool — normalize_request (Step 1)

Sanitize input, strip HTML/binary, normalize casing.
Extract metadata (language hints, site list).
Generate request_id, bind user_id.
Persist INIT state.
"""

import json
import logging
import re
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.normalize_request")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Normalize incoming request data."""
    input_data = context.input_data
    query = input_data.get("query", "")
    domain = input_data.get("domain", "")
    sites = input_data.get("sites", [])
    user_id = input_data.get("user_id", "")

    # Strip HTML tags
    cleaned_query = re.sub(r"<[^>]+>", "", query)

    # Strip binary/non-printable characters
    cleaned_query = re.sub(r"[^\x20-\x7E\u00A0-\uFFFF]", "", cleaned_query)

    # Normalize whitespace
    cleaned_query = re.sub(r"\s+", " ", cleaned_query).strip()

    # Normalize casing for domain
    normalized_domain = domain.lower().strip()

    # Normalize sites
    normalized_sites = []
    for site in sites:
        s = site.lower().strip().replace("www.", "")
        if s and "." in s:
            normalized_sites.append(s)

    # Extract language hints from query
    language_hints = []
    lang_keywords = {
        "hindi": "hi", "tamil": "ta", "telugu": "te",
        "malayalam": "ml", "english": "en", "kannada": "kn",
        "bengali": "bn", "marathi": "mr", "gujarati": "gu",
    }
    query_lower = cleaned_query.lower()
    for lang, code in lang_keywords.items():
        if lang in query_lower:
            language_hints.append({"language": lang, "code": code})

    return {
        "normalized_query": cleaned_query,
        "domain": normalized_domain,
        "sites": normalized_sites,
        "user_id": user_id,
        "language_hints": language_hints,
        "metadata": {
            "original_length": len(query),
            "normalized_length": len(cleaned_query),
            "sites_count": len(normalized_sites),
        },
    }
