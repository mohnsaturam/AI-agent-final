"""
MCP Tool — compute_intent_hash (Step 4a)

SHA256(normalized_intent + domain + schema_version)

IMPORTANT: Strips volatile/runtime fields AND normalizes query_type
so that semantically identical queries produce the SAME hash even when:
  - raw query strings differ ("movies released in 2013" vs "movies from 2013")
  - AI classifies query_type inconsistently ("list" vs "search")
"""

import hashlib
import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.compute_intent_hash")

# Fields injected at runtime that should NOT affect the hash
# These vary between runs even for semantically identical queries
VOLATILE_FIELDS = {
    "raw_query",           # The original user query text (varies for same intent)
    "_is_single_entity",   # Runtime flag set by orchestrator
    "_expanded_queries",   # Groq-generated query variants (non-deterministic)
    "query_type",          # AI classifies inconsistently ("list" vs "search")
}


async def execute(context, db_pool) -> Dict[str, Any]:
    """Compute idempotency-aware intent hash, ignoring volatile fields."""
    input_data = context.input_data
    parsed_intent = input_data.get("parsed_intent", {})
    domain = input_data.get("domain", "")
    schema_version = input_data.get("schema_version", "")

    # Strip volatile fields that would cause different hashes
    # for semantically identical queries
    stable_intent = {
        k: v for k, v in parsed_intent.items()
        if k not in VOLATILE_FIELDS
    }

    # Normalize intent to deterministic string
    normalized_intent = json.dumps(stable_intent, sort_keys=True, default=str)

    # SHA256(normalized_intent + domain + schema_version)
    raw = f"{normalized_intent}|{domain}|{schema_version}"
    intent_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    stripped_count = len(set(parsed_intent.keys()) & VOLATILE_FIELDS)
    logger.info(
        "Intent hash computed: %s (stripped %d volatile fields, stable keys: %s)",
        intent_hash[:16], stripped_count, sorted(stable_intent.keys()),
    )

    return {
        "intent_hash": intent_hash,
        "normalized_intent": normalized_intent,
        "domain": domain,
        "schema_version": schema_version,
    }
