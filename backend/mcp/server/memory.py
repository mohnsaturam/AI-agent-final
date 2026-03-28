"""
MCP Server — Memory Configuration (Section 14)

Cache stage definitions for the Partial Reuse Matrix.
Each pipeline stage has independent cache validity checks
implemented in individual MCP tools:

  - intent:        check_intent_cache (Step 6)
                   Level 1: Exact hash match in intent_cache
                   Level 2: Exact hash match in unified_results
                   Level 3: Semantic fallback (filters + primary_goal)
                   Level 4: Semantic key match (normalized canonical key)
  - site_relevance: check_site_validation_cache (Step 7)
  - robots:        store_allowed_paths / site_pipeline (Step 7a-7c)
  - url_patterns:  check_url_pattern_cache (Step 8-CACHE)
  - unified:       check_intent_cache Level 2 (Step 6)

Schema version gating ensures no cross-version cache reuse.
"""

import logging

logger = logging.getLogger("umsa.memory")


# Cache stages mapped to their table and evaluation criteria
# Used as documentation reference — actual cache checks are in individual tools
CACHE_STAGES = {
    "intent": {
        "table": "umsa_core.intent_cache",
        "reuse_keys": ["domain", "intent_hash", "schema_version", "semantic_key"],
        "tool": "check_intent_cache",
        "levels": [
            "Level 1: Exact intent_hash match in intent_cache",
            "Level 2: Exact intent_hash match in unified_results (COMPLETED requests only)",
            "Level 3: Semantic fallback (exact filters + primary_goal JSON match)",
            "Level 4: Semantic key match (normalized canonical key across AI variations)",
        ],
    },
    "site_relevance": {
        "table": "umsa_core.site_relevance",
        "reuse_keys": ["domain", "intent_hash", "schema_version"],
        "tool": "check_site_validation_cache",
    },
    "robots": {
        "table": "umsa_core.robots_cache",
        "reuse_keys": ["site_domain"],
        "tool": "store_allowed_paths",
    },
    "url_patterns": {
        "table": "umsa_core.url_patterns",
        "reuse_keys": ["domain", "site_domain", "schema_version"],
        "tool": "check_url_pattern_cache",
    },
    "unified": {
        "table": "umsa_core.unified_results",
        "reuse_keys": ["domain", "intent_hash", "schema_version"],
        "tool": "check_intent_cache (Level 2)",
    },
}
