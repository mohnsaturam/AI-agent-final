-- Migration: Refresh tool_registry with all 26 tools
-- Run this ONCE before starting the backend

-- Clear old entries
DELETE FROM umsa_core.tool_registry;

-- Insert all 26 tools (20 deterministic + 6 AI agents)
INSERT INTO umsa_core.tool_registry (tool_name, role, allowed_callers, domain_scope, input_schema, timeout_seconds, retry_count, critical_flag, description)
VALUES
-- DETERMINISTIC TOOLS (20)
(
    'normalize_request',
    'deterministic',
    ARRAY['api_routes', 'coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["query", "domain"], "properties": {"query": {"type": "string"}, "domain": {"type": "string"}, "sites": {"type": "array"}, "user_id": {"type": "string"}}}'::jsonb,
    2, 0, TRUE,
    'Step 1: Sanitize input, strip HTML/binary, normalize casing'
),
(
    'validate_intent_schema',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["parsed_intent", "domain"], "properties": {"parsed_intent": {"type": "object"}, "domain": {"type": "string"}}}'::jsonb,
    2, 0, TRUE,
    'Step 3a: Deep JSON schema validation (types, ranges, enums)'
),
(
    'validate_intent_constraints',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["parsed_intent", "confidence"], "properties": {"parsed_intent": {"type": "object"}, "confidence": {"type": "number"}}}'::jsonb,
    2, 0, TRUE,
    'Step 3b: Business rules — actionability gate, required fields per query_type'
),
(
    'compute_intent_hash',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["parsed_intent", "domain", "schema_version"], "properties": {"parsed_intent": {"type": "object"}, "domain": {"type": "string"}, "schema_version": {"type": "string"}}}'::jsonb,
    1, 0, TRUE,
    'Step 4a: SHA256(normalized_intent + domain + schema_version)'
),
(
    'check_intent_cache',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "intent_hash", "schema_version"], "properties": {"domain": {"type": "string"}, "intent_hash": {"type": "string"}, "schema_version": {"type": "string"}}}'::jsonb,
    3, 1, FALSE,
    'Step 4b: Check intent cache — IF CACHE HIT skip to final result'
),
(
    'check_site_validation_cache',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "intent_hash", "schema_version", "sites"], "properties": {"domain": {"type": "string"}, "intent_hash": {"type": "string"}, "schema_version": {"type": "string"}, "sites": {"type": "array"}}}'::jsonb,
    3, 1, FALSE,
    'Step 4c: Returns per-site cache status (hit/miss)'
),
(
    'extract_dom_signals',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["site_url", "domain"], "properties": {"site_url": {"type": "string"}, "domain": {"type": "string"}}}'::jsonb,
    10, 1, FALSE,
    'Step 5-DET: Extract DOM structure signals without AI'
),
(
    'store_site_relevance',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "intent_hash", "schema_version", "site_scores"], "properties": {"domain": {"type": "string"}, "intent_hash": {"type": "string"}, "schema_version": {"type": "string"}, "site_scores": {"type": "array"}}}'::jsonb,
    3, 1, FALSE,
    'Step 5-STORE: Persist scores to site_relevance table'
),
(
    'create_site_pipeline',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["request_id", "domain", "sites"], "properties": {"request_id": {"type": "string"}, "domain": {"type": "string"}, "sites": {"type": "array"}}}'::jsonb,
    3, 0, TRUE,
    'Step 6: Create pipeline DB records for validated sites'
),
(
    'fetch_robots_txt',
    'deterministic',
    ARRAY['site_pipeline', 'coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["site_domain", "domain"], "properties": {"site_domain": {"type": "string"}, "domain": {"type": "string"}}}'::jsonb,
    5, 2, TRUE,
    'Step 7a: HTTP fetch robots.txt with 2-retry backoff'
),
(
    'parse_robots_rules',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["raw_content", "site_domain"], "properties": {"raw_content": {"type": "string"}, "site_domain": {"type": "string"}}}'::jsonb,
    2, 0, FALSE,
    'Step 7b: Parse raw content into structured rules'
),
(
    'store_allowed_paths',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "site_domain", "rules"], "properties": {"domain": {"type": "string"}, "site_domain": {"type": "string"}, "rules": {"type": "object"}}}'::jsonb,
    3, 1, FALSE,
    'Step 7c: Persist parsed rules + allowed paths'
),
(
    'check_url_pattern_cache',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "site_domain", "schema_version", "intent"], "properties": {"domain": {"type": "string"}, "site_domain": {"type": "string"}, "schema_version": {"type": "string"}, "intent": {"type": "object"}}}'::jsonb,
    3, 1, FALSE,
    'Step 8-CACHE: URL pattern cache + deterministic URL construction'
),
(
    'inspect_url_dom',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["url"], "properties": {"url": {"type": "string"}, "timeout_ms": {"type": "integer"}}}'::jsonb,
    15, 1, FALSE,
    'Step 8b: Fetch DOM via Playwright and clean HTML'
),
(
    'select_best_url',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["candidates"], "properties": {"candidates": {"type": "array"}, "min_score": {"type": "integer"}}}'::jsonb,
    2, 0, FALSE,
    'Step 8d: Pick highest-scoring URL from candidates'
),
(
    'store_url_pattern',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["domain", "site_domain", "url", "schema_version"], "properties": {"domain": {"type": "string"}, "site_domain": {"type": "string"}, "url": {"type": "string"}, "schema_version": {"type": "string"}, "success": {"type": "boolean"}, "confidence": {"type": "number"}}}'::jsonb,
    3, 1, FALSE,
    'Step 8e: Persist successful URL pattern for future reuse'
),
(
    'scrape_structured_data',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["html", "url", "intent", "extraction_schema", "domain"], "properties": {"html": {"type": "string"}, "url": {"type": "string"}, "intent": {"type": "object"}, "extraction_schema": {"type": "object"}, "domain": {"type": "string"}}}'::jsonb,
    20, 1, FALSE,
    'Step 9: Schema-based extraction (JSON-LD + CSS + regex)'
),
(
    'validate_extraction',
    'deterministic',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["extracted_data", "extraction_schema"], "properties": {"extracted_data": {"type": "object"}, "extraction_schema": {"type": "object"}, "url": {"type": "string"}}}'::jsonb,
    2, 0, TRUE,
    'Step 9-VAL: Schema correctness, required fields, value sanity'
),
(
    'store_final_result',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["request_id", "domain", "unified_data"], "properties": {"request_id": {"type": "string"}, "domain": {"type": "string"}, "unified_data": {"type": "object"}, "intent_hash": {"type": "string"}, "schema_version": {"type": "string"}}}'::jsonb,
    5, 1, TRUE,
    'Step 12b: Persist result, update request state, cache for reuse'
),
(
    'audit_log_event',
    'deterministic',
    ARRAY['*'],
    ARRAY['*'],
    '{"type": "object", "required": ["event_type", "domain"], "properties": {"event_type": {"type": "string"}, "domain": {"type": "string"}, "request_id": {"type": "string"}, "metadata": {"type": "object"}}}'::jsonb,
    3, 1, FALSE,
    'Step 12c: Write complete audit trail'
),
-- AI AGENTS (6)
(
    'intent_agent',
    'ai_tier1',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["query", "domain", "intent_schema", "schema_version", "request_id"], "properties": {"query": {"type": "string"}, "domain": {"type": "string"}, "intent_schema": {"type": "object"}, "schema_version": {"type": "string"}, "request_id": {"type": "string"}}}'::jsonb,
    8, 2, TRUE,
    'Tier-1 AI: Understands user intent and parses into structured domain intent'
),
(
    'relevance_agent',
    'ai_tier1',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["intent", "candidate_sites", "domain", "intent_schema", "schema_version", "request_id"], "properties": {"intent": {"type": "object"}, "candidate_sites": {"type": "array"}, "domain": {"type": "string"}, "intent_schema": {"type": "object"}, "schema_version": {"type": "string"}, "request_id": {"type": "string"}}}'::jsonb,
    10, 2, FALSE,
    'Tier-1 AI: Scores candidate sites for relevance to the parsed intent'
),
(
    'url_agent',
    'ai_tier1',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["intent", "site_url", "domain", "schema_version", "request_id"], "properties": {"intent": {"type": "object"}, "site_url": {"type": "string"}, "domain": {"type": "string"}, "schema_version": {"type": "string"}, "request_id": {"type": "string"}}}'::jsonb,
    10, 2, FALSE,
    'Tier-1 AI: Generates candidate URLs for a given site based on intent'
),
(
    'scoring_agent',
    'ai_tier1',
    ARRAY['site_pipeline', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["html", "url", "intent", "extraction_schema", "domain"], "properties": {"html": {"type": "string"}, "url": {"type": "string"}, "intent": {"type": "object"}, "extraction_schema": {"type": "object"}, "domain": {"type": "string"}, "schema_version": {"type": "string"}}}'::jsonb,
    12, 2, FALSE,
    'Tier-1 AI: AI-based Chain-of-Thought DOM relevance scoring'
),
(
    'intent_classifier',
    'ai_tier1',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["intent", "extractions", "domain"], "properties": {"intent": {"type": "object"}, "extractions": {"type": "array"}, "domain": {"type": "string"}, "schema_version": {"type": "string"}}}'::jsonb,
    8, 1, FALSE,
    'Tier-1 AI: Routes by intent type (discovery/compare/collection)'
),
(
    'unification_agent',
    'ai_tier2',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{"type": "object", "required": ["candidates", "conflict_fields", "extraction_schema", "domain", "schema_version", "request_id"], "properties": {"candidates": {"type": "array"}, "conflict_fields": {"type": "array"}, "extraction_schema": {"type": "object"}, "domain": {"type": "string"}, "schema_version": {"type": "string"}, "request_id": {"type": "string"}}}'::jsonb,
    12, 1, FALSE,
    'Tier-2 AI: Resolves cross-site conflicts after deterministic dedup'
);
