-- ============================================================
-- UMSA Core Schema — Enterprise Universal Movie Scraper Agent
-- Version: 1.0
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS umsa_core;

-- ============================================================
-- ENUMS
-- ============================================================

CREATE TYPE umsa_core.request_state AS ENUM (
    'INIT',
    'INTENT_DONE',
    'VALIDATED',
    'CACHE_HIT',
    'RELEVANCE_DONE',
    'PIPELINES_RUNNING',
    'EXTRACTION_DONE',
    'UNIFIED',
    'COMPLETED',
    'FAILED'
);

CREATE TYPE umsa_core.pipeline_state AS ENUM (
    'PENDING',
    'ROBOTS_CHECK',
    'URL_DISCOVERY',
    'EXTRACTING',
    'EXTRACTED',
    'FAILED',
    'SKIPPED'
);

CREATE TYPE umsa_core.domain_status AS ENUM (
    'active',
    'inactive',
    'deprecated'
);

CREATE TYPE umsa_core.health_status AS ENUM (
    'healthy',
    'degraded',
    'disabled',
    'probe_pending'
);

CREATE TYPE umsa_core.cache_status AS ENUM (
    'valid',
    'expired',
    'invalidated'
);

CREATE TYPE umsa_core.failure_class AS ENUM (
    'NETWORK_TIMEOUT',
    'ROBOTS_BLOCKED',
    'DOM_STRUCTURE_CHANGED',
    'EXTRACTION_SCHEMA_FAIL',
    'AI_TIMEOUT',
    'POLICY_REJECTED',
    'RESOURCE_EXHAUSTED',
    'INVALID_TRANSITION',
    'RETRY_BUDGET_EXHAUSTED',
    'BOT_PROTECTION',
    'CLARIFICATION_REQUIRED',
    'INVALID_SITE',
    'NO_VALID_URL',
    'ALL_SITES_BLOCKED'
);

CREATE TYPE umsa_core.robots_status AS ENUM (
    'compliant',
    'blocked',
    'fetch_failed'
);

-- ============================================================
-- TABLES
-- ============================================================

-- 1. domains
CREATE TABLE umsa_core.domains (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(64) NOT NULL UNIQUE,
    display_name    VARCHAR(128) NOT NULL,
    status          umsa_core.domain_status NOT NULL DEFAULT 'active',
    schema_version  VARCHAR(32) NOT NULL,
    config          JSONB NOT NULL DEFAULT '{}',
    allowed_sites   TEXT[] NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. users
CREATE TABLE umsa_core.users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     VARCHAR(256) NOT NULL UNIQUE,
    role            VARCHAR(32) NOT NULL DEFAULT 'user',
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. requests
CREATE TABLE umsa_core.requests (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES umsa_core.users(id),
    domain              VARCHAR(64) NOT NULL,
    raw_query           TEXT NOT NULL,
    normalized_query    TEXT,
    idempotency_key     VARCHAR(128) NOT NULL,
    state               umsa_core.request_state NOT NULL DEFAULT 'INIT',
    schema_version      VARCHAR(32) NOT NULL,
    sites               TEXT[] NOT NULL DEFAULT '{}',
    result              JSONB,
    error               JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_requests_user_id ON umsa_core.requests(user_id);
CREATE INDEX idx_requests_idempotency ON umsa_core.requests(idempotency_key);
CREATE INDEX idx_requests_state ON umsa_core.requests(state);
CREATE INDEX idx_requests_domain ON umsa_core.requests(domain);

-- 4. intent_cache
CREATE TABLE umsa_core.intent_cache (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain          VARCHAR(64) NOT NULL,
    intent_hash     VARCHAR(128) NOT NULL,
    schema_version  VARCHAR(32) NOT NULL,
    parsed_intent   JSONB NOT NULL,
    confidence      FLOAT NOT NULL DEFAULT 0.0,
    status          umsa_core.cache_status NOT NULL DEFAULT 'valid',
    semantic_key    VARCHAR(512),
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_intent_cache_lookup
    ON umsa_core.intent_cache(domain, intent_hash, schema_version)
    WHERE status = 'valid';

CREATE INDEX idx_intent_cache_semantic
    ON umsa_core.intent_cache(domain, semantic_key, schema_version)
    WHERE status = 'valid';

-- 7. site_relevance
CREATE TABLE umsa_core.site_relevance (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id      UUID NOT NULL REFERENCES umsa_core.requests(id),
    domain          VARCHAR(64) NOT NULL,
    intent_hash     VARCHAR(128) NOT NULL,
    site_url        TEXT NOT NULL,
    relevance_score FLOAT NOT NULL DEFAULT 0.0,
    reasoning       TEXT,
    schema_version  VARCHAR(32) NOT NULL,
    status          umsa_core.cache_status NOT NULL DEFAULT 'valid',
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_site_relevance_request ON umsa_core.site_relevance(request_id);
CREATE INDEX idx_site_relevance_lookup
    ON umsa_core.site_relevance(domain, intent_hash, schema_version)
    WHERE status = 'valid';

-- 8. pipelines
CREATE TABLE umsa_core.pipelines (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id      UUID NOT NULL REFERENCES umsa_core.requests(id),
    domain          VARCHAR(64) NOT NULL,
    site_url        TEXT NOT NULL,
    state           umsa_core.pipeline_state NOT NULL DEFAULT 'PENDING',
    error           JSONB,
    failure_class   umsa_core.failure_class,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipelines_request ON umsa_core.pipelines(request_id);
CREATE INDEX idx_pipelines_state ON umsa_core.pipelines(state);

-- 9. robots_cache
CREATE TABLE umsa_core.robots_cache (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain          VARCHAR(64) NOT NULL,
    site_domain     VARCHAR(256) NOT NULL,
    status          umsa_core.robots_status NOT NULL,
    raw_content     TEXT,
    parsed_rules    JSONB NOT NULL DEFAULT '{}',
    schema_version  VARCHAR(32) NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    fetch_latency_ms INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_robots_cache_lookup
    ON umsa_core.robots_cache(site_domain)
    WHERE status != 'fetch_failed';

-- 10. url_patterns
CREATE TABLE umsa_core.url_patterns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain          VARCHAR(64) NOT NULL,
    site_domain     VARCHAR(256) NOT NULL,
    pattern         TEXT NOT NULL,
    pattern_type    VARCHAR(32) NOT NULL DEFAULT 'search',
    confidence      FLOAT NOT NULL DEFAULT 0.0,
    schema_version  VARCHAR(32) NOT NULL,
    status          umsa_core.cache_status NOT NULL DEFAULT 'valid',
    expires_at      TIMESTAMPTZ NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_url_patterns_lookup
    ON umsa_core.url_patterns(domain, site_domain, schema_version)
    WHERE status = 'valid';

-- 11. unified_results
CREATE TABLE umsa_core.unified_results (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id          UUID NOT NULL REFERENCES umsa_core.requests(id),
    domain              VARCHAR(64) NOT NULL,
    intent_hash         VARCHAR(128) NOT NULL,
    unified_data        JSONB NOT NULL,
    source_sites        TEXT[] NOT NULL DEFAULT '{}',
    resolved_conflicts  JSONB NOT NULL DEFAULT '{}',
    confidence          FLOAT NOT NULL DEFAULT 0.0,
    schema_version      VARCHAR(32) NOT NULL,
    status              umsa_core.cache_status NOT NULL DEFAULT 'valid',
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_unified_results_request ON umsa_core.unified_results(request_id);
CREATE INDEX idx_unified_results_cache
    ON umsa_core.unified_results(domain, intent_hash, schema_version)
    WHERE status = 'valid';

-- 13. execution_checkpoints
CREATE TABLE umsa_core.execution_checkpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id      UUID NOT NULL REFERENCES umsa_core.requests(id),
    state           umsa_core.request_state NOT NULL,
    previous_state  umsa_core.request_state,
    checkpoint_data JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_checkpoints_request ON umsa_core.execution_checkpoints(request_id);
CREATE INDEX idx_checkpoints_state ON umsa_core.execution_checkpoints(request_id, state);

-- 14. execution_logs
CREATE TABLE umsa_core.execution_logs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id          UUID REFERENCES umsa_core.requests(id),
    tool_name           VARCHAR(64),
    caller              VARCHAR(64),
    domain              VARCHAR(64),
    event_type          VARCHAR(64) NOT NULL,
    input_data          JSONB,
    output_data         JSONB,
    error               JSONB,
    failure_class       umsa_core.failure_class,
    latency_ms          INT,
    request_latency_ms  INT,
    stage_latency_ms    INT,
    ai_call_count       INT DEFAULT 0,
    cache_hit           BOOLEAN DEFAULT FALSE,
    retry_count         INT DEFAULT 0,
    semaphore_wait_ms   INT DEFAULT 0,
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_logs_request ON umsa_core.execution_logs(request_id);
CREATE INDEX idx_logs_tool ON umsa_core.execution_logs(tool_name);
CREATE INDEX idx_logs_event ON umsa_core.execution_logs(event_type);
CREATE INDEX idx_logs_created ON umsa_core.execution_logs(created_at);

-- 15. tool_registry
CREATE TABLE umsa_core.tool_registry (
    tool_name       VARCHAR(64) PRIMARY KEY,
    role            VARCHAR(64) NOT NULL,
    allowed_callers TEXT[] NOT NULL DEFAULT '{}',
    domain_scope    TEXT[] NOT NULL DEFAULT '{}',
    input_schema    JSONB NOT NULL DEFAULT '{}',
    timeout_seconds INT NOT NULL DEFAULT 10,
    retry_count     INT NOT NULL DEFAULT 1,
    critical_flag   BOOLEAN NOT NULL DEFAULT FALSE,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 16. domain_health
CREATE TABLE umsa_core.domain_health (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain              VARCHAR(64) NOT NULL,
    site_domain         VARCHAR(256) NOT NULL,
    status              umsa_core.health_status NOT NULL DEFAULT 'healthy',
    failure_count       INT NOT NULL DEFAULT 0,
    last_failure        TIMESTAMPTZ,
    last_failure_class  umsa_core.failure_class,
    cooldown_until      TIMESTAMPTZ,
    window_start        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_domain_health_lookup
    ON umsa_core.domain_health(domain, site_domain);

-- 17. schema_migrations
CREATE TABLE umsa_core.schema_migrations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain          VARCHAR(64) NOT NULL,
    schema_version  VARCHAR(32) NOT NULL,
    migrated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes           TEXT,
    UNIQUE(domain, schema_version)
);

-- ============================================================
-- SEED DATA
-- ============================================================

-- Seed: tool_registry (26 tools: 20 deterministic + 6 AI agents)
INSERT INTO umsa_core.tool_registry (tool_name, role, allowed_callers, domain_scope, input_schema, timeout_seconds, retry_count, critical_flag, description)
VALUES
-- ═══ DETERMINISTIC TOOLS (20) ═══
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
-- ═══ AI AGENTS (6) ═══
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

-- Seed: domain (movie)
INSERT INTO umsa_core.domains (name, display_name, status, schema_version, allowed_sites)
VALUES (
    'movie',
    'Movies',
    'active',
    'movie_v1',
    ARRAY[]::text[]
);
