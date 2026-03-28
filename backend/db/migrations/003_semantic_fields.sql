-- ============================================================
-- Migration: Add semantic_fields table
-- Stores AI-matched field metadata per request and site
-- ============================================================

CREATE TABLE IF NOT EXISTS umsa_core.semantic_fields (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id      UUID NOT NULL REFERENCES umsa_core.requests(id),
    site_domain     VARCHAR(256) NOT NULL,
    raw_field_key   VARCHAR(256) NOT NULL,
    display_name    VARCHAR(256) NOT NULL,
    relevance       FLOAT NOT NULL DEFAULT 0.0,
    category        VARCHAR(64),
    raw_value       TEXT,
    engine_source   VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_semantic_fields_request
    ON umsa_core.semantic_fields(request_id);
CREATE INDEX IF NOT EXISTS idx_semantic_fields_site
    ON umsa_core.semantic_fields(request_id, site_domain);
