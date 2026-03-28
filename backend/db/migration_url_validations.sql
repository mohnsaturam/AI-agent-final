-- URL Validations table for DOM-validated URL discovery
-- Stores each URL attempt with its DOM confidence score and decision

CREATE TABLE IF NOT EXISTS umsa_core.url_validations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL,
    pipeline_id UUID NOT NULL,
    site_domain TEXT NOT NULL,
    candidate_url TEXT NOT NULL,
    attempt_number INT NOT NULL,
    dom_confidence INT NOT NULL DEFAULT 0,
    validation_summary TEXT,
    relevant_signals JSONB DEFAULT '[]'::jsonb,
    missing_filters JSONB DEFAULT '[]'::jsonb,
    page_type TEXT,
    decision TEXT NOT NULL DEFAULT 'RETRY',
    latency_ms INT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_url_validations_request
    ON umsa_core.url_validations (request_id);

CREATE INDEX IF NOT EXISTS idx_url_validations_pipeline
    ON umsa_core.url_validations (pipeline_id);
