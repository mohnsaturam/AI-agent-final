-- Migration: Create site_memory table for cross-query site knowledge
-- Run date: 2026-03-09

CREATE TABLE IF NOT EXISTS umsa_core.site_memory (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain      VARCHAR NOT NULL,
    site_url    TEXT NOT NULL,
    avg_relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_queries       INTEGER NOT NULL DEFAULT 0,
    successful_queries  INTEGER NOT NULL DEFAULT 0,
    failed_queries      INTEGER NOT NULL DEFAULT 0,
    last_score          DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    is_valid    BOOLEAN NOT NULL DEFAULT true,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(domain, site_url)
);

-- Backfill from existing site_relevance data
INSERT INTO umsa_core.site_memory (domain, site_url, avg_relevance_score,
    total_queries, successful_queries, failed_queries, last_score, is_valid)
SELECT domain, site_url,
       AVG(relevance_score),
       COUNT(*)::int,
       COUNT(*) FILTER (WHERE relevance_score >= 0.3)::int,
       COUNT(*) FILTER (WHERE relevance_score < 0.3)::int,
       (ARRAY_AGG(relevance_score ORDER BY created_at DESC))[1],
       AVG(relevance_score) >= 0.3
FROM umsa_core.site_relevance
WHERE status = 'valid'
GROUP BY domain, site_url
ON CONFLICT (domain, site_url) DO NOTHING;

-- Create index for fast domain+site_url lookups
CREATE INDEX IF NOT EXISTS idx_site_memory_domain_site
    ON umsa_core.site_memory(domain, site_url);
