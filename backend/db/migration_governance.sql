-- Governance Contract Migration
-- Adds new failure classifications and sites column

-- New failure class values
ALTER TYPE umsa_core.failure_class ADD VALUE IF NOT EXISTS 'BOT_PROTECTION';
ALTER TYPE umsa_core.failure_class ADD VALUE IF NOT EXISTS 'CLARIFICATION_REQUIRED';
ALTER TYPE umsa_core.failure_class ADD VALUE IF NOT EXISTS 'INVALID_SITE';
ALTER TYPE umsa_core.failure_class ADD VALUE IF NOT EXISTS 'NO_VALID_URL';
ALTER TYPE umsa_core.failure_class ADD VALUE IF NOT EXISTS 'ALL_SITES_BLOCKED';

-- Sites column on requests table
ALTER TABLE umsa_core.requests ADD COLUMN IF NOT EXISTS sites text[] DEFAULT '{}';
