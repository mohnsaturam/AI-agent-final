-- ============================================================
-- Migration: Capability Resolution + Strategy Planning Tools
-- Date: 2026-03-03
-- Description: Registers 2 new deterministic tools in tool_registry
--              (Steps 4a and 4b in the orchestration pipeline)
-- ============================================================

-- Tool 1: resolve_capabilities (Step 4a)
-- Deterministic capability vector from parsed intent
INSERT INTO umsa_core.tool_registry (
    tool_name, role, allowed_callers, domain_scope,
    input_schema, timeout_seconds, retry_count, critical_flag, description
) VALUES (
    'resolve_capabilities',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{
        "type": "object",
        "required": ["parsed_intent", "num_sites"],
        "properties": {
            "parsed_intent": {"type": "object"},
            "num_sites": {"type": "integer", "minimum": 0}
        }
    }'::jsonb,
    2, 0, TRUE,
    'Step 4a: Deterministic capability vector from parsed intent — computes cardinality, filtering, ranking, and aggregation flags'
)
ON CONFLICT (tool_name) DO UPDATE SET
    input_schema = EXCLUDED.input_schema,
    description = EXCLUDED.description,
    updated_at = now();

-- Tool 2: plan_strategy (Step 4b)
-- Deterministic execution strategy from capability vector
INSERT INTO umsa_core.tool_registry (
    tool_name, role, allowed_callers, domain_scope,
    input_schema, timeout_seconds, retry_count, critical_flag, description
) VALUES (
    'plan_strategy',
    'deterministic',
    ARRAY['coordinator', 'orchestrator'],
    ARRAY['*'],
    '{
        "type": "object",
        "required": ["capability_vector", "num_sites"],
        "properties": {
            "capability_vector": {"type": "object"},
            "num_sites": {"type": "integer", "minimum": 0},
            "domain_config": {"type": "object"}
        }
    }'::jsonb,
    2, 0, TRUE,
    'Step 4b: Deterministic execution strategy from capability vector — produces URL hints, page type expectations, extraction mode, and unification instructions'
)
ON CONFLICT (tool_name) DO UPDATE SET
    input_schema = EXCLUDED.input_schema,
    description = EXCLUDED.description,
    updated_at = now();
