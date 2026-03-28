"""
MCP Tool — audit_log_event (Step 12c)

Write complete audit trail.
"""

import json
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.audit_log_event")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Write an audit trail event to execution_logs."""
    input_data = context.input_data
    request_id = input_data.get("request_id")
    event_type = input_data.get("event_type", "AUDIT_EVENT")
    domain = input_data.get("domain", "")
    tool_name = input_data.get("tool_name")
    caller = input_data.get("caller")
    metadata = input_data.get("metadata", {})
    latency_ms = input_data.get("latency_ms")
    ai_call_count = input_data.get("ai_call_count", 0)
    cache_stages_hit = input_data.get("cache_stages_hit", [])

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO umsa_core.execution_logs
                    (request_id, tool_name, caller, domain, event_type,
                     latency_ms, ai_call_count, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                """,
                request_id,
                tool_name,
                caller,
                domain,
                event_type,
                latency_ms,
                ai_call_count,
                json.dumps({
                    **metadata,
                    "cache_stages_hit": cache_stages_hit,
                }, default=str),
            )
    except Exception as e:
        logger.error("Failed to write audit log: %s", e)

    return {
        "logged": True,
        "event_type": event_type,
        "request_id": str(request_id) if request_id else None,
    }
