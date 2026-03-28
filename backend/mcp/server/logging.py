"""
MCP Server — Execution Logger (Section 18)

Persists all tool invocations, state transitions, and failures
to execution_logs and execution_checkpoints.
All writes go through the async DB pool.
"""

import json
import logging
from typing import Any, Dict, Optional
from uuid import UUID

logger = logging.getLogger("umsa.execution_logger")


class ExecutionLogger:
    """
    Structured logging to PostgreSQL for full auditability.
    Every tool call, state transition, and failure is persisted.
    """

    def __init__(self, db_pool):
        self._db_pool = db_pool

    async def log_tool_call(
        self,
        request_id: Optional[UUID] = None,
        tool_name: Optional[str] = None,
        caller: Optional[str] = None,
        domain: Optional[str] = None,
        event_type: str = "TOOL_INVOKED",
        input_data: Optional[dict] = None,
        output_data: Any = None,
        error: Any = None,
        failure_class: Optional[str] = None,
        latency_ms: Optional[int] = None,
        cache_hit: bool = False,
        retry_count: int = 0,
        semaphore_wait_ms: int = 0,
        metadata: Optional[dict] = None,
    ) -> None:
        """Persist a tool call event to execution_logs."""
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO umsa_core.execution_logs
                        (request_id, tool_name, caller, domain, event_type,
                         input_data, output_data, error, failure_class,
                         latency_ms, cache_hit, retry_count,
                         semaphore_wait_ms, metadata)
                    VALUES ($1, $2, $3, $4, $5,
                            $6::jsonb, $7::jsonb, $8::jsonb,
                            $9::umsa_core.failure_class,
                            $10, $11, $12, $13, $14::jsonb)
                    """,
                    request_id,
                    tool_name,
                    caller,
                    domain,
                    event_type,
                    _safe_jsonb(input_data),
                    _safe_jsonb(output_data),
                    _safe_jsonb(error),
                    failure_class,
                    latency_ms,
                    cache_hit,
                    retry_count,
                    semaphore_wait_ms,
                    _safe_jsonb(metadata or {}),
                )
        except Exception as e:
            # Logging should never crash the system
            logger.error("Failed to persist execution log: %s", e)

    async def log_state_transition(
        self,
        request_id: UUID,
        from_state: str,
        to_state: str,
    ) -> None:
        """Log a state machine transition."""
        await self.log_tool_call(
            request_id=request_id,
            event_type="STATE_TRANSITION",
            metadata={"from_state": from_state, "to_state": to_state},
        )

    async def log_failure(
        self,
        request_id: UUID,
        tool_name: str,
        domain: str,
        failure_class: str,
        error_message: str,
        latency_ms: int = 0,
    ) -> None:
        """Log a classified failure event."""
        await self.log_tool_call(
            request_id=request_id,
            tool_name=tool_name,
            domain=domain,
            event_type="TOOL_FAILED",
            error={"message": error_message},
            failure_class=failure_class,
            latency_ms=latency_ms,
        )

    async def log_cache_decision(
        self,
        request_id: UUID,
        domain: str,
        stage: str,
        cache_hit: bool,
        details: Optional[dict] = None,
    ) -> None:
        """Log a cache hit/miss decision for observability."""
        await self.log_tool_call(
            request_id=request_id,
            domain=domain,
            event_type="CACHE_DECISION",
            cache_hit=cache_hit,
            metadata={"stage": stage, **(details or {})},
        )

    async def log_request_complete(
        self,
        request_id: UUID,
        domain: str,
        total_latency_ms: int,
        ai_call_count: int = 0,
        cache_stages_hit: Optional[list] = None,
    ) -> None:
        """Log full request completion with lineage summary."""
        await self.log_tool_call(
            request_id=request_id,
            domain=domain,
            event_type="REQUEST_COMPLETED",
            latency_ms=total_latency_ms,
            metadata={
                "ai_call_count": ai_call_count,
                "cache_stages_hit": cache_stages_hit or [],
            },
        )

    async def get_request_logs(
        self, request_id: UUID
    ) -> list:
        """Retrieve all logs for a request (for replay/audit)."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM umsa_core.execution_logs
                WHERE request_id = $1
                ORDER BY created_at ASC
                """,
                request_id,
            )
        return [dict(r) for r in rows]


def _safe_jsonb(data: Any) -> Optional[str]:
    """Safely convert data to JSON string for JSONB columns."""
    if data is None:
        return None
    try:
        return json.dumps(data, default=str)
    except (TypeError, ValueError):
        return json.dumps({"raw": str(data)})
