"""
MCP Server — Tool Execution Gateway (Section 7)

This is the SOLE execution authority for ALL deterministic tools.
The 13-step enforcement sequence is implemented here.
Zero domain-specific imports allowed.
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import UUID

logger = logging.getLogger("umsa.gateway")


# ============================================================
# Data Structures (Section 7.1)
# ============================================================

@dataclass
class ToolExecutionContext:
    """Full context required for every tool invocation."""
    tool_name: str
    input_data: dict
    request_id: UUID
    user_id: UUID
    domain: str
    caller: str
    timeout_ms: int
    retry_budget: int
    semaphore_tokens: dict = field(default_factory=dict)
    schema_version: str = ""


@dataclass
class ToolResult:
    """Standard result envelope from tool execution."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    failure_class: Optional[str] = None
    latency_ms: int = 0
    retries_used: int = 0


# ============================================================
# Valid State Transitions (Section 9)
# ============================================================

VALID_TRANSITIONS = {
    "INIT": ["INTENT_DONE", "FAILED"],
    "INTENT_DONE": ["VALIDATED", "FAILED"],
    "VALIDATED": ["CACHE_HIT", "RELEVANCE_DONE", "FAILED"],
    "CACHE_HIT": ["COMPLETED"],
    "RELEVANCE_DONE": ["PIPELINES_RUNNING", "FAILED"],
    "PIPELINES_RUNNING": ["EXTRACTION_DONE", "FAILED"],
    "EXTRACTION_DONE": ["UNIFIED", "FAILED"],
    "UNIFIED": ["COMPLETED", "FAILED"],
    "COMPLETED": [],  # terminal
    "FAILED": [],  # terminal
}


# ============================================================
# Tool Implementation Registry (in-memory, populated by tools)
# ============================================================

_tool_implementations: Dict[str, Any] = {}


def register_tool_implementation(tool_name: str, func):
    """Register a callable implementation for a tool name."""
    _tool_implementations[tool_name] = func


def get_tool_implementation(tool_name: str):
    """Retrieve the registered implementation for a tool."""
    return _tool_implementations.get(tool_name)


# ============================================================
# Gateway — execute_tool (Section 7.2)
# ============================================================

async def execute_tool(
    context: ToolExecutionContext,
    db_pool,
    tool_registry_manager,
    policy_engine,
    execution_logger,
    concurrency_manager,
    metrics_collector=None,
) -> ToolResult:
    """
    Sole execution authority for all deterministic tools.
    Enforces the 13-step gateway sequence defined in Section 7.2.

    Direct tool invocation outside this function is a critical violation.
    """
    start_time = time.monotonic()
    semaphore_wait_start = None

    # ── Step 1: Validate tool_name exists in tool_registry ──
    tool_def = await tool_registry_manager.get_tool(context.tool_name)
    if tool_def is None:
        return ToolResult(
            success=False,
            error=f"Tool '{context.tool_name}' not found in registry",
            failure_class="POLICY_REJECTED",
        )

    # ── Step 2: Validate caller (P3.3) ──
    if not tool_registry_manager.validate_caller(tool_def, context.caller):
        logger.warning(
            "Caller '%s' is not authorized for tool '%s'. Allowed: %s",
            context.caller, context.tool_name, tool_def.get("allowed_callers", [])
        )
        return ToolResult(
            success=False,
            error=f"Caller '{context.caller}' not permitted for tool '{context.tool_name}'",
            failure_class="POLICY_REJECTED",
        )

    # ── Step 3: Validate domain is in domain_scope ──
    if not tool_registry_manager.validate_domain(tool_def, context.domain):
        return ToolResult(
            success=False,
            error=f"Domain '{context.domain}' not in scope for tool '{context.tool_name}'",
            failure_class="POLICY_REJECTED",
        )

    # ── Step 4: Validate input_data against input_schema ──
    validation_error = tool_registry_manager.validate_input(
        tool_def, context.input_data
    )
    if validation_error:
        return ToolResult(
            success=False,
            error=f"Input validation failed: {validation_error}",
            failure_class="POLICY_REJECTED",
        )

    # ── Step 5: Policy engine pre-condition check ──
    policy_result = await policy_engine.check(context)
    if not policy_result.allowed:
        return ToolResult(
            success=False,
            error=f"Policy rejected: {policy_result.reason}",
            failure_class="POLICY_REJECTED",
        )

    # ── Step 6: Acquire global semaphore token ──
    semaphore_wait_start = time.monotonic()
    global_token = await concurrency_manager.acquire_global(context.tool_name)
    if global_token is None:
        return ToolResult(
            success=False,
            error="Global semaphore exhausted",
            failure_class="RESOURCE_EXHAUSTED",
        )
    context.semaphore_tokens["global"] = global_token

    # ── Step 7: Acquire per-request semaphore token ──
    request_token = await concurrency_manager.acquire_per_request(
        context.request_id
    )
    if request_token is None:
        await concurrency_manager.release_global(
            context.tool_name, global_token
        )
        return ToolResult(
            success=False,
            error="Per-request semaphore exhausted",
            failure_class="RESOURCE_EXHAUSTED",
        )
    context.semaphore_tokens["per_request"] = request_token

    semaphore_wait_ms = int(
        (time.monotonic() - semaphore_wait_start) * 1000
    )

    # ── Step 8: Log tool invocation with full context ──
    await execution_logger.log_tool_call(
        request_id=context.request_id,
        tool_name=context.tool_name,
        caller=context.caller,
        domain=context.domain,
        input_data=context.input_data,
        event_type="TOOL_INVOKED",
        semaphore_wait_ms=semaphore_wait_ms,
    )

    if metrics_collector:
        metrics_collector.record_tool_invocation(
            domain=context.domain,
            tool_name=context.tool_name,
            caller=context.caller,
            status="started",
        )
        metrics_collector.record_semaphore_wait(
            semaphore_type="tool_execution",
            wait_ms=semaphore_wait_ms,
        )

    # ── Steps 9–12: Execute with retry loop ──
    tool_impl = get_tool_implementation(context.tool_name)
    if tool_impl is None:
        # Release semaphores before returning
        await concurrency_manager.release_global(
            context.tool_name, context.semaphore_tokens.get("global", "")
        )
        await concurrency_manager.release_per_request(
            context.request_id, context.semaphore_tokens.get("per_request", "")
        )
        return ToolResult(
            success=False,
            error=f"No implementation registered for tool '{context.tool_name}'",
            failure_class="POLICY_REJECTED",
        )


    timeout_seconds = context.timeout_ms / 1000.0
    print(f"DEBUG: Executing tool {context.tool_name} with timeout {timeout_seconds}s")
    retries_used = 0
    last_error = None
    last_failure_class = None

    try:
        while True:
            try:
                # ── Step 9: Execute tool with asyncio.wait_for(timeout) ──
                print(f"DEBUG: Calling tool_impl for {context.tool_name}...")
                result_data = await asyncio.wait_for(
                    tool_impl(context, db_pool),
                    timeout=timeout_seconds,
                )

                # ── Step 10: Success — log output, latency, persist checkpoint ──
                latency_ms = int((time.monotonic() - start_time) * 1000)

                await execution_logger.log_tool_call(
                    request_id=context.request_id,
                    tool_name=context.tool_name,
                    caller=context.caller,
                    domain=context.domain,
                    output_data=result_data,
                    event_type="TOOL_COMPLETED",
                    latency_ms=latency_ms,
                    retry_count=retries_used,
                )

                if metrics_collector:
                    metrics_collector.record_tool_invocation(
                        domain=context.domain,
                        tool_name=context.tool_name,
                        caller=context.caller,
                        status="success",
                    )
                    metrics_collector.record_tool_duration(
                        domain=context.domain,
                        tool_name=context.tool_name,
                        caller=context.caller,
                        duration_seconds=latency_ms / 1000.0,
                    )

                return ToolResult(
                    success=True,
                    data=result_data,
                    latency_ms=latency_ms,
                    retries_used=retries_used,
                )

            except asyncio.TimeoutError:
                # ── Step 11 (timeout): Classify failure ──
                latency_ms = int((time.monotonic() - start_time) * 1000)
                last_failure_class = (
                    "AI_TIMEOUT"
                    if tool_def and tool_def.get("role", "").startswith("ai_")
                    else "NETWORK_TIMEOUT"
                )
                last_error = f"Tool '{context.tool_name}' timed out after {context.timeout_ms}ms"

                await _handle_failure(
                    context=context,
                    failure_class=last_failure_class,
                    error=last_error,
                    latency_ms=latency_ms,
                    execution_logger=execution_logger,
                    concurrency_manager=concurrency_manager,
                    db_pool=db_pool,
                    metrics_collector=metrics_collector,
                )

                # ── Step 12: Bounded retry check ──
                if context.retry_budget > 0:
                    context.retry_budget -= 1
                    retries_used += 1
                    logger.info(
                        "Retrying tool '%s' (attempt %d, budget remaining: %d)",
                        context.tool_name, retries_used, context.retry_budget,
                    )
                    await asyncio.sleep(min(retries_used * 0.5, 2.0))  # backoff
                    continue
                else:
                    return ToolResult(
                        success=False,
                        error=f"{last_error} — retry budget exhausted",
                        failure_class="RETRY_BUDGET_EXHAUSTED",
                        latency_ms=latency_ms,
                        retries_used=retries_used,
                    )

            except Exception as exc:
                # ── Step 11 (exception): Classify and handle ──
                latency_ms = int((time.monotonic() - start_time) * 1000)
                last_failure_class = _classify_exception(exc)
                last_error = str(exc)

                await _handle_failure(
                    context=context,
                    failure_class=last_failure_class,
                    error=last_error,
                    latency_ms=latency_ms,
                    execution_logger=execution_logger,
                    concurrency_manager=concurrency_manager,
                    db_pool=db_pool,
                    metrics_collector=metrics_collector,
                )

                # ── Step 12: Bounded retry ──
                if context.retry_budget > 0:
                    context.retry_budget -= 1
                    retries_used += 1
                    logger.info(
                        "Retrying tool '%s' after error (attempt %d): %s",
                        context.tool_name, retries_used, last_error,
                    )
                    await asyncio.sleep(min(retries_used * 0.5, 2.0))
                    continue
                else:
                    return ToolResult(
                        success=False,
                        error=f"{last_error} — retry budget exhausted",
                        failure_class="RETRY_BUDGET_EXHAUSTED",
                        latency_ms=latency_ms,
                        retries_used=retries_used,
                    )

    finally:
        # ── Step 13: Always release semaphore tokens ──
        if "global" in context.semaphore_tokens:
            await concurrency_manager.release_global(
                context.tool_name, context.semaphore_tokens["global"]
            )
        if "per_request" in context.semaphore_tokens:
            await concurrency_manager.release_per_request(
                context.request_id, context.semaphore_tokens["per_request"]
            )


# ============================================================
# State Machine Transitions (Section 9)
# ============================================================

async def transition_state(
    db_pool,
    request_id: UUID,
    current_state: str,
    new_state: str,
    execution_logger,
    checkpoint_data: Optional[dict] = None,
    error_data: Optional[dict] = None,
) -> bool:
    """
    Atomically transition request state.
    Validates transition legality, persists checkpoint, logs everything.
    If new_state is FAILED and error_data is provided, sets the error field.
    """
    # Validate transition
    allowed = VALID_TRANSITIONS.get(current_state, [])
    if new_state not in allowed:
        await execution_logger.log_tool_call(
            request_id=request_id,
            event_type="INVALID_TRANSITION",
            error={
                "current_state": current_state,
                "attempted_state": new_state,
                "allowed": allowed,
            },
        )
        logger.error(
            "Invalid state transition: %s -> %s (allowed: %s)",
            current_state,
            new_state,
            allowed,
        )
        return False

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            if new_state == "FAILED" and error_data:
                updated = await conn.fetchval(
                    """
                    UPDATE umsa_core.requests
                    SET state = $1::umsa_core.request_state,
                        error = $4::jsonb,
                        updated_at = now()
                    WHERE id = $2 AND state = $3::umsa_core.request_state
                    RETURNING id
                    """,
                    new_state,
                    request_id,
                    current_state,
                    _safe_json(error_data),
                )
            else:
                updated = await conn.fetchval(
                    """
                    UPDATE umsa_core.requests
                    SET state = $1::umsa_core.request_state,
                        updated_at = now()
                    WHERE id = $2 AND state = $3::umsa_core.request_state
                    RETURNING id
                    """,
                    new_state,
                    request_id,
                    current_state,
                )
            if updated is None:
                logger.error(
                    "State transition failed (concurrent modification): %s -> %s for %s",
                    current_state,
                    new_state,
                    request_id,
                )
                return False

            # Persist checkpoint
            await conn.execute(
                """
                INSERT INTO umsa_core.execution_checkpoints
                    (request_id, state, previous_state, checkpoint_data)
                VALUES ($1, $2::umsa_core.request_state,
                        $3::umsa_core.request_state, $4::jsonb)
                """,
                request_id,
                new_state,
                current_state,
                _safe_json(checkpoint_data or {}),
            )

    # Log transition
    await execution_logger.log_state_transition(
        request_id=request_id,
        from_state=current_state,
        to_state=new_state,
    )

    logger.info("State: %s -> %s for request %s", current_state, new_state, request_id)
    return True


# ============================================================
# Helpers
# ============================================================

async def _handle_failure(
    context: ToolExecutionContext,
    failure_class: str,
    error: str,
    latency_ms: int,
    execution_logger,
    concurrency_manager,
    db_pool,
    metrics_collector=None,
):
    """Log failure, update domain_health, record metrics."""
    await execution_logger.log_tool_call(
        request_id=context.request_id,
        tool_name=context.tool_name,
        caller=context.caller,
        domain=context.domain,
        event_type="TOOL_FAILED",
        error={"message": error, "failure_class": failure_class},
        latency_ms=latency_ms,
    )

    # Update domain_health
    try:
        await _update_domain_health(db_pool, context.domain, failure_class)
    except Exception as health_err:
        logger.warning("Failed to update domain_health: %s", health_err)

    if metrics_collector:
        metrics_collector.record_tool_invocation(
            domain=context.domain,
            tool_name=context.tool_name,
            caller=context.caller,
            status="failed",
        )
        metrics_collector.record_domain_failure(
            domain=context.domain,
            site_domain=context.input_data.get("site_domain", "unknown"),
            failure_class=failure_class,
        )


async def _update_domain_health(db_pool, domain: str, failure_class: str):
    """Increment failure count and update status per Section 15."""
    async with db_pool.acquire() as conn:
        # This is a simplified update — site_domain is extracted from context
        # In practice, site_domain should be passed explicitly
        await conn.execute(
            """
            INSERT INTO umsa_core.domain_health
                (domain, site_domain, status, failure_count,
                 last_failure, last_failure_class, window_start)
            VALUES ($1, 'global', 'healthy', 1, now(),
                    $2::umsa_core.failure_class, now())
            ON CONFLICT (domain, site_domain) DO UPDATE SET
                failure_count = umsa_core.domain_health.failure_count + 1,
                last_failure = now(),
                last_failure_class = $2::umsa_core.failure_class,
                status = CASE
                    WHEN umsa_core.domain_health.failure_count + 1 >= 10
                        THEN 'disabled'::umsa_core.health_status
                    WHEN umsa_core.domain_health.failure_count + 1 >= 5
                        THEN 'degraded'::umsa_core.health_status
                    ELSE umsa_core.domain_health.status
                END,
                cooldown_until = CASE
                    WHEN umsa_core.domain_health.failure_count + 1 >= 10
                        THEN now() + interval '30 minutes'
                    ELSE umsa_core.domain_health.cooldown_until
                END,
                updated_at = now()
            """,
            domain,
            failure_class,
        )


def _classify_exception(exc: Exception) -> str:
    """Map exception types to failure classifications (Section 15.3)."""
    exc_name = type(exc).__name__
    exc_str = str(exc)
    # BOT_PROTECTION: HTTP 403 during scraping
    if "BOT_PROTECTION" in exc_str or "403" in exc_str:
        return "BOT_PROTECTION"
    if "Timeout" in exc_name or "timeout" in exc_str.lower():
        return "NETWORK_TIMEOUT"
    if "Connection" in exc_name or "connect" in exc_str.lower():
        return "NETWORK_TIMEOUT"
    if "Schema" in exc_name or "schema" in exc_str.lower():
        return "EXTRACTION_SCHEMA_FAIL"
    if "DOM" in exc_name or "selector" in exc_str.lower():
        return "DOM_STRUCTURE_CHANGED"
    return "NETWORK_TIMEOUT"  # default classification


def _safe_json(data) -> str:
    """Safely convert data to JSON string for DB storage."""
    import json
    try:
        return json.dumps(data, default=str)
    except (TypeError, ValueError):
        return json.dumps({"raw": str(data)})
