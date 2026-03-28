"""
MCP Client — Request Coordinator (Section 19)

PURE ORCHESTRATION LAYER.
Orchestrates the full 13-step mandatory execution flow.
ALL business logic lives inside MCP tools.
ALL operations go through execute_tool.
ZERO inline SQL, ZERO direct AI calls, ZERO business logic.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from mcp.server.gateway import (
    ToolExecutionContext,
    ToolResult,
    execute_tool,
    transition_state,
)
from pipelines.site_pipeline import SitePipeline
from observability.pipeline_log import PipelineFileLogger

logger = logging.getLogger("umsa.coordinator")


class RequestCoordinator:
    """
    Pure orchestration layer for request lifecycle.
    Every operation delegates to execute_tool.
    Only branching on success/failure and state transitions.
    """

    def __init__(
        self,
        db_pool,
        tool_registry,
        policy_engine,
        execution_logger,
        concurrency_manager,
        domain_registry,
        metrics_collector=None,
    ):
        self._db_pool = db_pool
        self._tool_registry = tool_registry
        self._policy_engine = policy_engine
        self._exec_logger = execution_logger
        self._concurrency = concurrency_manager
        self._domain_registry = domain_registry
        self._metrics = metrics_collector

    # ────────────────────────────────────────────
    # Gateway helper — single call pattern
    # ────────────────────────────────────────────

    async def _call(
        self, tool_name, input_data, request_id, user_id, domain,
        timeout_ms=5000, retry_budget=0, schema_version="",
        caller="coordinator",
    ) -> ToolResult:
        """Execute a tool through the MCP gateway. The ONLY way to invoke logic."""
        return await execute_tool(
            context=ToolExecutionContext(
                tool_name=tool_name,
                input_data=input_data,
                request_id=request_id,
                user_id=user_id,
                domain=domain,
                caller=caller,
                timeout_ms=timeout_ms,
                retry_budget=retry_budget,
                schema_version=schema_version,
            ),
            db_pool=self._db_pool,
            tool_registry_manager=self._tool_registry,
            policy_engine=self._policy_engine,
            execution_logger=self._exec_logger,
            concurrency_manager=self._concurrency,
            metrics_collector=self._metrics,
        )

    # ────────────────────────────────────────────
    # Failure helper — delegates to gateway transition_state
    # ────────────────────────────────────────────

    async def _fail(self, request_id, current_state, error, failure_class="INTERNAL_ERROR"):
        try:
            await transition_state(
                self._db_pool, request_id, current_state, "FAILED",
                self._exec_logger,
                checkpoint_data={"error": error, "failure_class": failure_class},
                error_data={"message": error, "failure_class": failure_class},
            )
        except Exception as e:
            logger.error("Failed to mark request %s as FAILED: %s", request_id, e)
        finally:
            self._concurrency.cleanup_request(request_id)

    # ════════════════════════════════════════════
    # MAIN FLOW — LLM-Driven Orchestration
    # ════════════════════════════════════════════

    async def process_request(
        self,
        request_id: UUID,
        user_id: UUID,
        query: str,
        domain_name: str,
        sites: Optional[List[str]] = None,
        unify: bool = False,
    ) -> Dict[str, Any]:
        """
        LLM-driven orchestration — the AI decides which tool to call next.

        Delegates to OrchestratorLLM which runs a dynamic loop:
          1. Ask LLM: "What tool should I call next?"
          2. Execute chosen tool via MCP gateway
          3. Update state with result
          4. Repeat until LLM says "DONE"

        Falls back to legacy fixed sequence if orchestrator fails on init.
        """
        from mcp.client.orchestrator_llm import OrchestratorLLM

        orchestrator = OrchestratorLLM(
            db_pool=self._db_pool,
            tool_registry=self._tool_registry,
            policy_engine=self._policy_engine,
            execution_logger=self._exec_logger,
            concurrency_manager=self._concurrency,
            domain_registry=self._domain_registry,
            metrics_collector=self._metrics,
        )

        return await orchestrator.process_request(
            request_id=request_id,
            user_id=user_id,
            query=query,
            domain_name=domain_name,
            sites=sites,
            unify=unify,
        )

