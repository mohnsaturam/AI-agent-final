"""
MCP Client — Hybrid Orchestrator

HYBRID APPROACH:
  Phase 1 (Deterministic): Fixed sequence for predictable steps.
           No AI is used to decide "what tool next".
           Each tool still goes through execute_tool() → MCP gateway.

  Phase 2 (AI-Driven): Site pipelines use AI internally
           (url_agent, scoring_agent) for genuinely dynamic decisions.
           Unification uses AI only if cross-site conflicts exist.

This saves ~6 AI calls per request compared to the full-LLM orchestrator,
while remaining fully MCP-compliant (all tools go through the gateway).
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from app.core.config import settings
from mcp.server.gateway import (
    ToolExecutionContext,
    ToolResult,
    execute_tool,
    transition_state,
)
from observability.pipeline_log import PipelineFileLogger

logger = logging.getLogger("umsa.orchestrator")


class OrchestratorLLM:
    """
    Hybrid orchestrator — deterministic backbone + AI for dynamic decisions.

    Phase 1: Deterministic sequence (normalize → intent → validate → cache)
    Phase 2: AI-driven site pipelines + unification
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
    # MCP Gateway helper
    # ────────────────────────────────────────────

    async def _call(
        self, tool_name, input_data, request_id, user_id, domain,
        timeout_ms=5000, retry_budget=0, schema_version="",
        caller="orchestrator",
    ) -> ToolResult:
        """Execute a tool through the MCP gateway."""
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

    async def _fail(self, request_id, current_state, error, failure_class="INTERNAL_ERROR"):
        """Mark request as failed."""
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

    async def _transition(self, state, from_state, to_state) -> None:
        """Call gateway state transition and update local state."""
        if state["current_state"] != from_state:
            logger.warning(
                "Orchestrator transition skip: current state is %s, not %s",
                state["current_state"], from_state
            )
            return

        success = await transition_state(
            self._db_pool, state["request_id"], from_state, to_state,
            self._exec_logger
        )

        if success:
            state["current_state"] = to_state
        else:
            # Sync with reality
            async with self._db_pool.acquire() as conn:
                real_state = await conn.fetchval(
                    "SELECT state FROM umsa_core.requests WHERE id = $1",
                    UUID(state["request_id"])
                )
                if real_state:
                    logger.info("Orchestrator state synced with DB: %s", real_state)
                    state["current_state"] = str(real_state)

    # ════════════════════════════════════════════
    # MAIN ENTRY POINT
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
        Hybrid orchestration: deterministic Phase 1 + AI-driven Phase 2.
        """
        start_time = time.monotonic()
        if not sites:
            sites = []

        flog = PipelineFileLogger(request_id, query, domain_name, sites)

        # Load domain module and config
        domain_module = self._domain_registry.get(domain_name)
        if not domain_module:
            await self._fail(request_id, "INIT", f"Domain '{domain_name}' not found")
            return {"error": f"Domain '{domain_name}' not loaded"}

        sv = domain_module["schema_version"]
        intent_schema = domain_module["intent_schema"]
        extraction_schema = domain_module["extraction_schema"]
        domain_config = domain_module.get("db_config", {}).get("config", {})

        # Build state tracker
        state = {
            "query": query,
            "domain": domain_name,
            "sites": sites,
            "request_id": str(request_id),
            "user_id": str(user_id),
            "schema_version": sv,
            "current_state": "INIT",
            "completed_steps": [],
            "results": {},
            "ai_call_count": 0,
        }

        flog.step(0, "Orchestrator Init", "OK",
                  f"Hybrid orchestration started (deterministic backbone + AI tools)\n"
                  f"Domain: {domain_name}, Schema: {sv}\n"
                  f"Sites: {sites}")

        try:
            # ════════════════════════════════════════
            # PHASE 1: Deterministic Backbone
            # ════════════════════════════════════════
            flog.separator("PHASE 1: Query Understanding (deterministic)")

            # ── Step 1: normalize_request ──
            flog.info("Normalizing user query: trimming, lowercasing, cleaning.")
            r = await self._call("normalize_request", {
                "query": query, "domain": domain_name,
                "sites": sites, "user_id": str(user_id),
                "request_id": str(request_id),
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("normalize_request")
            state["results"]["normalize_request"] = r.data if r.success else {"error": str(r.error)}

            flog.substep("Step 1: Normalize Query", "OK" if r.success else "FAILED",
                         f"Normalized: {r.data.get('normalized_query', query) if r.data else query}",
                         elapsed_ms=r.latency_ms)

            if not r.success:
                await self._fail(request_id, "INIT", f"Normalization failed: {r.error}")
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error=str(r.error))
                return {"error": f"Normalization failed: {r.error}"}

            normalized_query = r.data.get("normalized_query", query) if r.data else query

            # ── Step 2: intent_agent (AI — parses query into structured intent) ──
            flog.info("Parsing query into structured intent using AI.")
            r = await self._call("intent_agent", {
                "query": normalized_query,
                "domain": domain_name,
                "intent_schema": intent_schema,
                "schema_version": sv,
                "request_id": str(request_id),
                "intent_guidance": domain_config.get("intent_guidance", ""),
            }, request_id, user_id, domain_name,
                timeout_ms=15000, retry_budget=2, schema_version=sv)

            state["completed_steps"].append("intent_agent")
            state["ai_call_count"] += 1

            if not r.success:
                await self._fail(request_id, "INIT", f"Intent parsing failed: {r.error}")
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error=str(r.error), ai_call_count=state["ai_call_count"])
                return {"error": f"Intent parsing failed: {r.error}"}

            intent_data = r.data or {}
            parsed_intent = intent_data.get("result", intent_data)
            state["results"]["intent_agent"] = intent_data

            # Inject the original user query so SerpAPI _build_search_query can use it
            # (the AI intent doesn't return raw_query, but url_generation_agent needs it)
            parsed_intent["raw_query"] = state["query"]

            filters = parsed_intent.get("filters", {})
            filters_display = ", ".join(f"{k}={v}" for k, v in filters.items()) if isinstance(filters, dict) and filters else "none"
            req_fields = parsed_intent.get("requested_fields", [])
            req_fields_display = ", ".join(req_fields) if req_fields else "all (no specific field requested)"
            flog.substep("Step 2: Parse Intent (AI)", "OK",
                         f"Query Type: {parsed_intent.get('query_type', '?')}\n"
                         f"Title: {parsed_intent.get('title', 'N/A')}\n"
                         f"Year: {parsed_intent.get('year', 'N/A')}\n"
                         f"Filters: {filters_display}\n"
                         f"Requested Fields: {req_fields_display}",
                         elapsed_ms=r.latency_ms)
            flog.info(
                f"Intent Parsed: {parsed_intent.get('query_type', 'unknown')} | "
                f"Title: {parsed_intent.get('title', 'N/A')} | "
                f"Year: {parsed_intent.get('year', 'N/A')}"
            )

            # ── Step 3: validate_intent_schema ──
            flog.info("Validating intent structure against domain schema.")
            r = await self._call("validate_intent_schema", {
                "parsed_intent": parsed_intent,
                "domain": domain_name,
                "intent_schema": intent_schema,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("validate_intent_schema")
            state["results"]["validate_intent_schema"] = r.data if r.success else {"error": str(r.error)}

            flog.substep("Step 3: Validate Intent Schema", "OK" if r.success else "FAILED",
                         f"Valid: {r.data.get('valid', False) if r.data else False}",
                         elapsed_ms=r.latency_ms)

            if r.success and r.data and not r.data.get("valid", True):
                errors = r.data.get("errors", [])
                await self._fail(request_id, "INIT", f"Intent schema invalid: {errors}")
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error=f"Schema validation: {errors}", ai_call_count=state["ai_call_count"])
                return {"error": f"Intent schema invalid: {errors}"}

            # ── Step 4: validate_intent_constraints ──
            flog.info("Checking business constraints (confidence, actionability).")
            confidence = parsed_intent.get("confidence", 0.8)
            r = await self._call("validate_intent_constraints", {
                "parsed_intent": parsed_intent,
                "confidence": confidence,
                "domain": domain_name,
                "query_type": parsed_intent.get("query_type", "search"),
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("validate_intent_constraints")
            state["results"]["validate_intent_constraints"] = r.data if r.success else {"error": str(r.error)}

            flog.substep("Step 4: Validate Constraints", "OK" if r.success else "FAILED",
                         f"Actionable: {r.data.get('actionable', False) if r.data else False}",
                         elapsed_ms=r.latency_ms)

            if r.success and r.data and not r.data.get("actionable", True):
                reason = r.data.get("reason", "Not actionable")
                await self._fail(request_id, "INIT", reason)
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error=reason, ai_call_count=state["ai_call_count"])
                return {"error": reason}

            # ── Step 4a: resolve_capabilities (NEW — deterministic) ──
            flog.info("Resolving query capabilities into behavioral flags.")
            r = await self._call("resolve_capabilities", {
                "parsed_intent": parsed_intent,
                "num_sites": len(sites),
            }, request_id, user_id, domain_name, schema_version=sv)

            capability_vector = r.data if r.success and r.data else {
                "cardinality": "multiple",
                "needs_ranking": False,
                "needs_filtering": False,
                "single_entity_lookup": False,
                "needs_aggregation": False,
                "active_filters": {},
            }
            state["results"]["resolve_capabilities"] = capability_vector

            flog.substep("Step 4a: Resolve Capabilities", "OK" if r.success else "WARNING",
                         f"Cardinality: {capability_vector.get('cardinality', '?')}",
                         elapsed_ms=r.latency_ms)

            # ── Step 4b: plan_strategy (NEW — deterministic) ──
            flog.info("Planning execution strategy based on capabilities.")
            r = await self._call("plan_strategy", {
                "capability_vector": capability_vector,
                "num_sites": len(sites),
                "domain_config": domain_config,
            }, request_id, user_id, domain_name, schema_version=sv)

            execution_strategy = r.data if r.success and r.data else {
                "strategy": "search_endpoint_lookup",
                "url_pattern_hint": "search",
                "expected_page_type": "list_page",
                "extraction_mode": "multi_item",
                "unification_mode": "single_source",
            }
            state["execution_strategy"] = execution_strategy
            state["results"]["plan_strategy"] = execution_strategy

            flog.substep("Step 4b: Plan Strategy", "OK" if r.success else "WARNING",
                         f"Strategy: {execution_strategy.get('strategy', '?')}",
                         elapsed_ms=r.latency_ms)

            # Transition: INIT → INTENT_DONE → VALIDATED
            await self._transition(state, "INIT", "INTENT_DONE")
            await self._transition(state, "INTENT_DONE", "VALIDATED")

            # ── Step 5: compute_intent_hash ──
            flog.info("Computing deterministic hash for cache lookup.")
            r = await self._call("compute_intent_hash", {
                "parsed_intent": parsed_intent,
                "domain": domain_name,
                "schema_version": sv,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("compute_intent_hash")
            intent_hash = r.data.get("intent_hash", "") if r.success and r.data else ""
            state["results"]["compute_intent_hash"] = r.data if r.success else {}

            flog.substep("Step 5: Compute Intent Hash", "OK" if r.success else "FAILED",
                         f"Hash: {intent_hash[:16]}..." if intent_hash else "No hash",
                         elapsed_ms=r.latency_ms)

            # ── Step 6: check_intent_cache ──
            flog.info("Checking if a cached result exists for this intent.")
            r = await self._call("check_intent_cache", {
                "domain": domain_name,
                "intent_hash": intent_hash,
                "schema_version": sv,
                "parsed_intent": parsed_intent,
                "sites": sites,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("check_intent_cache")
            state["results"]["check_intent_cache"] = r.data if r.success else {}

            if r.success and r.data and r.data.get("cache_hit"):
                # CACHE HIT — return early, skip all extraction
                cached = r.data.get("cached_data", {})
                match_type = r.data.get("match_type", "unknown")
                original_query = r.data.get("original_query", "N/A")
                execution_status = r.data.get("execution_status", "N/A")
                await self._transition(state, "VALIDATED", "CACHE_HIT")
                await self._transition(state, "CACHE_HIT", "COMPLETED")

                total_ms = int((time.monotonic() - start_time) * 1000)
                flog.substep("Step 6: Cache Check", "OK",
                             f"💾 CACHE HIT! Returning cached result.\n"
                             f"Match Type: {match_type}\n"
                             f"Original Query: \"{original_query}\"\n"
                             f"Execution Status: {execution_status}\n"
                             f"Semantic Key: {r.data.get('semantic_key', 'N/A')}\n"
                             f"Skipping all site pipelines.")
                flog.finish("COMPLETED", total_ms,
                            summary=cached, ai_call_count=state["ai_call_count"])
                self._concurrency.cleanup_request(request_id)

                # Persist cached result to requests table so the frontend
                # polling endpoint (GET /v1/requests/:id) can read it.
                # Shape must match store_final_result: {unified_data, source_sites, confidence, resolved_conflicts}
                try:
                    result_json = json.dumps({
                        "unified_data": cached.get("unified_data", {}),
                        "source_sites": cached.get("source_sites", []),
                        "confidence": cached.get("confidence", 0),
                        "resolved_conflicts": cached.get("resolved_conflicts", {}),
                    }, default=str)
                    async with self._db_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE umsa_core.requests
                            SET result = $1::jsonb, updated_at = now()
                            WHERE id = $2
                            """,
                            result_json,
                            request_id,
                        )
                except Exception as e:
                    logger.warning("Failed to persist cache-hit result: %s", e)

                # Return unified_data directly (same shape as normal path)
                result_data = cached.get("unified_data", cached)
                return {
                    "request_id": str(request_id),
                    "status": "COMPLETED",
                    "data": result_data,
                }

            # Build semantic key for logging even on MISS
            from mcp.server.tools.check_intent_cache import _build_semantic_key
            current_semantic_key = _build_semantic_key(parsed_intent, domain_name)
            flog.substep("Step 6: Cache Check", "OK",
                         f"Cache MISS — proceeding to site analysis.\n"
                         f"Semantic Key: {current_semantic_key}\n"
                         f"Intent Hash: {intent_hash[:16]}...",
                         elapsed_ms=r.latency_ms)

            # ════════════════════════════════════════
            # PHASE 2: Site Pipelines (AI-driven internally)
            # ════════════════════════════════════════
            flog.separator("PHASE 2: Site Extraction (AI-driven per site)")
            flog.info(f"Processing {len(sites)} site(s): {sites}")

            # ── Step 7: Site validation cache + DOM signals + relevance ──
            flog.info("Checking site validation cache for previously scored sites.")
            r = await self._call("check_site_validation_cache", {
                "domain": domain_name,
                "intent_hash": intent_hash,
                "schema_version": sv,
                "sites": sites,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("check_site_validation_cache")
            cached_sites = r.data.get("cached_sites", []) if r.success and r.data else []
            uncached_sites = r.data.get("uncached_sites", sites) if r.success and r.data else sites

            flog.substep("Step 7: Site Validation Cache", "OK",
                         f"Cached: {cached_sites or 'none'}\n"
                         f"Need scoring: {uncached_sites or 'none'}")

            # ── Step 8: Extract DOM signals for uncached sites ──
            dom_signals = {}
            for site in uncached_sites:
                flog.info(f"Extracting DOM signals from {site} homepage.")
                r = await self._call("extract_dom_signals", {
                    "site_url": site, "domain": domain_name,
                }, request_id, user_id, domain_name,
                    timeout_ms=10000, retry_budget=1, schema_version=sv)

                if r.success and r.data:
                    dom_signals[site] = r.data
                flog.substep(f"Step 8: DOM Signals ({site})",
                             "OK" if r.success else "WARNING",
                             f"Accessible: {r.data.get('accessible', False) if r.data else False}",
                             elapsed_ms=r.latency_ms)

            # ── Step 9: AI relevance scoring (only if uncached sites exist) ──
            # Start with cached sites that passed threshold as accepted
            accepted_sites = []
            for cs in cached_sites:
                if isinstance(cs, dict):
                    if cs.get("relevance_score", 0) >= 0.3:
                        accepted_sites.append(cs.get("site_url", str(cs)))
                else:
                    accepted_sites.append(str(cs))

            if uncached_sites:
                # ── Deterministic pre-rejection: drop inaccessible sites ──
                scoreable_sites = []
                for site in uncached_sites:
                    signals = dom_signals.get(site, {})
                    accessible = signals.get("accessible", False)
                    if not accessible:
                        flog.info(f"🚫 {site} is INACCESSIBLE — auto-rejected (no AI call needed).")
                        # Save rejection signal as JSON for debugging
                        try:
                            _save_relevance_signal(request_id, site, {
                                "site_url": site,
                                "accessible": False,
                                "auto_rejected": True,
                                "reason": "Site homepage not accessible",
                                "dom_signals": signals,
                            })
                        except Exception:
                            pass
                        continue
                    scoreable_sites.append(site)

                if scoreable_sites:
                    flog.info("Scoring site relevance using AI.")
                    # Build accessibility context for AI prompt
                    site_accessibility = {
                        site: {
                            "accessible": dom_signals.get(site, {}).get("accessible", False),
                            "has_json_ld": dom_signals.get(site, {}).get("has_json_ld", False),
                            "query_relevance_hint": dom_signals.get(site, {}).get("query_relevance_hint", 0.0),
                        }
                        for site in scoreable_sites
                    }
                    r = await self._call("relevance_agent", {
                        "intent": parsed_intent,
                        "candidate_sites": scoreable_sites,
                        "domain": domain_name,
                        "intent_schema": intent_schema,
                        "schema_version": sv,
                        "request_id": str(request_id),
                        "dom_signals": dom_signals,
                        "site_accessibility": site_accessibility,
                    }, request_id, user_id, domain_name,
                        timeout_ms=40000, retry_budget=3, schema_version=sv)

                    state["completed_steps"].append("relevance_agent")
                    state["ai_call_count"] += 1
                    # AI returns {result: {sites: [{site_url, relevance_score}]}}
                    if r.success and r.data:
                        raw_result = r.data.get("result", {})
                        site_scores = raw_result.get("sites", []) if isinstance(raw_result, dict) else []
                    else:
                        site_scores = []
                    state["results"]["relevance_agent"] = r.data if r.success else {}

                    flog.substep("Step 9: Relevance Scoring (AI)", "OK" if r.success else "FAILED",
                                 f"Scores: {site_scores[:5]}",
                                 elapsed_ms=r.latency_ms)

                    # ── Step 10: Store relevance + filter ──
                    if site_scores:
                        r = await self._call("store_site_relevance", {
                            "domain": domain_name,
                            "intent_hash": intent_hash,
                            "schema_version": sv,
                            "site_scores": site_scores,
                            "request_id": str(request_id),
                        }, request_id, user_id, domain_name, schema_version=sv)

                        state["completed_steps"].append("store_site_relevance")
                        if r.success and r.data:
                            # Merge newly accepted sites with cached sites
                            newly_accepted = r.data.get("accepted_sites", [])
                            for ns in newly_accepted:
                                site_str = ns.get("site_url", str(ns)) if isinstance(ns, dict) else str(ns)
                                if site_str not in accepted_sites:
                                    accepted_sites.append(site_str)
                            rejected = r.data.get("rejected_sites", [])

                            # Save relevance signals as JSON for each scored site
                            for score_entry in site_scores:
                                try:
                                    site_name = score_entry.get("site_url", "unknown")
                                    _save_relevance_signal(request_id, site_name, {
                                        "site_url": site_name,
                                        "relevance_score": score_entry.get("relevance_score", 0),
                                        "reasoning": score_entry.get("reasoning", ""),
                                        "accessible": dom_signals.get(site_name, {}).get("accessible", False),
                                        "dom_signals_summary": {
                                            k: v for k, v in dom_signals.get(site_name, {}).items()
                                            if k in ("accessible", "has_json_ld", "schema_org_types",
                                                     "query_relevance_hint", "query_keyword_matches")
                                        },
                                    })
                                except Exception:
                                    pass

                            flog.substep("Step 10: Store Relevance", "OK",
                                         f"Accepted: {accepted_sites}\n"
                                         f"Rejected: {rejected}")

            await self._transition(state, "VALIDATED", "RELEVANCE_DONE")

            if not accepted_sites:
                await self._fail(request_id, state["current_state"],
                                 "No sites passed relevance threshold")
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error="No relevant sites", ai_call_count=state["ai_call_count"])
                return {"error": "No sites passed relevance threshold"}

            # ── Step 11: Create site pipelines ──
            r = await self._call("create_site_pipeline", {
                "request_id": str(request_id),
                "domain": domain_name,
                "sites": accepted_sites,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("create_site_pipeline")
            pipeline_ids = r.data.get("pipeline_ids", {}) if r.success and r.data else {}

            flog.substep("Step 11: Create Pipelines", "OK",
                         f"Pipelines created for {len(accepted_sites)} site(s)")

            await self._transition(state, "RELEVANCE_DONE", "PIPELINES_RUNNING")

            # ── Step 12: Run site pipelines (AI-driven internally) ──
            from pipelines.site_pipeline import SitePipeline

            pipeline_runner = SitePipeline(
                gateway={
                    "tool_registry": self._tool_registry,
                    "policy_engine": self._policy_engine,
                },
                execution_logger=self._exec_logger,
                concurrency_manager=self._concurrency,
                db_pool=self._db_pool,
                metrics_collector=self._metrics,
            )

            extractions = []
            site_urls = []
            for s in accepted_sites:
                if isinstance(s, dict):
                    site_urls.append(s.get("site_url", str(s)))
                else:
                    site_urls.append(str(s))

            # ── Pre-expand queries for multi-item (call Groq ONCE, not per-site) ──
            # Only expand for ACTUAL multi-item queries. Respect the capability
            # resolver's cardinality and strategy over the raw query_type — the
            # resolver already accounts for edge cases (e.g. "sirai" parsed as
            # query_type=search but cardinality=single → entity detail lookup).
            query_type = parsed_intent.get("query_type", "").lower()
            cardinality = capability_vector.get("cardinality", "single")
            strategy = execution_strategy.get("strategy", "")

            is_multi_item_query = False
            if cardinality == "multiple":
                is_multi_item_query = True
            elif query_type in ("list",) and cardinality != "single":
                # Explicit list intent that wasn't overridden to single
                is_multi_item_query = True
            elif parsed_intent.get("limit") and int(parsed_intent.get("limit", 0)) > 1:
                is_multi_item_query = True

            # Entity detail lookups are NEVER multi-item
            if strategy in ("entity_detail_lookup", "direct_url_lookup"):
                is_multi_item_query = False

            if is_multi_item_query:
                parsed_intent["_is_single_entity"] = False
                try:
                    qe_result = await self._call("query_expander", {
                        "intent": parsed_intent,
                    }, request_id, user_id, domain_name,
                        timeout_ms=15000, retry_budget=1, schema_version=sv,
                        caller="orchestrator")

                    if qe_result.success and qe_result.data:
                        expanded_queries = qe_result.data.get("expanded_queries", [])
                    else:
                        expanded_queries = []

                    if expanded_queries and len(expanded_queries) >= 2:
                        parsed_intent["_expanded_queries"] = expanded_queries
                        flog.substep("Step 11b: Query Expansion (Groq AI)", "OK",
                                     f"Generated {len(expanded_queries)} search queries:\n" +
                                     "\n".join(f"  {i+1}. \"{q}\"" for i, q in enumerate(expanded_queries)),
                                     elapsed_ms=qe_result.latency_ms)
                    else:
                        flog.substep("Step 11b: Query Expansion (Groq AI)", "WARNING",
                                     f"Groq returned {len(expanded_queries) if expanded_queries else 0} queries — using raw query fallback",
                                     elapsed_ms=qe_result.latency_ms)
                except Exception as e:
                    logger.warning("Query expansion failed: %s — will use raw query per-site", e)
                    flog.substep("Step 11b: Query Expansion (Groq AI)", "WARNING",
                                 f"Failed: {e} — sites will use raw query fallback")
            else:
                parsed_intent["_is_single_entity"] = True
                parsed_intent["_execution_strategy"] = execution_strategy
                flog.substep("Step 11b: Query Expansion", "SKIPPED",
                             f"Single-entity query (cardinality={cardinality}, strategy={strategy}) — no expansion needed")

            for i, site_url in enumerate(site_urls):
                if i > 0:
                    await asyncio.sleep(3)  # Rate limit between sites

                pipeline_id = pipeline_ids.get(site_url, str(uuid4()))
                flog.info(f"Running site pipeline for {site_url} ({i+1}/{len(site_urls)})")

                try:
                    result = await pipeline_runner.run(
                        site_url=site_url,
                        intent=parsed_intent,
                        domain_module=domain_module,
                        request_id=request_id,
                        user_id=user_id,
                        pipeline_id=UUID(pipeline_id) if isinstance(pipeline_id, str) else pipeline_id,
                        file_logger=flog,
                        execution_strategy=state.get("execution_strategy", {}),
                    )
                    if result.get("success") and result.get("extracted_data"):
                        extraction_entry = {
                            "source_site": site_url,
                            "extracted_data": result["extracted_data"],
                            "confidence": result.get("confidence", 0.0),
                        }
                        # Propagate multi-item results
                        if result.get("is_multi_item") and result.get("extracted_items"):
                            extraction_entry["is_multi_item"] = True
                            extraction_entry["extracted_items"] = result["extracted_items"]
                            extraction_entry["items_count"] = result.get("items_count", len(result["extracted_items"]))
                        extractions.append(extraction_entry)
                except Exception as e:
                    logger.error("Site pipeline failed for %s: %s", site_url, e)
                    flog.substep(f"Pipeline: {site_url}", "FAILED", str(e))

            state["completed_steps"].append("run_site_pipelines")
            await self._transition(state, "PIPELINES_RUNNING", "EXTRACTION_DONE")

            if not extractions:
                await self._fail(request_id, state["current_state"],
                                 "No sites returned extracted data")
                flog.finish("FAILED", int((time.monotonic() - start_time) * 1000),
                            error="All site pipelines failed",
                            ai_call_count=state["ai_call_count"])
                return {"error": "All site pipelines failed to extract data"}

            flog.substep("Site Pipelines Summary", "OK",
                         f"Successful extractions: {len(extractions)}/{len(accepted_sites)}")

            # ════════════════════════════════════════
            # PHASE 3: Unification (deterministic + AI if conflicts)
            # ════════════════════════════════════════
            flog.separator("PHASE 3: Data Unification")

            # ── Check if any site returned multi-item results ──
            has_multi_item = any(e.get("is_multi_item") for e in extractions)

            if has_multi_item:
                # Multi-item mode: collect items per-site, then unify
                from mcp.client.multi_item_unifier import unify_multi_items

                sites_data = {}
                total_items = 0
                for ext in extractions:
                    site = ext["source_site"]
                    if ext.get("is_multi_item") and ext.get("extracted_items"):
                        sites_data[site] = {
                            "items": ext["extracted_items"],
                            "items_count": len(ext["extracted_items"]),
                            "confidence": ext.get("confidence", 0.0),
                        }
                        total_items += len(ext["extracted_items"])
                    else:
                        # Single-item result from this site
                        sites_data[site] = {
                            "items": [ext["extracted_data"]],
                            "items_count": 1,
                            "confidence": ext.get("confidence", 0.0),
                        }
                        total_items += 1

                # Cross-site deduplication
                unified_items = unify_multi_items(sites_data)

                unified_data = {
                    "is_multi_item": True,
                    "sites": sites_data,
                    "unified_items": unified_items,
                    "total_items_raw": total_items,
                    "total_items": len(unified_items),
                }
                state["completed_steps"].append("pre_normalize")

                flog.substep("Step 13: Multi-Item Aggregation", "OK",
                             f"Mode: multi_item (cross-site dedup)\n"
                             f"Sites: {list(sites_data.keys())}\n"
                             f"Raw items: {total_items}\n"
                             f"After dedup: {len(unified_items)}")
                flog.info("Multi-item results — cross-site deduplication applied.")

            else:
                # Single-item mode: normal unification
                from mcp.client.pre_unification import pre_normalize, deterministic_dedup

                site_trust_weights = domain_config.get("site_trust_weights", {})
                candidates = pre_normalize(extractions, site_trust_weights=site_trust_weights)
                pre_unified, conflict_fields = deterministic_dedup(candidates)

                state["completed_steps"].append("pre_normalize")

                flog.substep("Step 13: Pre-Normalize & Dedup", "OK",
                             f"Fields unified: {len(pre_unified)}\n"
                             f"Conflicts requiring AI: {conflict_fields or 'none'}")

                # ── Step 13b: Query-Focused AI Extraction ──
                # When requested_fields exist, use AI to extract clean structured
                # values from text data (description, articleBody) before unification
                requested_fields = parsed_intent.get("requested_fields", [])
                if requested_fields and pre_unified:
                    t0_qfe = time.monotonic()
                    try:
                        from app.core.config import settings
                        import httpx as _httpx

                        # Build clean data summary — filter CSS noise, keep JSON-LD
                        # Priority tiers for QFE:
                        #   Tier 1: JSON-LD structured fields (actor, director, genre, etc.)
                        #   Tier 2: Text content (description, articleBody, name)
                        #   Tier 3: Other meaningful fields
                        jsonld_fields = {}  # Highest priority
                        text_content_fields = {}  # High priority
                        clean_fields = {}  # Lower priority

                        # JSON-LD keys that carry movie data
                        JSONLD_KEYS = {
                            "@type", "actor", "director", "genre", "name",
                            "dateCreated", "datePublished", "aggregateRating",
                            "description", "image", "url", "video",
                            "duration", "contentRating", "review",
                            "productionCompany", "countryOfOrigin",
                        }
                        # Requested field names for matching
                        req_lower = {f.lower() for f in requested_fields}

                        for k, v in pre_unified.items():
                            if v is None or not str(v).strip():
                                continue
                            k_lower = k.lower()

                            # Skip internal/source fields
                            if k.startswith("_") or k in ("source_url", "source_site"):
                                continue

                            # Skip CSS noise
                            if k_lower.startswith("text:"):
                                css_words = (
                                    "color", "display", "position", "margin", "padding",
                                    "border", "font-", "width", "height", "flex",
                                    "grid", "transform", "transition", "animation",
                                    "opacity", "overflow", "z-index", "cursor",
                                    "background", "box-shadow", "outline", "align",
                                    "justify", "visibility", "pointer", "min-",
                                    "max-", "gap", "scale", "rotate", "inset",
                                    "top", "left", "right", "bottom", "float",
                                    "letter-spacing", "text-align", "text-decoration",
                                    "text-indent", "text-overflow", "text-transform",
                                    "white-space", "word-break", "line-height",
                                    "tab-size", "resize", "appearance", "fill",
                                    "shape-rendering", "will-change", "backface",
                                    "-webkit", "box-sizing", "list-style",
                                    "vertical-align", "border-collapse",
                                )
                                suffix = k_lower.replace("text:", "", 1)
                                if suffix.startswith("--") or any(suffix.startswith(cw) for cw in css_words):
                                    continue

                            # Skip og:/twitter:/meta: duplicates
                            if k_lower.startswith(("og:", "twitter:", "meta:")):
                                continue

                            # Tier 1: JSON-LD structured fields or fields matching requested
                            if k in JSONLD_KEYS or k_lower in req_lower:
                                # Allow large values for structured arrays (actor, director, etc.)
                                val = json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (list, dict)) else str(v)
                                jsonld_fields[k] = val[:3000]
                            # Tier 2: Text content
                            elif k_lower in ("description", "articlebody", "headline",
                                             "name", "keywords", "articlesection"):
                                text_content_fields[k] = str(v)[:1500]
                            # Tier 3: Other fields
                            else:
                                clean_fields[k] = str(v)[:300]

                        # Build data summary for AI — priority order
                        data_parts = []
                        for k, v in jsonld_fields.items():
                            data_parts.append(f"[STRUCTURED] {k}: {v}")
                        for k, v in text_content_fields.items():
                            data_parts.append(f"[TEXT] {k}: {v}")
                        for k, v in list(clean_fields.items())[:30]:
                            data_parts.append(f"  {k}: {v}")

                        data_text = "\n".join(data_parts)
                        user_query = parsed_intent.get("title", "") or query or ""
                        fields_json = json.dumps(requested_fields)

                        qfe_prompt = (
                            "You are a data extraction specialist. Given raw data scraped from "
                            "web pages, extract SPECIFIC structured values that "
                            "answer the user's query.\n\n"
                            f"User Query: \"{user_query}\"\n"
                            f"Requested Fields: {fields_json}\n\n"
                            "INSTRUCTIONS:\n"
                            "1. Read ALL the text content carefully (description, articleBody)\n"
                            "2. Extract CLEAN structured values for each requested field\n"
                            "3. Return values as numbers when possible (e.g., 383.6 not '₹383.6 crores')\n"
                            "4. Include currency/unit as a separate field when relevant\n"
                            "5. ALSO extract any other useful structured data you find in the text "
                            "as 'suggestions' — things like movie title, director, cast, release_date, "
                            "genre, runtime, verdict, language, production_house etc.\n"
                            "6. Never hallucinate — only return data you actually find in the text\n\n"
                            f"Scraped Data:\n{data_text}\n\n"
                            "Respond with JSON:\n"
                            "{\n"
                            '  "query_answer": {\n'
                            '    "<field_name>": <value>,\n'
                            '    ... (only fields you found data for)\n'
                            "  },\n"
                            '  "suggestions": {\n'
                            '    "<field_name>": "<clean_value>",\n'
                            '    ... (other useful data found: title, director, cast, genre, etc.)\n'
                            "  },\n"
                            '  "confidence": 0.0-1.0,\n'
                            '  "source_text": "brief excerpt showing where you found the data"\n'
                            "}"
                        )

                        async with _httpx.AsyncClient(timeout=12.0) as ai_client:
                            ai_resp = await ai_client.post(
                                f"{settings.ai_base_url}/chat/completions",
                                headers={
                                    "Authorization": f"Bearer {settings.ai_api_key_phase3}",
                                    "Content-Type": "application/json",
                                },
                                json={
                                    "model": settings.ai_model,
                                    "messages": [
                                        {"role": "system",
                                         "content": "You are a precise data extractor. "
                                                    "Extract specific factual values from text. "
                                                    "Never hallucinate — only return data you find in the text."},
                                        {"role": "user", "content": qfe_prompt},
                                    ],
                                    "temperature": 0.0,
                                    "response_format": {"type": "json_object"},
                                },
                            )
                            ai_resp.raise_for_status()
                            qfe_data = ai_resp.json()

                        qfe_content = qfe_data["choices"][0]["message"]["content"]
                        qfe_result = json.loads(qfe_content)
                        query_answer = qfe_result.get("query_answer", {})
                        suggestions = qfe_result.get("suggestions", {})
                        qfe_confidence = qfe_result.get("confidence", 0)
                        qfe_source = qfe_result.get("source_text", "")

                        state["ai_call_count"] += 1
                        state["completed_steps"].append("query_focused_extraction")

                        if query_answer or suggestions:
                            # Merge query_answer and suggestions into pre_unified
                            if query_answer:
                                pre_unified["query_answer"] = query_answer
                            if suggestions:
                                pre_unified["suggestions"] = suggestions
                            pre_unified["_qfe_confidence"] = qfe_confidence

                            qfe_ms = int((time.monotonic() - t0_qfe) * 1000)
                            suggestion_preview = (
                                f"\nSuggestions: {json.dumps(suggestions, ensure_ascii=False)[:200]}"
                                if suggestions else ""
                            )
                            flog.substep("Step 13b: Query-Focused Extraction", "OK",
                                         f"Requested: {fields_json}\n"
                                         f"Found: {json.dumps(query_answer, ensure_ascii=False)[:300]}\n"
                                         f"Confidence: {qfe_confidence:.0%}\n"
                                         f"Source: {qfe_source[:150]}"
                                         f"{suggestion_preview}",
                                         elapsed_ms=qfe_ms)
                        else:
                            qfe_ms = int((time.monotonic() - t0_qfe) * 1000)
                            flog.substep("Step 13b: Query-Focused Extraction", "WARNING",
                                         f"AI found no values for requested fields: {fields_json}",
                                         elapsed_ms=qfe_ms)

                    except Exception as qfe_err:
                        qfe_ms = int((time.monotonic() - t0_qfe) * 1000)
                        logger.warning("Query-Focused Extraction failed: %s", qfe_err)
                        flog.substep("Step 13b: Query-Focused Extraction", "WARNING",
                                     f"Non-critical: {str(qfe_err)[:200]}",
                                     elapsed_ms=qfe_ms)

                # ── Step 14: AI unification (only if conflicts exist AND user opted in) ──
                unified_data = pre_unified
                if conflict_fields and unify:
                    flog.info(f"Resolving {len(conflict_fields)} conflict(s) using AI unification.")
                    r = await self._call("unification_agent", {
                        "candidates": candidates,
                        "conflict_fields": conflict_fields,
                        "pre_unified": pre_unified,
                        "extraction_schema": extraction_schema,
                        "domain": domain_name,
                        "schema_version": sv,
                        "request_id": str(request_id),
                    }, request_id, user_id, domain_name,
                        timeout_ms=15000, retry_budget=2, schema_version=sv)

                    state["ai_call_count"] += 1
                    state["completed_steps"].append("unification_agent")

                    if r.success and r.data:
                        unified_data = r.data.get("unified_record", pre_unified)
                    flog.substep("Step 14: AI Unification", "OK" if r.success else "WARNING",
                                 f"Resolved: {conflict_fields}",
                                 elapsed_ms=r.latency_ms)
                elif conflict_fields and not unify:
                    flog.info(f"Conflicts detected ({conflict_fields}) but unify=false — skipping AI unification.")
                    flog.substep("Step 14: AI Unification", "SKIPPED",
                                 f"User did not opt-in (unify=false). Conflicts: {conflict_fields}")
                else:
                    flog.info("No conflicts detected. Skipping AI unification.")

            await self._transition(state, "EXTRACTION_DONE", "UNIFIED")

            # ── Step 15: Store final result ──
            # Build clean result: prioritize query_answer over raw field dump
            source_sites = [e["source_site"] for e in extractions]
            avg_confidence = sum(e.get("confidence", 0) for e in extractions) / max(len(extractions), 1)

            # If QFE produced a query_answer, build a clean result
            qfe_answer = unified_data.get("query_answer") if isinstance(unified_data, dict) else None
            qfe_suggs = unified_data.get("suggestions") if isinstance(unified_data, dict) else None
            qfe_conf = unified_data.get("_qfe_confidence", 0) if isinstance(unified_data, dict) else 0

            if qfe_answer:
                # Clean result: query_answer first, suggestions second, key metadata
                clean_result = {
                    "query_answer": qfe_answer,
                }
                if qfe_suggs:
                    clean_result["suggestions"] = qfe_suggs
                clean_result["_qfe_confidence"] = qfe_conf
                # Include key JSON-LD structured data if present
                for key in ("@type", "actor", "director", "genre", "name",
                            "aggregateRating", "dateCreated", "description",
                            "image", "url", "video"):
                    if key in unified_data and key not in clean_result:
                        clean_result[key] = unified_data[key]
                final_data = clean_result
                final_confidence = float(qfe_conf) if qfe_conf else avg_confidence
            else:
                final_data = unified_data
                final_confidence = avg_confidence

            r = await self._call("store_final_result", {
                "request_id": str(request_id),
                "domain": domain_name,
                "intent_hash": intent_hash,
                "schema_version": sv,
                "unified_data": final_data,
                "source_sites": source_sites,
                "confidence": final_confidence,
                "parsed_intent": parsed_intent,
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("store_final_result")

            flog.substep("Step 15: Store Result", "OK" if r.success else "FAILED",
                         f"Sources: {source_sites}\n"
                         f"Confidence: {final_confidence:.0%}",
                         elapsed_ms=r.latency_ms)

            await self._transition(state, "UNIFIED", "COMPLETED")

            # ── Step 16: Audit log ──
            total_ms = int((time.monotonic() - start_time) * 1000)
            await self._call("audit_log_event", {
                "request_id": str(request_id),
                "event_type": "REQUEST_COMPLETED",
                "domain": domain_name,
                "latency_ms": total_ms,
                "ai_call_count": state["ai_call_count"],
                "metadata": {
                    "sites": source_sites,
                    "confidence": avg_confidence,
                    "steps": state["completed_steps"],
                },
            }, request_id, user_id, domain_name, schema_version=sv)

            state["completed_steps"].append("audit_log_event")

            # Build readable summary for the log finish block
            if isinstance(unified_data, dict) and unified_data.get("is_multi_item"):
                # Multi-item: show unified deduplicated list
                u_items = unified_data.get("unified_items", [])
                summary_dict = {
                    "mode": "multi_item",
                    "total_items (raw)": unified_data.get("total_items_raw", 0),
                    "total_items (deduped)": len(u_items),
                }
                for idx, item in enumerate(u_items):
                    item_title = (
                        item.get('_heading')
                        or item.get('name')
                        or item.get('title')
                        or item.get('headline')
                        or item.get('_primary_link_text')
                        or '?'
                    )
                    if len(str(item_title)) > 60:
                        item_title = str(item_title)[:57] + '...'
                    sources = item.get('_source_sites', [])
                    label = f"  {idx+1}. {item_title}"
                    summary_dict[label] = f"[{', '.join(sources)}]" if sources else ""
                flog.finish("COMPLETED", total_ms,
                            summary=summary_dict,
                            ai_call_count=state["ai_call_count"],
                            sources=source_sites)
            else:
                flog.finish("COMPLETED", total_ms,
                            summary=final_data,
                            ai_call_count=state["ai_call_count"],
                            sources=source_sites)

            self._concurrency.cleanup_request(request_id)
            return {
                "request_id": str(request_id),
                "status": "COMPLETED",
                "data": unified_data,
            }

        except Exception as e:
            logger.error("Orchestrator exception: %s", e, exc_info=True)
            total_ms = int((time.monotonic() - start_time) * 1000)
            flog.finish("FAILED", total_ms,
                        error=str(e), ai_call_count=state.get("ai_call_count", 0))
            try:
                await self._fail(request_id, state.get("current_state", "INIT"), str(e))
            except Exception:
                pass
            self._concurrency.cleanup_request(request_id)
            return {"error": str(e)}


def _save_relevance_signal(request_id, site_domain, data):
    """Save site relevance signal as JSON for debugging."""
    import os
    import json
    from datetime import datetime

    signals_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "site_relevance_signals",
    )
    os.makedirs(signals_dir, exist_ok=True)

    clean_domain = site_domain.replace(".", "_").replace("/", "_")
    filename = f"{request_id}_{clean_domain}.json"
    filepath = os.path.join(signals_dir, filename)

    data["request_id"] = str(request_id)
    data["timestamp"] = datetime.now().isoformat()

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
