"""
FastAPI Application Bootstrap — UMSA

Initializes DB pool, tool registry, domain registry, agents,
and mounts routes + Prometheus metrics endpoint.
"""

import asyncio
import sys

# Windows event loop policy fix for Playwright/Subprocesses (MUST BE DONE FIRST)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.core.config import settings



logger = logging.getLogger("umsa.main")

# Global app state — set during lifespan
_app_state: Dict[str, Any] = {}

print(f"DEBUG: Current event loop policy: {type(asyncio.get_event_loop_policy())}")
try:
    loop = asyncio.get_running_loop()
    print(f"DEBUG: Current running loop: {type(loop)}")
except RuntimeError:
    print("DEBUG: No running loop yet")


def get_app_state() -> Dict[str, Any]:
    """Access global app state from routes and other modules."""
    return _app_state


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan: startup and shutdown.
    Initializes DB pool, loads tool registry, loads domains,
    registers tool implementations, creates coordinator.
    """
    global _app_state

    # Configure logging with rotation
    import os as _os
    from logging.handlers import RotatingFileHandler

    log_dir = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "logs")
    _os.makedirs(log_dir, exist_ok=True)

    log_formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    # Rotating file handler: 10MB max, 5 backups
    file_handler = RotatingFileHandler(
        _os.path.join(log_dir, "umsa_app.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)

    # Console handler (keeps stdout output for dev)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logger.info("Starting UMSA...")

    # ── Initialize DB pool ──
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
    )
    logger.info("Database pool created (min=%d, max=%d)", settings.db_pool_min, settings.db_pool_max)

    # ── Load tool registry ──
    from mcp.server.tool_registry import ToolRegistryManager

    tool_registry = ToolRegistryManager()
    await tool_registry.load(db_pool)

    # ── Self-healing: ensure capability resolution tools exist ──
    _SELF_HEAL_TOOLS = [
        {
            "tool_name": "resolve_capabilities",
            "role": "deterministic",
            "allowed_callers": ["coordinator", "orchestrator"],
            "domain_scope": ["*"],
            "input_schema": '{"type": "object", "required": ["parsed_intent", "num_sites"], "properties": {"parsed_intent": {"type": "object"}, "num_sites": {"type": "integer", "minimum": 0}}}',
            "timeout_seconds": 2,
            "retry_count": 0,
            "critical_flag": True,
            "description": "Step 4a: Deterministic capability vector from parsed intent",
        },
        {
            "tool_name": "plan_strategy",
            "role": "deterministic",
            "allowed_callers": ["coordinator", "orchestrator"],
            "domain_scope": ["*"],
            "input_schema": '{"type": "object", "required": ["capability_vector", "num_sites"], "properties": {"capability_vector": {"type": "object"}, "num_sites": {"type": "integer", "minimum": 0}, "domain_config": {"type": "object"}}}',
            "timeout_seconds": 2,
            "retry_count": 0,
            "critical_flag": True,
            "description": "Step 4b: Deterministic execution strategy from capability vector",
        },
    ]
    for tool_def in _SELF_HEAL_TOOLS:
        try:
            exists = await db_pool.fetchval(
                "SELECT 1 FROM umsa_core.tool_registry WHERE tool_name = $1",
                tool_def["tool_name"],
            )
            if not exists:
                await db_pool.execute(
                    """
                    INSERT INTO umsa_core.tool_registry
                        (tool_name, role, allowed_callers, domain_scope,
                         input_schema, timeout_seconds, retry_count,
                         critical_flag, description)
                    VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)
                    ON CONFLICT (tool_name) DO NOTHING
                    """,
                    tool_def["tool_name"], tool_def["role"],
                    tool_def["allowed_callers"], tool_def["domain_scope"],
                    tool_def["input_schema"],
                    tool_def["timeout_seconds"], tool_def["retry_count"],
                    tool_def["critical_flag"], tool_def["description"],
                )
                logger.info("Self-healed: registered missing tool '%s'", tool_def["tool_name"])
            await tool_registry.load(db_pool)  # Reload to pick up any new tools
        except Exception as e:
            logger.warning("Self-heal check for '%s' failed: %s", tool_def["tool_name"], e)

    # ── Load domain registry ──
    from domains.registry import DomainRegistry

    domain_registry = DomainRegistry()
    await domain_registry.load_all(db_pool)

    # ── Initialize policy engine ──
    from mcp.server.policy_engine import PolicyEngine

    policy_engine = PolicyEngine()

    # Load domain-specific policy rules
    for domain_name in domain_registry.list_active():
        domain_module = domain_registry.get(domain_name)
        if domain_module and "policy_rules" in domain_module:
            policy_engine.load_rules(
                domain_name,
                domain_module["policy_rules"].check,
            )

    # ── Initialize execution logger ──
    from mcp.server.logging import ExecutionLogger

    execution_logger = ExecutionLogger(db_pool)

    # ── Initialize concurrency manager ──
    from app.core.concurrency import concurrency_manager

    # ── Initialize metrics collector ──
    metrics_collector = MetricsCollector()



    # ── Register ALL 26 tool implementations (P2.20) ──
    from mcp.server.gateway import register_tool_implementation

    # --- Deterministic Tools (20) ---
    from mcp.server.tools import normalize_request
    from mcp.server.tools import validate_intent_schema
    from mcp.server.tools import validate_intent_constraints
    from mcp.server.tools import resolve_capabilities
    from mcp.server.tools import plan_strategy
    from mcp.server.tools import compute_intent_hash
    from mcp.server.tools import check_intent_cache
    from mcp.server.tools import check_site_validation_cache
    from mcp.server.tools import extract_dom_signals
    from mcp.server.tools import store_site_relevance
    from mcp.server.tools import create_site_pipeline
    from mcp.server.tools import fetch_robots_txt
    from mcp.server.tools import parse_robots_rules
    from mcp.server.tools import store_allowed_paths
    from mcp.server.tools import check_url_pattern_cache
    from mcp.server.tools import inspect_url_dom
    from mcp.server.tools import select_best_url
    from mcp.server.tools import store_url_pattern
    from mcp.server.tools import scrape_structured_data
    from mcp.server.tools import sub_page_navigator
    from mcp.server.tools import validate_extraction
    from mcp.server.tools import store_final_result
    from mcp.server.tools import audit_log_event

    register_tool_implementation("normalize_request", normalize_request.execute)
    register_tool_implementation("validate_intent_schema", validate_intent_schema.execute)
    register_tool_implementation("validate_intent_constraints", validate_intent_constraints.execute)
    register_tool_implementation("resolve_capabilities", resolve_capabilities.execute)
    register_tool_implementation("plan_strategy", plan_strategy.execute)
    register_tool_implementation("compute_intent_hash", compute_intent_hash.execute)
    register_tool_implementation("check_intent_cache", check_intent_cache.execute)
    register_tool_implementation("check_site_validation_cache", check_site_validation_cache.execute)
    register_tool_implementation("extract_dom_signals", extract_dom_signals.execute)
    register_tool_implementation("store_site_relevance", store_site_relevance.execute)
    register_tool_implementation("create_site_pipeline", create_site_pipeline.execute)
    register_tool_implementation("fetch_robots_txt", fetch_robots_txt.execute)
    register_tool_implementation("parse_robots_rules", parse_robots_rules.execute)
    register_tool_implementation("store_allowed_paths", store_allowed_paths.execute)
    register_tool_implementation("check_url_pattern_cache", check_url_pattern_cache.execute)
    register_tool_implementation("inspect_url_dom", inspect_url_dom.execute)
    register_tool_implementation("select_best_url", select_best_url.execute)
    register_tool_implementation("store_url_pattern", store_url_pattern.execute)
    register_tool_implementation("scrape_structured_data", scrape_structured_data.execute)
    register_tool_implementation("sub_page_navigator", sub_page_navigator.execute)
    register_tool_implementation("validate_extraction", validate_extraction.execute)
    register_tool_implementation("store_final_result", store_final_result.execute)
    register_tool_implementation("audit_log_event", audit_log_event.execute)

    # --- AI Agents (8) ---
    from mcp.client.agents.intent_agent import IntentAgent
    from mcp.client.agents.relevance_agent import RelevanceAgent
    from mcp.client.agents.url_generation_agent import URLGenerationAgent
    from mcp.client.agents.scoring_agent import ScoringAgent
    # intent_classifier removed — replaced by plan_strategy (capability resolution)
    from mcp.client.agents.unification_agent import UnificationAgent
    from mcp.client.agents.query_expander import QueryExpander
    from mcp.client.agents.dom_validation_agent import score_dom_relevance
    from mcp.client.agents.ai_extractor_agent import AIExtractorAgent

    register_tool_implementation("intent_agent", IntentAgent.execute)
    register_tool_implementation("relevance_agent", RelevanceAgent.execute)
    register_tool_implementation("url_agent", URLGenerationAgent.execute)
    register_tool_implementation("scoring_agent", ScoringAgent.execute)
    register_tool_implementation("unification_agent", UnificationAgent.execute)
    register_tool_implementation("query_expander", QueryExpander.execute)
    register_tool_implementation("dom_validation_agent", score_dom_relevance)
    register_tool_implementation("ai_extractor_agent", AIExtractorAgent.execute)

    logger.info("Registered 31 tool implementations (23 deterministic + 8 AI)")

    # ── Create coordinator ──
    from mcp.client.coordinator import RequestCoordinator

    coordinator = RequestCoordinator(
        db_pool=db_pool,
        tool_registry=tool_registry,
        policy_engine=policy_engine,
        execution_logger=execution_logger,
        concurrency_manager=concurrency_manager,
        domain_registry=domain_registry,
        metrics_collector=metrics_collector,
    )

    # ── Start background cleanup task (P3.6) ──
    from app.core.cleanup import start_cleanup_loop

    cleanup_task = asyncio.create_task(
        start_cleanup_loop(db_pool, execution_logger, interval_seconds=60)
    )
    logger.info("Background cleanup loop started")

    # ── Store app state ──
    _app_state = {
        "db_pool": db_pool,
        "tool_registry": tool_registry,
        "domain_registry": domain_registry,
        "policy_engine": policy_engine,
        "execution_logger": execution_logger,
        "concurrency_manager": concurrency_manager,
        "coordinator": coordinator,
        "metrics_collector": metrics_collector,

    }

    logger.info("UMSA started successfully (MCP Remediation v2)")
    yield

    # Cancel cleanup task
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    # ── Shutdown ──
    logger.info("Shutting down UMSA...")
    await db_pool.close()
    logger.info("UMSA shutdown complete")


# ============================================================
# Metrics Collector (wraps Prometheus metrics)
# ============================================================

class MetricsCollector:
    """Thin wrapper around Prometheus metrics for use in gateway."""

    def record_tool_invocation(
        self, domain: str, tool_name: str, caller: str, status: str
    ):
        from observability.metrics import TOOL_INVOCATIONS
        TOOL_INVOCATIONS.labels(
            domain=domain, tool_name=tool_name, caller=caller, status=status
        ).inc()

    def record_tool_duration(
        self, domain: str, tool_name: str, caller: str, duration_seconds: float
    ):
        from observability.metrics import TOOL_DURATION
        TOOL_DURATION.labels(
            domain=domain, tool_name=tool_name, caller=caller
        ).observe(duration_seconds)

    def record_semaphore_wait(self, semaphore_type: str, wait_ms: int):
        from observability.metrics import SEMAPHORE_WAIT
        SEMAPHORE_WAIT.labels(semaphore_type=semaphore_type).observe(wait_ms)

    def record_domain_failure(
        self, domain: str, site_domain: str, failure_class: str
    ):
        from observability.metrics import DOMAIN_FAILURES
        DOMAIN_FAILURES.labels(
            domain=domain, site_domain=site_domain, failure_class=failure_class
        ).inc()

    def record_cache_hit(self, domain: str, stage: str):
        from observability.metrics import CACHE_HITS
        CACHE_HITS.labels(domain=domain, stage=stage).inc()

    def record_cache_miss(self, domain: str, stage: str):
        from observability.metrics import CACHE_MISSES
        CACHE_MISSES.labels(domain=domain, stage=stage).inc()


# ============================================================
# FastAPI App
# ============================================================

app = FastAPI(
    title="UMSA — Universal Movie Scraper Agent",
    description="Enterprise-grade domain-extensible MCP scraping system",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS (Section 16 — strict origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OPTIONS handler for CORS preflight (extra safety)
@app.options("/{rest_of_path:path}")
async def preflight_handler(rest_of_path: str):
    return {}

# Mount API routes
from app.api.routes import router as api_router

app.include_router(api_router)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
