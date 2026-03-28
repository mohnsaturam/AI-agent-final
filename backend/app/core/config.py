"""
Core configuration — loads all settings from environment variables.
All safety limits from Section 6 are defined here.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # --- Database ---
    database_url: str = Field(
        default="",
        alias="DATABASE_URL",
    )
    db_pool_min: int = Field(default=5, alias="DB_POOL_MIN")
    db_pool_max: int = Field(default=30, alias="DB_POOL_MAX")

    # --- JWT ---
    jwt_secret: str = Field(default="", alias="JWT_SECRET")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")

    # --- AI Provider ---
    # Phase 1: Unification + Intent understanding + Site relevance scoring
    ai_api_key_phase1: str = Field(default="", alias="AI_API_KEY_PHASE1")
    # Phase 2: DOM scoring
    ai_api_key_phase2: str = Field(default="", alias="AI_API_KEY_PHASE2")
    # Phase 3: Query expansion
    ai_api_key_phase3: str = Field(default="", alias="AI_API_KEY_PHASE3")
    ai_base_url: str = Field(
        default="https://api.groq.com/openai/v1",
        alias="AI_BASE_URL",
    )
    ai_model: str = Field(default="llama-3.3-70b-versatile", alias="AI_MODEL")
    ai_model_tier2: str = Field(default="llama-3.3-70b-versatile", alias="AI_MODEL_TIER2")

    orchestrator_model: str = Field(
        default="llama-3.3-70b-versatile",
        alias="ORCHESTRATOR_MODEL",
        description="Model for orchestration decisions (can be cheaper/faster than ai_model)",
    )

    # --- SerpAPI (URL Generation) ---
    serpapi_key: str = Field(
        default="",
        alias="SERPAPI_KEY",
    )

    # --- Per-User Limits (Section 6) ---
    rate_limit_per_minute: int = Field(default=30)

    # --- Per-Request Limits ---
    max_pipelines_per_request: int = Field(default=10)

    # --- Global Limits ---
    max_global_pipelines: int = Field(default=100)
    max_playwright_instances: int = Field(default=3)
    max_ai_calls_per_minute: int = Field(default=200)

    # --- Timeouts (Section 12) in seconds ---
    timeout_intent_agent: int = Field(default=8)
    timeout_relevance_agent: int = Field(default=10)
    timeout_url_agent: int = Field(default=10)
    timeout_scraper: int = Field(default=60)
    timeout_robots_fetch: int = Field(default=5)
    timeout_unification_agent: int = Field(default=12)
    timeout_scoring_agent: int = Field(default=12)
    timeout_dom_fetch: int = Field(default=45)

    # --- Circuit Breaker (Section 15) ---
    circuit_breaker_degraded_threshold: int = Field(default=5)
    circuit_breaker_disabled_threshold: int = Field(default=10)
    circuit_breaker_window_minutes: int = Field(default=10)
    circuit_breaker_cooldown_minutes: int = Field(default=30)

    # --- Cache TTLs (seconds) ---
    cache_ttl_intent: int = Field(default=3600)  # 1 hour
    cache_ttl_site_relevance: int = Field(default=1800)  # 30 min
    cache_ttl_robots: int = Field(default=86400)  # 24 hours
    cache_ttl_url_patterns: int = Field(default=3600)
    cache_ttl_extraction: int = Field(default=1800)
    cache_ttl_unified: int = Field(default=1800)

    # --- CORS ---
    cors_origins: List[str] = Field(default=["http://localhost:5173"])

    # --- Semaphore Timeouts ---
    semaphore_acquire_timeout: float = Field(default=10.0)

    # --- Server ---
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
