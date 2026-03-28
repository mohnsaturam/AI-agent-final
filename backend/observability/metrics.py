"""
Prometheus metrics — all counters and histograms for UMSA observability.
Metrics are incremented exclusively inside execute_tool (gateway.py).
Namespace: umsa_
Labels: domain, tool_name, caller, status
"""

from prometheus_client import Counter, Histogram, Gauge, Info


# --- Request-level metrics ---
REQUEST_LATENCY = Histogram(
    "umsa_request_latency_ms",
    "Total request wall time in milliseconds",
    labelnames=["domain", "status"],
    buckets=[100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000],
)

STAGE_LATENCY = Histogram(
    "umsa_stage_latency_ms",
    "Per-stage breakdown latency in milliseconds",
    labelnames=["domain", "tool_name", "status"],
    buckets=[50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000],
)

# --- Tool execution metrics ---
TOOL_INVOCATIONS = Counter(
    "umsa_tool_invocations_total",
    "Total tool invocation count",
    labelnames=["domain", "tool_name", "caller", "status"],
)

TOOL_DURATION = Histogram(
    "umsa_tool_duration_seconds",
    "Tool execution duration in seconds",
    labelnames=["domain", "tool_name", "caller"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0],
)

# --- AI call metrics ---
AI_CALL_COUNT = Counter(
    "umsa_ai_call_count_total",
    "Total AI calls (Tier-1 and Tier-2)",
    labelnames=["domain", "tier", "task"],
)

# --- Cache metrics ---
CACHE_HITS = Counter(
    "umsa_cache_hits_total",
    "Cache hit count per stage",
    labelnames=["domain", "stage"],
)

CACHE_MISSES = Counter(
    "umsa_cache_misses_total",
    "Cache miss count per stage",
    labelnames=["domain", "stage"],
)

# --- Domain health metrics ---
DOMAIN_FAILURES = Counter(
    "umsa_domain_failure_total",
    "Domain failure count by classification",
    labelnames=["domain", "site_domain", "failure_class"],
)

# --- Extraction metrics ---
EXTRACTION_CONFIDENCE = Histogram(
    "umsa_extraction_confidence",
    "Extraction confidence score distribution",
    labelnames=["domain", "site_domain"],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# --- Retry metrics ---
RETRY_COUNT = Counter(
    "umsa_retry_count_total",
    "Retry count per tool per request",
    labelnames=["domain", "tool_name"],
)

# --- Semaphore metrics ---
SEMAPHORE_WAIT = Histogram(
    "umsa_semaphore_wait_ms",
    "Time spent waiting for semaphore acquisition in milliseconds",
    labelnames=["semaphore_type"],
    buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

# --- Active gauges ---
ACTIVE_PIPELINES = Gauge(
    "umsa_active_pipelines",
    "Currently active pipeline count",
)

ACTIVE_PLAYWRIGHT = Gauge(
    "umsa_active_playwright_instances",
    "Currently active Playwright instance count",
)

# --- System info ---
SYSTEM_INFO = Info(
    "umsa_system",
    "UMSA system information",
)
