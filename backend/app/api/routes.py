"""
API Routes — Versioned REST Endpoints (Section 22)

POST /v1/scrape       — Submit scrape request
GET  /v1/requests/:id — Poll request status
GET  /v1/domains      — List active domains
GET  /v1/health       — System health summary

All endpoints require JWT. Response format: {status, data, error}
"""

import json
import logging
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator

from app.core.auth import AuthenticatedUser, validate_jwt
from app.core.rate_limiter import rate_limiter
from app.core.idempotency import idempotency_manager
from app.core.concurrency import concurrency_manager

logger = logging.getLogger("umsa.routes")

router = APIRouter(prefix="/v1")


# ============================================================
# Request/Response Models
# ============================================================

class ScrapeRequest(BaseModel):
    """Scrape request body. user_id is NEVER accepted here."""
    query: str = Field(..., min_length=1, max_length=1000)
    sites: List[str] = Field(..., min_length=1, description="User-provided target site domains")
    domain: str = Field(default="movie", max_length=64)
    unify: bool = Field(default=False, description="If true, run AI unification to merge cross-site data")

    @field_validator('sites')
    @classmethod
    def validate_sites(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one site domain is required')
        cleaned = []
        for s in v:
            s = s.strip().lower().replace('https://', '').replace('http://', '').rstrip('/')
            if not s:
                raise ValueError('Empty site domain is not allowed')
            cleaned.append(s)
        return cleaned


class APIResponse(BaseModel):
    """Standard API response envelope."""
    status: str
    data: Optional[dict] = None
    error: Optional[dict] = None


# ============================================================
# POST /v1/scrape
# ============================================================

@router.post("/scrape", response_model=APIResponse)
async def submit_scrape(
    body: ScrapeRequest,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """
    Step 1 — Request Intake (Section 19).
    Validates JWT, normalizes input, checks rate limit,
    reserves semaphores, generates request_id, persists INIT state.
    """
    from app.main import get_app_state

    app_state = get_app_state()
    db_pool = app_state["db_pool"]
    domain_registry = app_state["domain_registry"]
    coordinator = app_state["coordinator"]

    # Rate limit check
    await rate_limiter.check(user.user_id)

    # Validate domain exists and is active
    domain_module = domain_registry.get(body.domain)
    if not domain_module:
        raise HTTPException(
            status_code=400,
            detail=f"Domain '{body.domain}' not found or inactive",
        )

    schema_version = domain_module["schema_version"]

    # Normalize query
    normalized_query = body.query.strip().lower()

    # Generate idempotency key
    idempotency_key = idempotency_manager.generate_key(
        user_id=user.user_id,
        normalized_query=normalized_query,
        domain=body.domain,
        schema_version=schema_version,
        sites=body.sites,
    )

    # Idempotency check
    existing = await idempotency_manager.check(db_pool, idempotency_key)
    if existing:
        action = existing["action"]
        if action == "return":
            return APIResponse(
                status="completed",
                data={
                    "request_id": existing["request_id"],
                    "result": existing["result"],
                    "cache_hit": True,
                },
            )
        elif action == "attach":
            return APIResponse(
                status="processing",
                data={
                    "request_id": existing["request_id"],
                    "state": existing.get("state", "INIT"),
                    "message": "Request already in progress",
                },
            )
        # action == "retry" → fall through to create new request

    # Ensure user exists in DB
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO umsa_core.users (id, external_id, role)
            VALUES ($1, $2, $3)
            ON CONFLICT (external_id) DO UPDATE SET updated_at = now()
            """,
            user.user_id,
            str(user.user_id),
            user.user_role,
        )

    # Generate request_id and persist INIT state
    request_id = uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO umsa_core.requests
                (id, user_id, domain, raw_query, normalized_query,
                 idempotency_key, state, schema_version, sites)
            VALUES ($1, $2, $3, $4, $5, $6, 'INIT', $7, $8)
            """,
            request_id,
            user.user_id,
            body.domain,
            body.query,
            normalized_query,
            idempotency_key,
            schema_version,
            body.sites,
        )

    # Fire and forget — process asynchronously
    import asyncio
    asyncio.create_task(
        coordinator.process_request(
            request_id=request_id,
            user_id=user.user_id,
            query=body.query,
            domain_name=body.domain,
            sites=body.sites,
            unify=body.unify,
        )
    )

    return APIResponse(
        status="accepted",
        data={
            "request_id": str(request_id),
            "domain": body.domain,
            "state": "INIT",
            "message": "Request submitted for processing",
        },
    )


# ============================================================
# GET /v1/requests/{request_id}
# ============================================================

@router.get("/requests/{request_id}", response_model=APIResponse)
async def get_request_status(
    request_id: UUID,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """Poll the status and result of a scrape request."""
    from app.main import get_app_state

    db_pool = get_app_state()["db_pool"]

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, state, domain, raw_query, result, error,
                   created_at, updated_at
            FROM umsa_core.requests
            WHERE id = $1 AND user_id = $2
            """,
            request_id,
            user.user_id,
        )

    if row is None:
        raise HTTPException(status_code=404, detail="Request not found")

    state = row["state"]
    data = {
        "request_id": str(row["id"]),
        "state": state,
        "domain": row["domain"],
        "query": row["raw_query"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
    }

    # Fetch pipelines for this request
    async with db_pool.acquire() as conn:
        pipeline_rows = await conn.fetch(
            """
            SELECT id, site_url, state, failure_class,
                   started_at, completed_at
            FROM umsa_core.pipelines
            WHERE request_id = $1
            ORDER BY started_at ASC NULLS LAST
            """,
            request_id,
        )
    data["pipelines"] = [
        {
            "id": str(pr["id"]),
            "site_url": pr["site_url"],
            "state": pr["state"],
            "failure_class": pr["failure_class"],
            "started_at": pr["started_at"].isoformat() if pr["started_at"] else None,
            "completed_at": pr["completed_at"].isoformat() if pr["completed_at"] else None,
        }
        for pr in pipeline_rows
    ]

    # Fetch schema_version from domain module
    domain_registry = get_app_state()["domain_registry"]
    dm = domain_registry.get(row["domain"])
    data["schema_version"] = dm["schema_version"] if dm else None

    if state == "COMPLETED" and row["result"]:
        data["result"] = row["result"]

    if state == "FAILED" and row["error"]:
        error_data = row["error"]
        if isinstance(error_data, str):
            try:
                error_data = json.loads(error_data)
            except json.JSONDecodeError:
                error_data = {"message": error_data}
        # Include error in data so frontend can access request.error
        data["error"] = error_data
        return APIResponse(
            status="failed",
            data=data,
            error=error_data,
        )

    status_map = {
        "INIT": "processing",
        "INTENT_DONE": "processing",
        "VALIDATED": "processing",
        "CACHE_HIT": "completed",
        "RELEVANCE_DONE": "processing",
        "PIPELINES_RUNNING": "processing",
        "EXTRACTION_DONE": "processing",
        "UNIFIED": "processing",
        "COMPLETED": "completed",
        "FAILED": "failed",
    }

    return APIResponse(
        status=status_map.get(state, "processing"),
        data=data,
    )


# ============================================================
# POST /v1/requests/{request_id}/resume
# ============================================================

@router.post("/requests/{request_id}/resume", response_model=APIResponse)
async def resume_request(
    request_id: UUID,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """
    Resume a stuck/failed request from its last checkpoint.
    Only works for requests in FAILED state or stuck processing states.
    """
    from app.main import get_app_state
    import asyncio

    app_state = get_app_state()
    db_pool = app_state["db_pool"]
    coordinator = app_state["coordinator"]

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, state, domain, raw_query, user_id,
                   checkpoint_data, sites
            FROM umsa_core.requests
            WHERE id = $1 AND user_id = $2
            """,
            request_id,
            user.user_id,
        )

    if row is None:
        raise HTTPException(status_code=404, detail="Request not found")

    state = row["state"]
    resumable_states = ["FAILED", "INTENT_DONE", "VALIDATED",
                        "RELEVANCE_DONE", "PIPELINES_RUNNING",
                        "EXTRACTION_DONE", "UNIFIED"]

    if state not in resumable_states:
        raise HTTPException(
            status_code=400,
            detail=f"Request in state '{state}' cannot be resumed. "
                   f"Resumable states: {resumable_states}",
        )

    # Fire and forget — resume asynchronously
    asyncio.create_task(
        coordinator.process_request(
            request_id=request_id,
            user_id=row["user_id"],
            query=row["raw_query"],
            domain_name=row["domain"],
            sites=row.get("sites", []),
        )
    )

    return APIResponse(
        status="accepted",
        data={
            "request_id": str(request_id),
            "resumed_from_state": state,
            "message": "Request resumption initiated",
        },
    )


# ============================================================
# GET /v1/domains
# ============================================================

@router.get("/domains", response_model=APIResponse)
async def list_domains(
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """List all active domains."""
    from app.main import get_app_state

    domain_registry = get_app_state()["domain_registry"]
    domains = domain_registry.list_all()

    return APIResponse(
        status="ok",
        data={"domains": domains},
    )


# ============================================================
# GET /v1/domains/{domain_name}/sites
# ============================================================

@router.get("/domains/{domain_name}/sites", response_model=APIResponse)
async def list_domain_sites(
    domain_name: str,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """List allowed sites for a specific domain."""
    from app.main import get_app_state

    domain_registry = get_app_state()["domain_registry"]
    domain_module = domain_registry.get(domain_name)

    if not domain_module:
        raise HTTPException(status_code=404, detail=f"Domain '{domain_name}' not found")

    db_config = domain_module.get("db_config", {})
    allowed_sites = db_config.get("allowed_sites", []) or []

    return APIResponse(
        status="ok",
        data={
            "domain": domain_name,
            "allowed_sites": allowed_sites,
        },
    )


# ============================================================
# GET /v1/health
# ============================================================

@router.get("/health", response_model=APIResponse)
async def health_check(
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """System health summary."""
    from app.main import get_app_state

    app_state = get_app_state()
    db_pool = app_state["db_pool"]

    # Check DB connectivity
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_healthy = True
    except Exception:
        db_healthy = False

    # Get domain health
    domain_health = []
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT domain, site_domain, status, failure_count,
                       last_failure, cooldown_until
                FROM umsa_core.domain_health
                ORDER BY domain, site_domain
                """
            )
        domain_health = [
            {
                "domain": r["domain"],
                "site": r["site_domain"],
                "status": r["status"],
                "failures": r["failure_count"],
            }
            for r in rows
        ]
    except Exception:
        pass

    # Concurrency stats
    semaphore_stats = concurrency_manager.get_stats()

    return APIResponse(
        status="ok",
        data={
            "database": "healthy" if db_healthy else "unhealthy",
            "active_domains": app_state["domain_registry"].list_active(),
            "semaphores": semaphore_stats,
            "domain_health": domain_health,
        },
    )


# ============================================================
# GET /v1/requests/{request_id}/fields — Semantic Fields
# ============================================================

@router.get("/requests/{request_id}/fields", response_model=APIResponse)
async def get_semantic_fields(
    request_id: UUID,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """Return per-site semantic fields for field selection UI."""
    from app.main import get_app_state

    db_pool = get_app_state()["db_pool"]

    # Verify request belongs to user
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            "SELECT id FROM umsa_core.requests WHERE id = $1 AND user_id = $2",
            request_id, user.user_id,
        )
    if req is None:
        raise HTTPException(status_code=404, detail="Request not found")

    # Fetch semantic fields
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT site_domain, raw_field_key, display_name,
                   relevance, category, raw_value, engine_source
            FROM umsa_core.semantic_fields
            WHERE request_id = $1
            ORDER BY site_domain, relevance DESC
            """,
            request_id,
        )

    # Group by site
    sites = {}
    for row in rows:
        site = row["site_domain"]
        if site not in sites:
            sites[site] = {"fields": []}
        sites[site]["fields"].append({
            "raw_key": row["raw_field_key"],
            "display_name": row["display_name"],
            "relevance": row["relevance"],
            "category": row["category"],
            "preview": row["raw_value"][:200] if row["raw_value"] else None,
            "engine": row["engine_source"],
        })

    return APIResponse(
        status="ok",
        data={"sites": sites},
    )


# ============================================================
# POST /v1/requests/{request_id}/select-fields
# ============================================================

class FieldSelectionRequest(BaseModel):
    """User field selection per site."""
    selections: dict = Field(
        ...,
        description="Dict of site_domain → list of raw_field_keys"
    )


@router.post("/requests/{request_id}/select-fields", response_model=APIResponse)
async def select_fields(
    request_id: UUID,
    body: FieldSelectionRequest,
    user: AuthenticatedUser = Depends(validate_jwt),
):
    """Return normalized values for user-selected fields per site."""
    from app.main import get_app_state

    db_pool = get_app_state()["db_pool"]

    # Verify request belongs to user
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            "SELECT id FROM umsa_core.requests WHERE id = $1 AND user_id = $2",
            request_id, user.user_id,
        )
    if req is None:
        raise HTTPException(status_code=404, detail="Request not found")

    result_sites = {}

    for site_domain, selected_keys in body.selections.items():
        if not isinstance(selected_keys, list):
            continue

        # Fetch the semantic field metadata + raw values for selected fields
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT raw_field_key, display_name, relevance,
                       category, raw_value, engine_source
                FROM umsa_core.semantic_fields
                WHERE request_id = $1
                  AND site_domain = $2
                  AND raw_field_key = ANY($3)
                ORDER BY relevance DESC
                """,
                request_id,
                site_domain,
                selected_keys,
            )

        fields_data = []
        for row in rows:
            raw_value = row["raw_value"]

            # Try to parse JSON values for better display
            normalized_value = raw_value
            if raw_value:
                try:
                    parsed = json.loads(raw_value)
                    # Flatten nested objects for display
                    if isinstance(parsed, dict):
                        # Extract the most useful value
                        if "ratingValue" in parsed:
                            normalized_value = f"{parsed['ratingValue']}"
                            if "ratingCount" in parsed:
                                normalized_value += f" ({parsed['ratingCount']} votes)"
                        elif "name" in parsed:
                            normalized_value = parsed["name"]
                        else:
                            normalized_value = raw_value
                    elif isinstance(parsed, list):
                        # Join list items
                        items = []
                        for item in parsed[:10]:
                            if isinstance(item, dict) and "name" in item:
                                items.append(item["name"])
                            elif isinstance(item, str):
                                items.append(item)
                            else:
                                items.append(str(item))
                        normalized_value = ", ".join(items)
                except (json.JSONDecodeError, TypeError):
                    normalized_value = raw_value

            fields_data.append({
                "raw_key": row["raw_field_key"],
                "display_name": row["display_name"],
                "value": normalized_value,
                "category": row["category"],
                "engine": row["engine_source"],
            })

        result_sites[site_domain] = {
            "fields": fields_data,
            "field_count": len(fields_data),
        }

    return APIResponse(
        status="ok",
        data={"sites": result_sites},
    )

