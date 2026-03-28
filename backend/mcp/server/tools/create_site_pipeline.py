"""
MCP Tool — create_site_pipeline (Step 6)

ONLY for validated+relevant sites.
Creates pipeline DB records.
Returns pipeline_ids.
"""

import logging
from typing import Any, Dict
from uuid import uuid4

logger = logging.getLogger("umsa.tools.create_site_pipeline")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Create pipeline records for validated sites."""
    input_data = context.input_data
    request_id = input_data.get("request_id", "")
    domain = input_data.get("domain", "")
    sites = input_data.get("sites", [])

    pipeline_ids = []

    async with db_pool.acquire() as conn:
        for site_url in sites:
            pipeline_id = uuid4()
            await conn.execute(
                """
                INSERT INTO umsa_core.pipelines
                    (id, request_id, domain, site_url, state)
                VALUES ($1, $2, $3, $4, 'PENDING')
                """,
                pipeline_id,
                request_id if isinstance(request_id, str) and len(request_id) == 36 else None,
                domain,
                site_url,
            )
            pipeline_ids.append({
                "pipeline_id": str(pipeline_id),
                "site_url": site_url,
                "state": "PENDING",
            })

    logger.info("Created %d pipeline(s) for request %s", len(pipeline_ids), request_id)

    return {
        "pipelines": pipeline_ids,
        "count": len(pipeline_ids),
    }
