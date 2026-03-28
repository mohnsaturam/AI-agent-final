"""
Core — Idempotency Manager (Section 10)

Generates SHA256 idempotency keys and implements lookup rules.
Schema version is included to bust keys on domain schema upgrades.
"""

import hashlib
import json
import logging
from typing import Any, Dict, Optional
from uuid import UUID

logger = logging.getLogger("umsa.idempotency")


class IdempotencyManager:
    """Manages idempotency key generation and request deduplication."""

    @staticmethod
    def generate_key(
        user_id: UUID,
        normalized_query: str,
        domain: str,
        schema_version: str,
        sites: list = None,
    ) -> str:
        """
        Generate idempotency key:
        SHA256(user_id + normalized_query + domain + schema_version + sorted_sites)
        """
        sites_str = ",".join(sorted(sites)) if sites else ""
        raw = f"{user_id}|{normalized_query}|{domain}|{schema_version}|{sites_str}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    async def check(
        db_pool,
        idempotency_key: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up existing request by idempotency key.

        Returns:
            None — no record found, proceed normally
            {"action": "attach", "request_id": UUID} — actively running, attach
            {"action": "return", "result": dict} — completed, return result
            {"action": "retry"} — failed, allow full retry
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, state, result, error
                FROM umsa_core.requests
                WHERE idempotency_key = $1
                ORDER BY created_at DESC
                LIMIT 1
                """,
                idempotency_key,
            )

        if row is None:
            return None  # No record → proceed normally

        state = row["state"]
        request_id = row["id"]

        if state == "COMPLETED":
            # Return persisted result immediately
            return {
                "action": "return",
                "request_id": str(request_id),
                "result": row["result"],
            }

        if state == "FAILED":
            # Allow full retry
            return {"action": "retry"}

        # Request is actively running (any non-terminal state)
        return {
            "action": "attach",
            "request_id": str(request_id),
            "state": state,
        }


# Global singleton
idempotency_manager = IdempotencyManager()
