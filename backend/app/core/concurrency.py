"""
Core — Concurrency Manager (Section 6)

Manages global and per-request semaphores.
Global: 100 pipelines, 20 Playwright, 200 AI calls/min.
Per-request: 10 pipelines.
All acquisitions have timeouts — never block indefinitely.
"""

import asyncio
import logging
import time
from typing import Dict, Optional
from uuid import UUID

from app.core.config import settings

logger = logging.getLogger("umsa.concurrency")


class ConcurrencyManager:
    """
    Manages semaphores for resource governance.
    Returns HTTP 503 on exhaustion.
    """

    def __init__(self):
        # Global semaphores
        self._global_pipeline = asyncio.Semaphore(settings.max_global_pipelines)
        self._global_playwright = asyncio.Semaphore(settings.max_playwright_instances)

        # Per-request semaphores (created on demand)
        self._per_request: Dict[UUID, asyncio.Semaphore] = {}

        # Timeout for acquisition
        self._timeout = settings.semaphore_acquire_timeout

    def _get_per_request_semaphore(self, request_id: UUID) -> asyncio.Semaphore:
        """Get or create a per-request semaphore."""
        if request_id not in self._per_request:
            self._per_request[request_id] = asyncio.Semaphore(
                settings.max_pipelines_per_request
            )
        return self._per_request[request_id]

    async def acquire_global(self, tool_name: str) -> Optional[str]:
        """
        Acquire a global semaphore token.
        Returns a token string on success, None on timeout.
        """
        semaphore = self._global_pipeline
        if tool_name == "scraper":
            semaphore = self._global_playwright

        try:
            acquired = await asyncio.wait_for(
                semaphore.acquire(), timeout=self._timeout
            )
            if acquired:
                return f"global_{tool_name}_{id(semaphore)}"
            return None
        except asyncio.TimeoutError:
            logger.warning(
                "Global semaphore timeout for tool '%s' after %.1fs",
                tool_name,
                self._timeout,
            )
            return None

    async def release_global(self, tool_name: str, token: str) -> None:
        """Release a global semaphore token."""
        semaphore = self._global_pipeline
        if tool_name == "scraper":
            semaphore = self._global_playwright

        try:
            semaphore.release()
        except ValueError:
            logger.warning("Attempted to release already-released global semaphore")

    async def acquire_per_request(self, request_id: UUID) -> Optional[str]:
        """
        Acquire a per-request semaphore token.
        Returns token string on success, None on timeout.
        """
        semaphore = self._get_per_request_semaphore(request_id)
        try:
            acquired = await asyncio.wait_for(
                semaphore.acquire(), timeout=self._timeout
            )
            if acquired:
                return f"request_{request_id}"
            return None
        except asyncio.TimeoutError:
            logger.warning(
                "Per-request semaphore timeout for request %s", request_id
            )
            return None

    async def release_per_request(
        self, request_id: UUID, token: str
    ) -> None:
        """Release a per-request semaphore token."""
        semaphore = self._per_request.get(request_id)
        if semaphore:
            try:
                semaphore.release()
            except ValueError:
                logger.warning(
                    "Attempted to release already-released per-request semaphore"
                )

    def cleanup_request(self, request_id: UUID) -> None:
        """Clean up per-request semaphore after request completion."""
        self._per_request.pop(request_id, None)

    def get_stats(self) -> dict:
        """Get current semaphore utilization stats."""
        return {
            "global_pipeline": {
                "max": settings.max_global_pipelines,
                "available": self._global_pipeline._value,
            },
            "global_playwright": {
                "max": settings.max_playwright_instances,
                "available": self._global_playwright._value,
            },
            "active_requests": len(self._per_request),
        }


# Global singleton
concurrency_manager = ConcurrencyManager()
