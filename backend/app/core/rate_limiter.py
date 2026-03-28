"""
Core — Rate Limiter (Section 6)

Per-user sliding window rate limiter: 30 requests per minute.
Returns HTTP 429 when exceeded.
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, List
from uuid import UUID

from fastapi import HTTPException

from app.core.config import settings

logger = logging.getLogger("umsa.rate_limiter")


class RateLimiter:
    """
    In-memory sliding window rate limiter.
    Tracks per-user request timestamps.
    """

    def __init__(self, max_requests: int = None, window_seconds: int = 60):
        self._max_requests = max_requests or settings.rate_limit_per_minute
        self._window_seconds = window_seconds
        self._requests: Dict[UUID, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def check(self, user_id: UUID) -> None:
        """
        Check if the user has exceeded their rate limit.
        Raises HTTP 429 if exceeded.
        """
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds

            # Clean expired entries
            timestamps = self._requests[user_id]
            self._requests[user_id] = [
                ts for ts in timestamps if ts > cutoff
            ]

            if len(self._requests[user_id]) >= self._max_requests:
                logger.warning(
                    "Rate limit exceeded for user %s: %d/%d in %ds",
                    user_id,
                    len(self._requests[user_id]),
                    self._max_requests,
                    self._window_seconds,
                )
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Rate limit exceeded",
                        "limit": self._max_requests,
                        "window_seconds": self._window_seconds,
                        "retry_after": int(
                            self._requests[user_id][0] + self._window_seconds - now
                        ),
                    },
                )

            # Record this request
            self._requests[user_id].append(now)

    async def get_usage(self, user_id: UUID) -> dict:
        """Get current rate limit usage for a user."""
        now = time.monotonic()
        cutoff = now - self._window_seconds
        timestamps = self._requests.get(user_id, [])
        active = [ts for ts in timestamps if ts > cutoff]
        return {
            "used": len(active),
            "limit": self._max_requests,
            "remaining": max(0, self._max_requests - len(active)),
        }


# Global singleton
rate_limiter = RateLimiter()
