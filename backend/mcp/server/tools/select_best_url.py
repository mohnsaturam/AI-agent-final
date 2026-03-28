"""
MCP Tool — select_best_url (Step 8d)

Pick highest-scoring URL from candidates.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger("umsa.tools.select_best_url")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Select the best URL from scored candidates."""
    input_data = context.input_data
    candidates = input_data.get("candidates", [])
    min_score = input_data.get("min_score", 50)

    if not candidates:
        return {
            "selected": False,
            "url": None,
            "reason": "No candidates provided",
        }

    # Sort by score descending
    sorted_candidates = sorted(
        candidates,
        key=lambda c: c.get("score", c.get("confidence", 0)),
        reverse=True,
    )

    best = sorted_candidates[0]
    best_score = best.get("score", best.get("confidence", 0))

    if best_score >= min_score:
        return {
            "selected": True,
            "url": best.get("url", ""),
            "score": best_score,
            "pattern_type": best.get("pattern_type", "search"),
            "reasoning": best.get("reasoning", ""),
            "all_candidates": [
                {"url": c.get("url"), "score": c.get("score", c.get("confidence", 0))}
                for c in sorted_candidates[:5]
            ],
        }

    return {
        "selected": False,
        "url": best.get("url", ""),
        "score": best_score,
        "reason": f"Best score {best_score} below minimum {min_score}",
        "all_candidates": [
            {"url": c.get("url"), "score": c.get("score", c.get("confidence", 0))}
            for c in sorted_candidates[:5]
        ],
    }
