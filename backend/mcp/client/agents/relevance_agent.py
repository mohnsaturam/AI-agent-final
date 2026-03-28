"""
MCP Client — Relevance Agent (Tier-1)

Scores candidate sites for relevance to the parsed intent.
Batched single Tier-1 call per Section 11.1.
Timeout: 10s (Section 12).
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.relevance")


class RelevanceAgent:
    """Tier-1 AI agent for site relevance scoring."""

    TOOL_NAME = "relevance_agent"
    CALLER = "coordinator"

    @staticmethod
    async def build_input(
        intent: dict,
        candidate_sites: List[str],
        domain: str,
        intent_schema: dict,
        schema_version: str,
        request_id: UUID,
    ) -> dict:
        """Build the Tier-1 input envelope for relevance scoring."""
        return {
            "intent": intent,
            "candidate_sites": candidate_sites,
            "domain": domain,
            "intent_schema": intent_schema,
            "schema_version": schema_version,
            "request_id": str(request_id),
        }

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        Tool implementation — registered in gateway.
        Calls AI to score candidate sites for relevance.
        """
        # Phase 1 key for relevance scoring
        current_key = settings.ai_api_key_phase1
        if not current_key:
            raise RuntimeError("AI_API_KEY_PHASE1 not configured")

        input_data = context.input_data
        intent = input_data["intent"]
        candidate_sites = input_data["candidate_sites"]
        domain = input_data["domain"]
        schema_version = input_data["schema_version"]

        system_prompt = (
            "You are a site relevance scoring agent.\n"
            "Given a parsed user intent and a list of candidate websites, "
            "score each site's relevance to the intent on a 0.0–1.0 scale.\n\n"
            f"Domain: {domain}\n"
            f"Intent: {json.dumps(intent, indent=2)}\n\n"
            "Respond with JSON:\n"
            '{"task": "site_relevance", "domain": "' + domain + '", '
            '"schema_version": "' + schema_version + '", '
            '"result": {"sites": [{"site_url": "<url>", "relevance_score": <0.0-1.0>, '
            '"reasoning": "<why>"}]}, '
            '"confidence": <0.0-1.0>, "reasoning_trace": "<overall explanation>"}'
        )

        user_message = (
            f"Score these sites for the given intent:\n"
            f"Sites: {json.dumps(candidate_sites)}"
        )

        max_retries = 3
        ai_response = None

        # Use a generous timeout — scoring 6+ sites produces a long response
        ai_timeout = max(settings.timeout_relevance_agent, 30)

        for retry_attempt in range(max_retries + 1):

            try:
                async with httpx.AsyncClient(timeout=ai_timeout) as client:
                    response = await client.post(
                        f"{settings.ai_base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {current_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": settings.ai_model,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_message},
                            ],
                            "temperature": 0.1,
                            "response_format": {"type": "json_object"},
                        },
                    )
                    response.raise_for_status()
                    ai_response = response.json()
                    break  # Success
            except httpx.TimeoutException:
                logger.warning(
                    "Relevance agent timed out (attempt %d/%d, timeout=%ds, sites=%d)",
                    retry_attempt + 1, max_retries + 1, ai_timeout, len(candidate_sites),
                )
                if retry_attempt < max_retries:
                    await asyncio.sleep(1)
                    continue
                raise TimeoutError(
                    f"Relevance agent AI call timed out after {max_retries + 1} attempts "
                    f"(timeout={ai_timeout}s, sites={len(candidate_sites)})"
                )
            except httpx.HTTPStatusError as e:
                logger.warning(
                    "Relevance agent HTTP error %d (attempt %d/%d)",
                    e.response.status_code, retry_attempt + 1, max_retries + 1,
                )
                if e.response.status_code == 429 and retry_attempt < max_retries:
                    wait_secs = 2 ** (retry_attempt + 1)
                    logger.warning(
                        "Relevance agent hit 429, retrying in %ds",
                        wait_secs,
                    )
                    await asyncio.sleep(wait_secs)
                    continue
                raise

        if ai_response is None:
            raise RuntimeError("Relevance agent AI call failed after multiple retries.")

        # Parse and validate
        try:
            content = ai_response["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to parse relevance AI response: {e}")

        # Validate output envelope
        _validate_relevance_output(parsed, domain, schema_version)
        return parsed


def _validate_relevance_output(
    output: dict, expected_domain: str, expected_schema_version: str
) -> None:
    """Validate Tier-1 relevance output envelope."""
    required = ["task", "domain", "schema_version", "result", "confidence"]
    for key in required:
        if key not in output:
            raise RuntimeError(f"Relevance output missing key: '{key}'")

    result = output.get("result", {})
    if "sites" not in result:
        raise RuntimeError("Relevance output missing 'result.sites'")

    for site in result["sites"]:
        if "site_url" not in site or "relevance_score" not in site:
            raise RuntimeError("Each site must have 'site_url' and 'relevance_score'")
