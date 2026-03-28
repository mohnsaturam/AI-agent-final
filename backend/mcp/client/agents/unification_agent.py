"""
MCP Client — Unification Agent (Tier-2)

Resolves cross-site data conflicts after deterministic normalization.
Only invoked when conflicts remain (Section 11.2).
Timeout: 12s (Section 12).
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.unification")


class UnificationAgent:
    """Tier-2 AI agent for cross-site unification."""

    TOOL_NAME = "unification_agent"
    CALLER = "coordinator"

    @staticmethod
    async def build_input(
        candidates: List[dict],
        conflict_fields: List[str],
        extraction_schema: dict,
        domain: str,
        schema_version: str,
        request_id: UUID,
    ) -> dict:
        """Build the Tier-2 input envelope (Section 11.2)."""
        return {
            "candidates": candidates,
            "conflict_fields": conflict_fields,
            "extraction_schema": extraction_schema,
            "domain": domain,
            "schema_version": schema_version,
            "request_id": str(request_id),
        }

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        Tool implementation — registered in gateway.
        Calls Tier-2 AI to resolve data conflicts.
        """
        # Phase 1 key for unification
        current_key = settings.ai_api_key_phase1
        if not current_key:
            raise RuntimeError("AI_API_KEY_PHASE1 not configured")

        input_data = context.input_data
        candidates = input_data["candidates"]
        conflict_fields = input_data["conflict_fields"]
        extraction_schema = input_data["extraction_schema"]
        domain = input_data["domain"]
        schema_version = input_data["schema_version"]

        system_prompt = (
            "You are a cross-site data unification agent.\n"
            "Given extraction results from multiple sites with conflicting fields, "
            "resolve conflicts by choosing the most accurate value.\n\n"
            f"Domain: {domain}\n"
            f"Schema version: {schema_version}\n"
            f"Conflict fields: {json.dumps(conflict_fields)}\n"
            f"Extraction schema:\n{json.dumps(extraction_schema, indent=2)}\n\n"
            "Rules:\n"
            "- Prefer higher-weight sources\n"
            "- Prefer more specific/detailed values\n"
            "- All fields in the extraction schema must be present in unified_record\n"
            "- Explain each conflict resolution\n\n"
            "Respond with JSON (Tier-2 output envelope):\n"
            '{"task": "cross_site_unification", "domain": "' + domain + '", '
            '"schema_version": "' + schema_version + '", '
            '"unified_record": {<all schema fields>}, '
            '"resolved_conflicts": {"<field>": {"chosen_value": "<v>", '
            '"chosen_source": "<site>", "resolution_reason": "<why>"}}, '
            '"confidence": <0.0-1.0>}'
        )

        user_message = (
            f"Resolve conflicts in these extraction results:\n"
            f"{json.dumps(candidates, indent=2)}"
        )

        max_retries = 3
        ai_response = None

        for retry_attempt in range(max_retries + 1):
            
            try:
                async with httpx.AsyncClient(timeout=settings.timeout_unification_agent) as client:
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
                    break # Exit loop on successful response
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and retry_attempt < max_retries:
                    wait_secs = 2 ** (retry_attempt + 1)
                    logger.warning(
                        "Unification agent hit 429 rate limit, retrying in %ds (attempt %d/%d)",
                        wait_secs, retry_attempt + 1, max_retries,
                    )
                    await asyncio.sleep(wait_secs)
                    continue # Retry with same key
                raise # Re-raise other HTTP errors or 429 after max retries
            except httpx.TimeoutException:
                raise TimeoutError("Unification agent AI call timed out")
            except Exception as e:
                # Catch any other unexpected errors during the request
                raise RuntimeError(f"Unification agent failed during AI call: {e}")

        if ai_response is None:
            raise RuntimeError("Unification agent failed to get a response after retries.")

        try:
            content = ai_response["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to parse unification response: {e}")

        # Validate Tier-2 output envelope (Section 11.2)
        _validate_tier2_output(parsed, domain, schema_version, extraction_schema)
        return parsed


def _validate_tier2_output(
    output: dict,
    expected_domain: str,
    expected_schema_version: str,
    extraction_schema: dict,
) -> None:
    """Validate Tier-2 output envelope per Section 11.2."""
    required = [
        "task",
        "domain",
        "schema_version",
        "unified_record",
        "resolved_conflicts",
        "confidence",
    ]
    for key in required:
        if key not in output:
            raise RuntimeError(f"Tier-2 output missing key: '{key}'")

    if output["task"] != "cross_site_unification":
        raise RuntimeError(f"Tier-2 task mismatch: '{output['task']}'")

    if output["domain"] != expected_domain:
        raise RuntimeError(f"Tier-2 domain mismatch: '{output['domain']}'")

    if output["schema_version"] != expected_schema_version:
        raise RuntimeError(f"Tier-2 schema_version mismatch")

    # Validate unified_record against extraction_schema
    unified = output.get("unified_record", {})
    schema_required = extraction_schema.get("required", [])
    for field in schema_required:
        if field not in unified:
            raise RuntimeError(
                f"EXTRACTION_SCHEMA_FAIL: unified_record missing required field '{field}'"
            )
