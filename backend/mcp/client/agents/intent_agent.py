"""
MCP Client — Intent Agent (Tier-1)

Understands user queries and parses them into structured domain intents.
Formats input/output per Section 11.1 Tier-1 contract.
Timeout: 8s (Section 12).
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
from uuid import UUID

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.intent")


class IntentAgent:
    """Tier-1 AI agent for intent understanding."""

    TOOL_NAME = "intent_agent"
    CALLER = "coordinator"

    @staticmethod
    async def build_input(
        query: str,
        domain: str,
        intent_schema: dict,
        schema_version: str,
        request_id: UUID,
        intent_guidance: str = "",
    ) -> dict:
        """Build the Tier-1 input envelope (Section 11.1)."""
        return {
            "query": query,
            "domain": domain,
            "intent_schema": intent_schema,
            "schema_version": schema_version,
            "request_id": str(request_id),
        }

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        Tool implementation — registered in gateway.
        Calls AI to parse the user's query into a structured domain intent.
        Validates output matches intent_schema.
        """
        # Phase 1 key for intent understanding
        current_key = settings.ai_api_key_phase1
        if not current_key:
            raise RuntimeError("AI_API_KEY_PHASE1 not configured")

        input_data = context.input_data
        query = input_data["query"]
        domain = input_data["domain"]
        intent_schema = input_data["intent_schema"]
        schema_version = input_data["schema_version"]
        request_id = input_data["request_id"]

        # Get domain-specific guidance from MCP context (from DB config)
        guidance_text = input_data.get("intent_guidance", "")

        system_prompt = (
            "You are an intent understanding agent. "
            "Parse the user query into a structured intent matching the provided schema.\n\n"
            f"Domain: {domain}\n"
            f"Schema version: {schema_version}\n"
            f"Intent schema:\n{json.dumps(intent_schema, indent=2)}\n\n"
        )

        if guidance_text:
            system_prompt += f"Domain-specific guidance:\n{guidance_text}\n\n"

        system_prompt += (
            "QUERY TYPE RULES (follow strictly):\n\n"
            "DEFAULT RULE: If the query is a single word or short phrase that could be "
            "a movie/show/game title (even if you don't recognize it), set query_type = 'details'.\n\n"
            "- query_type: 'details' → the query names ONE specific movie/show/entity.\n"
            "  Examples: 'Inception', 'Sirai', 'Dhurandhar', 'The Dark Knight',\n"
            "  'Interstellar 2014', 'KGF Chapter 2', 'pushpa', 'movie xyz'\n"
            "- query_type: 'list' → user explicitly asks for MULTIPLE items.\n"
            "  Examples: 'top 10 movies', 'best movies of 2025', 'hindi movies released 2024'\n"
            "- query_type: 'search' → query has ONLY filters (genre/language/year) with NO title.\n"
            "  Examples: 'action movies 2024', 'korean horror films', 'sci-fi movies'\n\n"
            "IMPORTANT: A bare unfamiliar title is STILL 'details', NOT 'search'.\n"
            "'Dhurandhar' → details  |  'Sirai' → details  |  'Vidaamuyarchi' → details\n\n"
            "YEAR RULES:\n"
            "- Populate 'year' field when a year is mentioned as a filter.\n"
            "- 'Inception 2010' → title='Inception', year=2010, query_type='details'\n"
            "- '2012 movie' → title='2012', year=null (2012 IS the title)\n\n"
            "FILTER RULES:\n"
            "- Language mentioned → filters.language (hindi, tamil, korean, etc.)\n"
            "- Genre mentioned → filters.genre (horror, comedy, action, etc.)\n\n"
            "REQUESTED FIELDS RULES:\n"
            "- Analyze the query for SPECIFIC data the user is asking about.\n"
            "- Map to extraction schema field names: title, year, director, cast, genres,\n"
            "  rating, synopsis, runtime_minutes, language, box_office, budget,\n"
            "  poster_url, trailer_url, certification, release_date, critic_score, audience_score.\n"
            "- Examples:\n"
            "  'worldwide collection of Dhurandhar' → requested_fields: ['box_office']\n"
            "  'cast of Inception' → requested_fields: ['cast']\n"
            "  'Inception rating' → requested_fields: ['rating']\n"
            "  'who directed Interstellar' → requested_fields: ['director']\n"
            "  'budget and collection of KGF' → requested_fields: ['budget', 'box_office']\n"
            "  'top 5 tamil movies 2024' → requested_fields: [] (user wants all data)\n"
            "  'Inception' → requested_fields: [] (no specific field requested)\n"
            "- Only populate if user EXPLICITLY asks for specific data. Empty otherwise.\n\n"
            "Respond with a JSON object:\n"
            '{"task": "intent_understanding", "domain": "' + domain + '", '
            '"schema_version": "' + schema_version + '", "result": {<parsed intent>}, '
            '"confidence": <0.0-1.0>, "reasoning_trace": "<explanation>"}'
        )

        # Call AI provider with 429 retry
        max_retries = 3
        ai_response = None
        try:
            for retry_attempt in range(max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=settings.timeout_intent_agent) as client:
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
                                    {"role": "user", "content": query},
                                ],
                                "temperature": 0.1,
                                "response_format": {"type": "json_object"},
                            },
                        )
                        response.raise_for_status()
                        ai_response = response.json()
                        break  # Success
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429 and retry_attempt < max_retries:
                        wait_secs = 2 ** (retry_attempt + 1)  # 2s, 4s, 8s
                        logger.warning(
                            "Intent agent hit 429 rate limit, retrying in %ds (attempt %d/%d)",
                            wait_secs, retry_attempt + 1, max_retries,
                        )
                        await asyncio.sleep(wait_secs)
                        continue
                    raise RuntimeError(
                        f"Intent agent AI call failed: {e.response.status_code}"
                    )

            if ai_response is None:
                raise RuntimeError("Intent agent AI call failed: exhausted all retries")

        except httpx.TimeoutException:
            raise TimeoutError("Intent agent AI call timed out")

        # Parse AI response
        try:
            content = ai_response["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to parse AI response: {e}")

        # Validate output envelope (Section 11.1)
        validated = _validate_tier1_output(parsed, domain, schema_version)
        return validated


def _validate_tier1_output(
    output: dict, expected_domain: str, expected_schema_version: str
) -> dict:
    """Validate Tier-1 output envelope per Section 11.1."""
    required_keys = ["task", "domain", "schema_version", "result", "confidence"]
    for key in required_keys:
        if key not in output:
            raise RuntimeError(f"Tier-1 output missing required key: '{key}'")

    if output["domain"] != expected_domain:
        raise RuntimeError(
            f"Tier-1 domain mismatch: expected '{expected_domain}', got '{output['domain']}'"
        )

    if output["schema_version"] != expected_schema_version:
        raise RuntimeError(
            f"Tier-1 schema_version mismatch: expected '{expected_schema_version}'"
        )

    confidence = output.get("confidence", 0.0)
    if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        output["confidence"] = 0.5  # Default on invalid

    return output
