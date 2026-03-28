"""
MCP Client — Scoring Agent (Tier-1) — Step 8c

Fully AI-based DOM scoring with Chain of Thought (COT) reasoning.

The AI evaluates DOM signals from inspect_url_dom output and produces:
  - A confidence score (0–100)
  - COT reasoning explaining the score
  - Recommendation (VALID / RETRY / SKIP)

NO heuristic scoring — the AI drives all scoring decisions with
structured COT reasoning on the DOM signals.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.scoring")


class ScoringAgent:
    """Tier-1 AI agent for DOM relevance scoring with COT reasoning."""

    TOOL_NAME = "scoring_agent"
    CALLER = "site_pipeline"

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        AI-based DOM scoring with Chain of Thought reasoning.

        The AI receives:
          - Cleaned HTML (truncated for token efficiency)
          - The user's intent
          - The extraction schema (what fields we need)
          - The URL being evaluated

        The AI produces COT reasoning analyzing:
          - Does the page contain the expected content type?
          - Does it match the user's intent (title, year, language)?
          - Is there structured data (JSON-LD, microdata)?
          - Is this a detail page, search results, or irrelevant page?
          - Are there error signals (captcha, login, 403)?

        Returns a score 0–100 with reasoning.
        """
        # Phase 2 key for DOM scoring
        current_key = settings.ai_api_key_phase2
        if not current_key:
            return {
                "confidence": 0,
                "validation_summary": "AI_API_KEY_PHASE2 not configured",
                "recommendation": "RETRY",
                "scoring_method": "ai_error",
                "cot_reasoning": "Phase 2 API key not configured.",
            }

        input_data = context.input_data
        html = input_data.get("html", "")
        url = input_data.get("url", "")
        intent = input_data.get("intent", {})
        extraction_schema = input_data.get("extraction_schema", {})
        domain = input_data.get("domain", "")
        expected_page_type = input_data.get("expected_page_type", "")

        if not html:
            return {
                "confidence": 0,
                "validation_summary": "No HTML content to score",
                "recommendation": "SKIP",
                "scoring_method": "ai",
                "cot_reasoning": "Page has no HTML content — cannot evaluate.",
            }

        schema_fields = list(extraction_schema.get("properties", {}).keys())

        # Truncate HTML for token efficiency (keep first ~8000 chars)
        html_truncated = html[:8000]
        if len(html) > 8000:
            html_truncated += f"\n... [truncated, total {len(html)} chars]"

        system_prompt = (
            "You are a page relevance scoring agent. You must evaluate whether "
            "the given HTML page contains the data the user is looking for.\n\n"
            "Use Chain of Thought (COT) reasoning to analyze the page step by step, "
            "then provide a final confidence score.\n\n"
            "## Scoring Criteria\n"
            "Evaluate these signals and reason through each:\n\n"
            "1. **Content Match**: Does the page contain content matching the user's intent?\n"
            "   - Check for title matches, year matches, language matches\n"
            "   - Look for schema.org structured data (JSON-LD)\n"
            "2. **Page Type**: Is this a detail page, search results, listing, or irrelevant?\n"
            "   - Detail pages (single item) are best for single-item queries\n"
            "   - Search/listing pages are best for list/discovery queries\n"
            "3. **Data Quality**: Can we likely extract the required fields?\n"
            f"   - Required fields: {schema_fields}\n"
            "   - Look for structured data, clear headings, labeled content\n"
            "4. **Error Detection**: Is there evidence of blocking?\n"
            "   - Check for captcha, login walls, access denied, rate limiting\n"
            "   - Very short pages (< 500 chars) suggest errors\n"
            "5. **Domain Relevance**: Does the page belong to the expected content domain?\n"
            f"   - Expected domain: {domain}\n"
        )

        # Inject expected page type constraint
        if expected_page_type:
            system_prompt += (
                f"\n6. **CRITICAL — Expected Page Type**: The execution strategy expects "
                f"a '{expected_page_type}' page.\n"
                f"   - If this page is NOT a '{expected_page_type}', set confidence to 0-5.\n"
                f"   - A search results page when expecting 'detail' = score 0.\n"
                f"   - A detail page when expecting 'list' = score 0.\n"
            )

        system_prompt += (
            "\n## Output Format\n"
            "Respond with valid JSON only:\n"
            "{\n"
            '  "cot_reasoning": "Step 1: [analysis]... Step 2: [analysis]... Step 3: ...",\n'
            '  "confidence": <0-100 integer>,\n'
            '  "recommendation": "VALID" | "RETRY" | "SKIP",\n'
            '  "page_type": "detail" | "search_results" | "listing" | "error" | "other",\n'
            '  "validation_summary": "<one line summary of findings>",\n'
            '  "items_found": <estimated number of items on page>,\n'
            '  "missing_filters": ["<filter not satisfied>", ...]\n'
            "}\n\n"
            "Score guide:\n"
            "- 90-100: Perfect match, all data likely extractable\n"
            "- 70-89: Good match, most data present\n"
            "- 50-69: Partial match, some data present\n"
            "- 25-49: Weak match, minimal relevant data\n"
            "- 0-24: Not relevant, error page, or blocked"
        )

        user_prompt = (
            f"## User Intent\n"
            f"```json\n{json.dumps(intent, indent=2)}\n```\n\n"
            f"## URL Being Evaluated\n{url}\n\n"
            f"## Page HTML (truncated)\n```html\n{html_truncated}\n```\n\n"
            "Analyze this page with COT reasoning and score it."
        )

        try:
            # Retry with exponential backoff on 429 (rate limit)
            max_retries = 3
            ai_response = None
            for retry_attempt in range(max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=settings.timeout_scoring_agent) as client:
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
                                    {"role": "user", "content": user_prompt},
                                ],
                                "temperature": 0.0,
                                "response_format": {"type": "json_object"},
                            },
                        )
                        response.raise_for_status()
                        ai_response = response.json()
                        break  # Success — exit retry loop
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429 and retry_attempt < max_retries:
                        wait_secs = 2 ** (retry_attempt + 1)  # 2s, 4s, 8s
                        logger.warning(
                            "Scoring agent hit 429 rate limit, retrying in %ds (attempt %d/%d)",
                            wait_secs, retry_attempt + 1, max_retries,
                        )
                        await asyncio.sleep(wait_secs)
                        continue
                    raise  # Non-429 error or exhausted retries — propagate

            if ai_response is None:
                raise RuntimeError("AI scoring exhausted all retries")

            content = ai_response["choices"][0]["message"]["content"]
            parsed = json.loads(content)

            confidence = max(0, min(100, int(parsed.get("confidence", 0))))
            detected_page_type = parsed.get("page_type", "other")

            # Enforce page type mismatch penalty
            if expected_page_type and detected_page_type:
                # Normalize to canonical types so equivalent types don't mismatch.
                # list_page / search_results / listing are all "list" for scoring.
                _type_map = {
                    "detail": "detail",
                    "detail_page": "detail",
                    "search_results": "list",
                    "search": "list",
                    "listing": "list",
                    "list": "list",
                    "list_page": "list",
                }
                expected_norm = _type_map.get(expected_page_type, expected_page_type)
                detected_norm = _type_map.get(detected_page_type, detected_page_type)

                if expected_norm != detected_norm and detected_norm not in ("other", "error"):
                    logger.info(
                        "Page type mismatch: expected=%s, detected=%s — penalizing score",
                        expected_page_type, detected_page_type,
                    )
                    confidence = min(confidence, 5)
                    parsed["validation_summary"] = (
                        f"PAGE_TYPE_MISMATCH: expected '{expected_page_type}', "
                        f"got '{detected_page_type}'. {parsed.get('validation_summary', '')}"
                    )

            return {
                "confidence": confidence,
                "validation_summary": parsed.get("validation_summary", ""),
                "recommendation": parsed.get("recommendation", "RETRY"),
                "scoring_method": "ai_cot",
                "cot_reasoning": parsed.get("cot_reasoning", ""),
                "page_type": detected_page_type,
                "items_found": parsed.get("items_found", 0),
                "missing_filters": parsed.get("missing_filters", []),
            }

        except Exception as e:
            logger.warning("AI COT scoring failed: %s", e)
            return {
                "confidence": 0,
                "validation_summary": f"AI scoring failed: {str(e)}",
                "recommendation": "RETRY",
                "scoring_method": "ai_error",
                "cot_reasoning": f"AI call failed with error: {str(e)}",
            }
