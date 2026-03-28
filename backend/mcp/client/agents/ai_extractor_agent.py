"""
MCP Client — Full-Page AI Extractor Agent (Tier-1 fallback)

Extracts tabular or list-based data directly from unstructured
Markdown text (converted from HTML) using the Groq high-speed LLM.
Used as a powerful fallback when deterministic structural clustering
fails or generates low-quality noise.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
from pydantic import ValidationError

from app.core.config import settings

logger = logging.getLogger("umsa.agents.ai_extractor")


class AIExtractorAgent:
    """Tier-1 AI agent for full-page unstructured extraction."""

    TOOL_NAME = "ai_extractor_agent"
    CALLER = "site_pipeline"

    @staticmethod
    async def build_input(
        markdown_text: str,
        intent: dict,
        extraction_schema: dict,
        domain: str,
        schema_version: str,
        request_id: UUID,
    ) -> dict:
        """Build the extractor input envelope."""
        return {
            "markdown_text": markdown_text,
            "intent": intent,
            "extraction_schema": extraction_schema,
            "domain": domain,
            "schema_version": schema_version,
            "request_id": str(request_id),
        }

    @staticmethod
    async def execute(context, db_pool) -> Dict[str, Any]:
        """
        Tool implementation.
        Calls Groq AI to extract an array of items matching the schema.
        """
        current_key = settings.ai_api_key_phase1
        if not current_key:
            raise RuntimeError("AI_API_KEY_PHASE1 not configured")

        input_data = context.input_data
        md_text = input_data["markdown_text"]
        intent = input_data["intent"]
        ext_schema = input_data["extraction_schema"]
        domain = input_data["domain"]
        schema_version = input_data["schema_version"]
        request_id_str = input_data.get("request_id", "unknown")

        fl = context.logger if hasattr(context, "logger") else None

        if fl:
            fl.step("AIExtractorAgent: Starting Full-Page AI Extraction")
            fl.substep("Input", "INFO", f"Domain: {domain}\nMarkdown Length: {len(md_text)} chars")

        user_query = intent.get("query") or intent.get("title") or "Extract all list items"
        
        # Prevent massive payloads from exceeding Groq's free-tier limits.
        # Groq free tier: body limit is tight; 20k chars (~5k tokens) is safe.
        # The tables contain the most valuable data and appear early in the content.
        MAX_MD_CHARS = 20000  
        md_truncated = md_text[:MAX_MD_CHARS] if len(md_text) > MAX_MD_CHARS else md_text

        if len(md_text) > MAX_MD_CHARS and fl:
            fl.substep("Truncation", "WARNING", f"Markdown exceeded limit. Truncated from {len(md_text)} to {MAX_MD_CHARS} chars.")

        system_prompt = (
            "You are a sophisticated data extraction AI. You process unstructured but clean Markdown "
            "text (which may include Markdown tables like | Col1 | Col2 |) and convert it into a "
            "rigid JSON array of objects based on a required schema.\n\n"
            f"Domain: {domain}\n"
            f"Schema version: {schema_version}\n"
            f"Extraction Schema for each item:\n{json.dumps(ext_schema, indent=2)}\n\n"
            "CRITICAL INSTRUCTIONS:\n"
            f"1. FOCUS EXCLUSIVELY ON THIS USER QUERY: \"{user_query}\"\n"
            "2. Identify the core tables or lists that contain the requested data.\n"
            "3. BE EXHAUSTIVE AND COMPREHENSIVE: Extract EVERY SINGLE ITEM matching the query from the "
            "provided Markdown. Do not stop early, do not skip items, and do not truncate the list. "
            "If there are 50, 100, or more items, you MUST extract all of them from A to Z.\n"
            "4. Map all available structural information from the text into the corresponding JSON fields. "
            "Do not leave any relevant info behind. Missing fields should be mapped strictly to null.\n"
            "5. Ignore completely irrelevant sections like 'References', 'See also', or footnotes at the bottom.\n"
            "6. The entire output must be valid JSON in this exact structure:\n"
            '{"task": "full_page_extraction", "domain": "' + domain + '", '
            '"schema_version": "' + schema_version + '", '
            '"extracted_items": [{<item1 features>}, {<item2 features>}, ...], '
            '"items_count": <int>, "confidence": <0.0-1.0>}'
        )

        user_message = (
            f"Extract the items matching '{user_query}' from the following page Markdown:\n\n"
            f"=== PAGE MARKDOWN (truncated to {MAX_MD_CHARS} chars) ===\n"
            f"{md_truncated}\n"
            f"=== END MARKDOWN ==="
        )

        max_retries = 3
        ai_response = None

        for retry_attempt in range(max_retries + 1):
            try:
                # Use longer timeout for parsing large markdown tables
                async with httpx.AsyncClient(timeout=30.0) as client:
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
                            "temperature": 0.0, # Deterministic extraction
                            "max_tokens": 8000, # Large buffer for exhaustive extraction
                            "response_format": {"type": "json_object"},
                        },
                    )
                    response.raise_for_status()
                    ai_response = response.json()
                    break
            except httpx.HTTPStatusError as e:
                # 413 Payload Too Large shouldn't be retried
                if e.response.status_code == 413:
                    logger.error(f"Payload too large for AI extraction: {len(md_truncated)} chars provided.")
                    raise RuntimeError("AI Extractor failed: 413 Payload Too Large. Markdown is too large even after truncation.")
                    
                if e.response.status_code == 429 and retry_attempt < max_retries:
                    wait_secs = 2 ** (retry_attempt + 1)
                    logger.warning(
                        "AI Extractor hit 429 rate limit, retrying in %ds (attempt %d/%d)",
                        wait_secs, retry_attempt + 1, max_retries,
                    )
                    await asyncio.sleep(wait_secs)
                    continue
                raise
            except httpx.TimeoutException:
                if fl: fl.substep("NetworkError", "ERROR", "AI Extractor timed out")
                raise TimeoutError("AI Extractor timed out")
            except Exception as e:
                if fl: fl.substep("Error", "ERROR", f"AI Extractor failed: {e}")
                raise RuntimeError(f"AI Extractor failed: {e}")

        if ai_response is None:
            if fl: fl.substep("Error", "ERROR", "AI Extractor failed to get a response after retries.")
            raise RuntimeError("AI Extractor failed to get a response after retries.")

        try:
            content = ai_response["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, json.JSONDecodeError) as e:
            if fl: fl.substep("ParseError", "ERROR", f"Failed to parse extractor response: {e}")
            raise RuntimeError(f"Failed to parse extractor response: {e}")

        # Validate
        if "extracted_items" not in parsed:
            if fl: fl.substep("SchemaError", "ERROR", "Missing extracted_items in AI response")
            raise RuntimeError("Missing extracted_items in AI response")
        if not isinstance(parsed["extracted_items"], list):
            if fl: fl.substep("SchemaError", "ERROR", "extracted_items must be a list")
            raise RuntimeError("extracted_items must be a list")

        if fl:
            item_count = len(parsed["extracted_items"])
            fl.substep("Success", "OK", f"Extracted {item_count} items matching '{user_query}'")

        return parsed
