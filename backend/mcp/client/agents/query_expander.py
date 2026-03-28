import asyncio
import json
import logging
from typing import Any, Dict, List

import httpx

from app.core.config import settings

logger = logging.getLogger("umsa.agents.query_expander")

# Maximum expanded queries to generate
MAX_EXPANDED_QUERIES = 5
MIN_EXPANDED_QUERIES = 3


class QueryExpander:
    """Gateway-compatible wrapper for query expansion."""

    @staticmethod
    async def execute(input_data: dict, context: Any = None) -> dict:
        """
        MCP gateway entry point.
        Input: {"intent": {...}, "site_domain": "optional"}
        Output: {"expanded_queries": [...], "count": N}
        """
        intent = input_data.get("intent", {})
        site_domain = input_data.get("site_domain", "")
        queries = await expand_query_for_multi_item(intent, site_domain)
        return {
            "expanded_queries": queries,
            "count": len(queries),
        }


async def expand_query_for_multi_item(
    intent: dict,
    site_domain: str = "",
) -> List[str]:
    """
    Use Groq AI to generate 3-5 search queries optimized for finding
    list/collection pages that match the user's multi-item query.

    Args:
        intent: The parsed intent from the intent agent.
        site_domain: Optional site domain for context (NOT added to queries here;
                     the caller adds site: restriction later).

    Returns:
        List of 3-5 search query strings. On failure, returns [raw_query]
        so the pipeline falls back gracefully to the existing single-query flow.
    """
    raw_query = (intent.get("raw_query") or "").strip()
    if not raw_query:
        return [raw_query] if raw_query else []

    # Build context from intent for the AI
    intent_context = _build_intent_context(intent)

    system_prompt = (
        "You are a search query optimization expert. Your task is to generate "
        "alternative Google search queries that help discover WEB PAGES containing "
        "LISTS or COLLECTIONS of items matching the user's request.\n\n"

        "STEP 1 — DETERMINE QUERY INTENT TYPE\n"
        "First determine whether the user query is requesting a LIST/COLLECTION "
        "or a RATING/RANKING of items.\n\n"

        "INTENT TYPES:\n"
        "LIST_COLLECTION:\n"
        "- The user wants a general list or catalog of items.\n"
        "- Examples: 'list of movies from 2017', 'movies released in 2020', "
        "'all smartphones launched in 2023'.\n\n"

        "RATING_RANKING:\n"
        "- The user wants ranked, rated, or curated items.\n"
        "- Examples: 'top 10 movies of 2017', 'best laptops under 1000', "
        "'highest rated sci-fi movies'.\n\n"

        "STEP 2 — GENERATE SEARCH QUERIES\n"
        "Generate search queries based on the detected intent.\n\n"

        "IF intent = LIST_COLLECTION:\n"
        "- Focus on queries that find database pages, release lists, "
        "or catalog-style pages.\n"
        "- Avoid ranking phrases such as: best, top, highest rated, greatest.\n"
        "- Example patterns:\n"
        "  'movies released in 2017'\n"
        "  '2017 film releases'\n"
        "  'list of films released in 2017'\n"
        "  'movies from 2017 database'\n\n"

        "IF intent = RATING_RANKING:\n"
        "- Generate queries targeting curated or ranked lists.\n"
        "- Example patterns:\n"
        "  'best movies of 2017'\n"
        "  'top movies 2017'\n"
        "  'highest rated films 2017'\n"
        "  'popular movies 2017'\n\n"

        "IMPORTANT RULES:\n"
        "1. Generate exactly 3 to 5 search queries.\n"
        "2. Each query must target a DIFFERENT discovery angle.\n"
        "3. DO NOT generate URLs — only search query text.\n"
        "4. DO NOT include site: restrictions — the system handles that.\n"
        "5. Prefer queries that return list pages, database pages, or collection pages.\n"
        "6. Avoid queries that lead to blog posts or opinion articles when intent is LIST_COLLECTION.\n"
        "7. If the user specified a year, make sure queries target THAT EXACT YEAR.\n"
        "8. If the user mentioned filters (genre, language, etc.), include them naturally.\n\n"

        "Respond ONLY with a JSON object in this format:\n"
        '{"intent_type": "LIST_COLLECTION or RATING_RANKING", '
        '"queries": ["query1", "query2", "query3", "query4"]}'
    )

    user_message = (
        f"User's original query: \"{raw_query}\"\n\n"
        f"Parsed intent context:\n{intent_context}\n\n"
        "Generate 3-5 Google search queries that will find web pages listing "
        "the items the user is looking for. Remember: NO site: restrictions, "
        "NO URLs, just search query text."
    )

    try:
        # Phase 3 key for query expansion
        queries = await _call_groq(system_prompt, user_message)

        if queries and len(queries) >= MIN_EXPANDED_QUERIES:
            logger.info(
                "Query expander: generated %d queries from: %r",
                len(queries), raw_query,
            )
            for i, q in enumerate(queries):
                logger.info("  Expanded query %d: %r", i + 1, q)
            return queries[:MAX_EXPANDED_QUERIES]

        # AI returned too few queries — include raw query as fallback
        logger.warning(
            "Query expander returned %d queries (need %d) — adding raw query",
            len(queries) if queries else 0, MIN_EXPANDED_QUERIES,
        )
        result = [raw_query]
        if queries:
            result.extend(queries)
        return result[:MAX_EXPANDED_QUERIES]

    except Exception as e:
        logger.warning("Query expander failed: %s — falling back to raw query", e)
        return [raw_query]


def _build_intent_context(intent: dict) -> str:
    """Build a human-readable context string from the parsed intent."""
    parts = []

    if intent.get("query_type"):
        parts.append(f"Query type: {intent['query_type']}")
    if intent.get("title"):
        parts.append(f"Title/subject: {intent['title']}")
    if intent.get("year"):
        parts.append(f"Year: {intent['year']}")
    if intent.get("language"):
        parts.append(f"Language: {intent['language']}")

    # Extract filters
    filters = intent.get("filters", {})
    if isinstance(filters, dict):
        for k, v in filters.items():
            if isinstance(v, dict) and "value" in v:
                parts.append(f"Filter {k}: {v['value']}")
            elif v:
                parts.append(f"Filter {k}: {v}")

    if intent.get("limit"):
        parts.append(f"Requested count: {intent['limit']}")
    if intent.get("primary_goal"):
        parts.append(f"Goal: {intent['primary_goal']}")
    if intent.get("ranking_strategy"):
        parts.append(f"Ranking: {intent['ranking_strategy']}")

    return "\n".join(parts) if parts else "No additional context available."


async def _call_groq(system_prompt: str, user_message: str) -> List[str]:
    """
    Call Groq AI (OpenAI-compatible endpoint) and parse the response.
    Returns a list of search query strings.
    """
    max_retries = 2

    for retry_attempt in range(max_retries + 1):
        # Phase 3 key for query expansion
        current_key = settings.ai_api_key_phase3
        
        try:
            async with httpx.AsyncClient(timeout=settings.timeout_url_agent) as client:
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
                        "temperature": 0.3,
                        "response_format": {"type": "json_object"},
                    },
                )
                response.raise_for_status()
                ai_response = response.json()

                content = ai_response["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                # ... query processing logic ...

                queries = parsed.get("queries", [])
                if isinstance(queries, list):
                    # Clean and validate each query
                    cleaned = []
                    for q in queries:
                        if isinstance(q, str) and q.strip():
                            # Remove any accidental site: restrictions
                            clean_q = q.strip()
                            if "site:" not in clean_q.lower():
                                cleaned.append(clean_q)
                            else:
                                # Strip the site: part
                                import re
                                clean_q = re.sub(r'\s*site:\S+', '', clean_q).strip()
                                if clean_q:
                                    cleaned.append(clean_q)
                    return cleaned

                return []

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 and retry_attempt < max_retries:
                wait_secs = 2 ** (retry_attempt + 1)
                logger.warning(
                    "Query expander hit 429 rate limit, retrying in %ds (%d/%d)",
                    wait_secs, retry_attempt + 1, max_retries,
                )
                await asyncio.sleep(wait_secs)
                continue
            raise

    return []
