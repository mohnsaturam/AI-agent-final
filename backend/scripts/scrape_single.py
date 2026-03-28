"""
Universal AI-Driven Scraper

Fetches HTML with Playwright, then sends cleaned HTML to AI
for intelligent extraction. No hardcoded selectors — the AI
dynamically analyzes any site's structure.
"""

import asyncio
import json
import sys
import re
import httpx

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


def _clean_html(raw_html: str, max_chars: int = 30000) -> str:
    """
    Clean HTML for AI consumption:
    - Strip <script>, <style>, <svg>, <noscript>
    - Remove attributes except href, class, data-testid, aria-label
    - Collapse whitespace
    - Truncate to max_chars
    """
    soup = BeautifulSoup(raw_html, "html.parser")

    # Remove non-content tags
    for tag in soup.find_all(["script", "style", "svg", "noscript", "link", "meta", "head"]):
        tag.decompose()

    # Strip noisy attributes, keep only semantically useful ones
    keep_attrs = {"href", "class", "data-testid", "aria-label", "alt", "title", "datetime"}
    for tag in soup.find_all(True):
        attrs = dict(tag.attrs)
        for attr in attrs:
            if attr not in keep_attrs:
                del tag[attr]

    text = str(soup)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    # Truncate
    if len(text) > max_chars:
        text = text[:max_chars] + "\n<!-- TRUNCATED -->"
    return text


async def scrape(url: str, extraction_schema: dict, intent: dict, ai_config: dict):
    """Fetch page and extract data using AI."""
    try:
        # ── Step 1: Fetch HTML with Playwright ──
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
            context = await browser.new_context(user_agent=user_agent)
            page = await context.new_page()
            page.set_default_timeout(15000)

            response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            status = response.status if response else 0

            if status >= 400:
                print(json.dumps({"error": f"Status {status}", "status": status}))
                await browser.close()
                return

            # Wait for dynamic content to load
            await page.wait_for_timeout(2000)
            raw_html = await page.content()
            await browser.close()

        # ── Step 2: Clean HTML for AI ──
        cleaned_html = _clean_html(raw_html)

        # ── Step 3: AI Extraction ──
        result = await _ai_extract(cleaned_html, url, extraction_schema, intent, ai_config)
        result["status"] = status
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({"error": str(e)}))


async def _ai_extract(
    html: str,
    url: str,
    extraction_schema: dict,
    intent: dict,
    ai_config: dict,
) -> dict:
    """Send cleaned HTML to AI for structured extraction."""

    query_type = intent.get("query_type", "details")
    limit = intent.get("limit", 10)

    # Build field descriptions from schema
    fields_desc = []
    for field, defn in extraction_schema.get("properties", {}).items():
        if field in ("source_url", "source_site"):
            continue  # We add these ourselves
        fields_desc.append(f"- {field} ({defn.get('type', 'string')}): {defn.get('description', '')}")
    fields_text = "\n".join(fields_desc)

    if query_type in ("list", "search"):
        output_instruction = (
            f"Return a JSON object with:\n"
            f"- \"items\": an array of up to {limit} movie objects found on the page\n"
            f"- Each item should have the relevant fields from the schema below\n"
            f"- At minimum, include: title, year (if available), rating (if available)\n"
            f"- \"total_found\": number of items extracted\n\n"
            f"If the page is a search/listing page, extract ALL visible movie entries (up to {limit})."
        )
    else:
        output_instruction = (
            "Return a JSON object with the extracted fields from the schema below.\n"
            "Extract as many fields as you can find on the page.\n"
            "If a field is not found, omit it."
        )

    system_prompt = (
        "You are a universal web data extraction agent.\n"
        "You are given the cleaned HTML of a web page and a data schema.\n"
        "Your job is to analyze the HTML structure and extract the relevant data.\n\n"
        "Rules:\n"
        "- Extract data ONLY from the HTML content provided\n"
        "- Do NOT invent or hallucinate data\n"
        "- If you cannot find a field, omit it\n"
        "- Return valid JSON only, no markdown formatting\n\n"
        f"Schema fields:\n{fields_text}\n\n"
        f"{output_instruction}"
    )

    user_message = (
        f"URL: {url}\n"
        f"Intent: {json.dumps(intent)}\n\n"
        f"HTML content:\n{html}"
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{ai_config['base_url']}/chat/completions",
                headers={
                    "Authorization": f"Bearer {ai_config['api_key']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": ai_config.get("model", "llama-3.3-70b-versatile"),
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            ai_response = resp.json()

        content = ai_response["choices"][0]["message"]["content"]
        extracted = json.loads(content)

    except Exception as e:
        return {
            "success": False,
            "extracted_data": {},
            "confidence": 0.0,
            "field_confidence": {},
            "error": f"AI extraction failed: {str(e)}",
        }

    # Normalize the output
    from urllib.parse import urlparse
    site_domain = urlparse(url).netloc.replace("www.", "")

    if query_type in ("list", "search"):
        items = extracted.get("items", [])
        if not items and isinstance(extracted, dict):
            # AI might have returned fields directly; wrap
            items = [extracted]

        return {
            "success": len(items) > 0,
            "extracted_data": {
                "items": items,
                "total_found": extracted.get("total_found", len(items)),
                "source_url": url,
                "source_site": site_domain,
            },
            "confidence": 0.7 if items else 0.0,
            "field_confidence": {"items": 0.7 if items else 0.0},
        }
    else:
        # Single item — add source info
        extracted["source_url"] = url
        extracted["source_site"] = site_domain

        filled = [k for k, v in extracted.items() if v and k not in ("source_url", "source_site")]
        confidence = min(len(filled) / 5.0, 1.0)  # Rough heuristic

        return {
            "success": bool(filled),
            "extracted_data": extracted,
            "confidence": confidence,
            "field_confidence": {k: 0.7 for k in filled},
        }


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Test OK")
        sys.exit(0)

    try:
        raw_input = sys.stdin.read()
        if not raw_input:
            print(json.dumps({"error": "No input received on stdin"}))
            sys.exit(1)
        job = json.loads(raw_input)
    except Exception as e:
        print(json.dumps({"error": f"Failed to parse input: {str(e)}"}))
        sys.exit(1)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(scrape(
        url=job["url"],
        extraction_schema=job.get("extraction_schema", {}),
        intent=job.get("intent", {}),
        ai_config=job.get("ai_config", {}),
    ))
