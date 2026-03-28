"""
DOM Validator — DOM-Level Semantic Validation Engine

Fetches a URL via Playwright, cleans the HTML, then uses AI to score
how relevant the page content is to the user's intent.

Used in the iterative URL discovery loop:
  generate URL → fetch DOM → score relevance → accept/retry

Supports 2-mode rendering:
  Mode A (light): Allow all JS, block images/fonts/media. Default.
  Mode B (full): Allow everything, wait for networkidle. Escalation only.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("umsa.dom_validator")


def _clean_html(raw_html: str, max_chars: int = 150000) -> str:
    """
    Clean HTML for AI consumption:
    - Strip non-content <script>, <style>, <svg>, <noscript>
    - PRESERVE <title> and JSON-LD <script type="application/ld+json">
    - PRESERVE <meta property="og:*"> tags for OpenGraph extraction
    - Preserve attributes: href, src, class, data-testid, aria-label,
      itemprop, itemscope, itemtype, property, content, alt, title, datetime, type
    - Collapse whitespace
    - Truncate to max_chars
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw_html, "html.parser")

    # Extract title before stripping head
    title_tag = soup.find("title")
    title_text = title_tag.string.strip() if title_tag and title_tag.string else ""

    # Extract JSON-LD (structured data) before stripping scripts
    json_ld_blocks = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        json_ld_blocks.append(str(script))

    # Extract OpenGraph meta tags before stripping head
    og_blocks = []
    for meta in soup.find_all("meta", attrs={"property": re.compile(r"^og:")}):
        og_blocks.append(str(meta))

    # Remove non-content tags
    for tag in soup.find_all("style"):
        tag.decompose()
    for tag in soup.find_all("svg"):
        tag.decompose()
    for tag in soup.find_all("noscript"):
        tag.decompose()
    for tag in soup.find_all("link"):
        tag.decompose()

    # Remove scripts EXCEPT JSON-LD
    for tag in soup.find_all("script"):
        script_type = tag.get("type", "")
        if script_type != "application/ld+json":
            tag.decompose()

    # Remove <head> meta/link tags but keep <title>
    head = soup.find("head")
    if head:
        for tag in head.find_all("meta"):
            tag.decompose()

    # Strip noisy attributes, keep semantically useful ones
    # IMPORTANT: keep itemprop, itemscope, itemtype for Microdata extraction
    # IMPORTANT: keep src for images, property+content for OG
    keep_attrs = {
        "href", "src", "data-src", "class", "data-testid", "aria-label",
        "alt", "title", "datetime", "type",
        "itemprop", "itemscope", "itemtype",  # Microdata
        "property", "content",                 # OpenGraph
    }
    for tag in soup.find_all(True):
        attrs = dict(tag.attrs)
        for attr in attrs:
            if attr not in keep_attrs:
                del tag[attr]

    text = str(soup)

    # Re-inject title at the top if it was found
    if title_text:
        text = f"<title>{title_text}</title>\n{text}"

    # Re-inject OpenGraph meta tags
    if og_blocks:
        text = "\n".join(og_blocks) + "\n" + text

    # Re-inject JSON-LD blocks if they were found
    if json_ld_blocks:
        text = "\n".join(json_ld_blocks) + "\n" + text

    text = re.sub(r"\s+", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n<!-- TRUNCATED -->"
    return text


async def fetch_dom(
    url: str, timeout_ms: int = 15000, mode: str = "light"
) -> Dict[str, Any]:
    """
    Fetch a URL's DOM content using Playwright in a subprocess.

    Modes:
        "light" (default): Allow all JS. Block images, fonts, media only.
                           wait_until=domcontentloaded + 2s settle.
        "full" (escalation): Allow everything. wait_until=networkidle.
                             Timeout 30s. Used when light mode yields insufficient content.
    """
    start = time.monotonic()

    if mode == "full":
        actual_timeout = max(timeout_ms, 30000)
        wait_until = "networkidle"
        block_types = '("image", "font", "media")'  # minimal blocking
    else:
        actual_timeout = timeout_ms
        wait_until = "domcontentloaded"
        block_types = '("image", "font", "media")'  # allow JS + stylesheets

    script = f'''
import asyncio, json, sys
from playwright.async_api import async_playwright

async def fetch():
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={{"width": 1280, "height": 720}},
                locale="en-US",
            )
            page = await ctx.new_page()

            # Block only heavy resources — allow JS and stylesheets
            async def block_resources(route):
                if route.request.resource_type in {block_types}:
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_resources)

            page.set_default_timeout({actual_timeout})
            resp = await page.goto("{url}", wait_until="{wait_until}", timeout={actual_timeout})
            status = resp.status if resp else 0
            if status >= 400:
                print(json.dumps({{"error": f"HTTP {{status}}", "status": status, "html": ""}}))
            else:
                # Wait for page to settle, then get content with retry
                html = ""
                for attempt in range(3):
                    try:
                        if attempt == 0:
                            await page.wait_for_timeout(2000)
                        else:
                            await page.wait_for_load_state("networkidle", timeout=30000)
                        
                        # Cleanup bloat BEFORE capturing content to maximize useful chars
                        await page.evaluate("""() => {{
                            const selectors = 'script:not([type="application/ld+json"]), style, svg, noscript, iframe';
                            document.querySelectorAll(selectors).forEach(el => el.remove());
                        }}""")

                        html = await page.content()
                        break
                    except Exception as content_err:
                        if attempt < 2:
                            await page.wait_for_timeout(1500)
                        else:
                            print(json.dumps({{"error": str(content_err), "status": status, "html": ""}}))
                            await browser.close()
                            return
                print(json.dumps({{"status": status, "html": html, "error": None}}))
            await browser.close()
    except Exception as e:
        print(json.dumps({{"error": str(e), "status": 0, "html": ""}}))

asyncio.run(fetch())
'''

    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-c", script],
            text=True,
            capture_output=True,
            timeout=60,
        )

        latency_ms = int((time.monotonic() - start) * 1000)

        if proc.returncode != 0:
            return {
                "success": False,
                "html": "",
                "status_code": 0,
                "error": f"Playwright process failed: {proc.stderr[:500]}",
                "latency_ms": latency_ms,
            }

        # Parse result
        json_line = None
        for line in proc.stdout.strip().split("\n"):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                json_line = line

        if not json_line:
            return {
                "success": False,
                "html": "",
                "raw_html": "",
                "status_code": 0,
                "error": "No JSON in Playwright output",
                "latency_ms": latency_ms,
            }

        res = json.loads(json_line)

        if res.get("error"):
            return {
                "success": False,
                "html": "",
                "raw_html": "",
                "status_code": res.get("status", 0),
                "error": res["error"],
                "latency_ms": latency_ms,
            }

        # Keep raw HTML for DOM signal extraction
        raw_html = res.get("html", "")

        # Clean the HTML for AI consumption
        cleaned = _clean_html(raw_html)

        return {
            "success": True,
            "html": cleaned,
            "raw_html": raw_html[:500000],  # cap raw at 500K (now it's mostly body content)
            "status_code": res.get("status", 200),
            "error": None,
            "latency_ms": latency_ms,
        }

    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "html": "",
            "status_code": 0,
            "error": "DOM fetch timed out (30s)",
            "latency_ms": int((time.monotonic() - start) * 1000),
        }
    except Exception as e:
        return {
            "success": False,
            "html": "",
            "status_code": 0,
            "error": str(e),
            "latency_ms": int((time.monotonic() - start) * 1000),
        }


async def fetch_dom_with_scroll(
    url: str, timeout_ms: int = 30000
) -> Dict[str, Any]:
    """
    Fetch a URL's DOM content with comprehensive smart scrolling.

    Five-strategy scroll approach:
      1. Gradual scrolling – 500px increments, 300ms pauses
      2. Height change detection – tracks scrollHeight growth
      3. Network activity monitoring – injected JS interceptors for XHR/fetch
      4. Load More button detection – finds & clicks common expand buttons
      5. Stability + timeout stop – 3 stable checks or 50s hard limit

    Returns same format as fetch_dom for drop-in compatibility,
    plus scroll_stats for diagnostics.
    """
    start = time.monotonic()

    script = f'''
import asyncio, json, sys
from playwright.async_api import async_playwright

async def fetch():
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={{"width": 1280, "height": 720}},
                locale="en-US",
            )
            page = await ctx.new_page()

            async def block_resources(route):
                if route.request.resource_type in ("image", "font", "media"):
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_resources)

            page.set_default_timeout({timeout_ms})
            resp = await page.goto("{url}", wait_until="domcontentloaded", timeout={timeout_ms})
            status = resp.status if resp else 0
            if status >= 400:
                print(json.dumps({{"error": f"HTTP {{status}}", "status": status, "html": "", "scrolled": False}}))
            else:
                # Wait for initial JS hydration (React, Next.js, etc.)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except:
                    await page.wait_for_timeout(3000)

                # ── STRATEGY 3: Inject network activity monitor ──
                # Intercepts fetch() and XMLHttpRequest to track pending requests.
                await page.evaluate("""() => {{
                    window.__pendingRequests = 0;

                    // Intercept fetch()
                    const origFetch = window.fetch;
                    window.fetch = function(...args) {{
                        window.__pendingRequests++;
                        return origFetch.apply(this, args)
                            .then(r => {{ window.__pendingRequests--; return r; }})
                            .catch(e => {{ window.__pendingRequests--; throw e; }});
                    }};

                    // Intercept XMLHttpRequest
                    const origOpen = XMLHttpRequest.prototype.open;
                    const origSend = XMLHttpRequest.prototype.send;
                    XMLHttpRequest.prototype.open = function(...args) {{
                        this.__tracked = true;
                        return origOpen.apply(this, args);
                    }};
                    XMLHttpRequest.prototype.send = function(...args) {{
                        if (this.__tracked) {{
                            window.__pendingRequests++;
                            this.addEventListener('loadend', () => {{ window.__pendingRequests--; }});
                        }}
                        return origSend.apply(this, args);
                    }};
                }}""")

                import time as _time
                scroll_start = _time.time()
                MAX_SCROLL_TIME = 50  # hard timeout (seconds)
                SCROLL_STEP = 500    # pixels per scroll
                SCROLL_PAUSE = 300   # ms between scrolls
                STABLE_CHECKS_NEEDED = 3  # consecutive stable checks to stop

                prev_height = 0
                stable_count = 0
                scroll_ok = True
                scrolls_done = 0
                buttons_clicked = 0

                while True:
                    elapsed = _time.time() - scroll_start
                    if elapsed > MAX_SCROLL_TIME:
                        break

                    try:
                        # ── STRATEGY 1: Gradual scroll ──
                        await page.evaluate(f"window.scrollBy(0, {{SCROLL_STEP}})")
                        await page.wait_for_timeout(SCROLL_PAUSE)
                        scrolls_done += 1

                        # ── STRATEGY 2: Height change detection ──
                        curr_height = await page.evaluate("document.body.scrollHeight")
                        scroll_pos = await page.evaluate("window.scrollY + window.innerHeight")

                        # ── STRATEGY 3: Check pending network requests ──
                        pending = await page.evaluate("window.__pendingRequests || 0")

                        # If requests are in flight, give them time to finish
                        if pending > 0:
                            waited = 0
                            while waited < 2000:
                                await page.wait_for_timeout(200)
                                waited += 200
                                pending = await page.evaluate("window.__pendingRequests || 0")
                                if pending <= 0:
                                    break
                            # Re-check height — new content may have loaded
                            curr_height = await page.evaluate("document.body.scrollHeight")

                        # ── STRATEGY 4: Load More button detection ──
                        try:
                            load_more_clicked = await page.evaluate("""() => {{
                                const keywords = ['load more', 'show more', 'see more',
                                                  'view more', 'expand', 'show all'];
                                const selectors = 'button, a, [role="button"]';
                                for (const el of document.querySelectorAll(selectors)) {{
                                    const txt = (el.textContent || '').trim().toLowerCase();
                                    const rect = el.getBoundingClientRect();
                                    // Must be visible and in viewport
                                    if (rect.width === 0 || rect.height === 0) continue;
                                    if (rect.top < 0 || rect.top > window.innerHeight) continue;
                                    for (const kw of keywords) {{
                                        if (txt.includes(kw) && txt.length < 50) {{
                                            el.click();
                                            return true;
                                        }}
                                    }}
                                }}
                                return false;
                            }}""")
                            if load_more_clicked:
                                buttons_clicked += 1
                                await page.wait_for_timeout(1000)
                                curr_height = await page.evaluate("document.body.scrollHeight")
                        except:
                            pass

                        # ── STRATEGY 5: Stability check ──
                        at_bottom = scroll_pos >= curr_height - 50
                        height_stable = (curr_height == prev_height)
                        no_network = (pending <= 0)

                        if at_bottom and height_stable and no_network:
                            stable_count += 1
                            if stable_count >= STABLE_CHECKS_NEEDED:
                                break
                        else:
                            stable_count = 0

                        prev_height = curr_height

                    except Exception:
                        scroll_ok = False
                        break

                # Final settle — wait for any remaining requests
                if scroll_ok:
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except:
                        pass

                scroll_time = round(_time.time() - scroll_start, 1)
                final_height = await page.evaluate("document.body.scrollHeight") if scroll_ok else prev_height

                # Scroll back to top before capturing
                try:
                    await page.evaluate("window.scrollTo(0, 0)")
                    await page.wait_for_timeout(500)
                except:
                    pass

                # ── DOM Cleanup: strip non-content bloat ──
                # Scripts, styles, SVGs, iframes, etc. can bloat HTML to 2MB+.
                # Stripping them shrinks it to ~400KB of actual content, so
                # the box office section at the bottom won't be truncated.
                try:
                    await page.evaluate(\"\"\"() => {{
                        const selectors = 'script, style, svg, noscript, iframe, link[rel=stylesheet]';
                        document.querySelectorAll(selectors).forEach(el => el.remove());
                        const walker = document.createTreeWalker(
                            document, NodeFilter.SHOW_COMMENT, null, false
                        );
                        const comments = [];
                        while (walker.nextNode()) comments.push(walker.currentNode);
                        comments.forEach(c => c.parentNode.removeChild(c));
                    }}\"\"\")
                except:
                    pass  # If cleanup fails, we still capture full HTML

                try:
                    html = await page.content()
                    print(json.dumps({{
                        "status": status,
                        "html": html,
                        "error": None,
                        "scrolled": scroll_ok,
                        "scroll_stats": {{
                            "scrolls_done": scrolls_done,
                            "height_final": final_height,
                            "buttons_clicked": buttons_clicked,
                            "time_sec": scroll_time,
                        }},
                    }}))
                except:
                    print(json.dumps({{"error": "Failed to capture HTML after scroll", "status": status, "html": "", "scrolled": False}}))
            await browser.close()
    except Exception as e:
        print(json.dumps({{"error": str(e), "status": 0, "html": "", "scrolled": False}}))

asyncio.run(fetch())
'''

    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-c", script],
            text=True,
            capture_output=True,
            timeout=90,
        )

        latency_ms = int((time.monotonic() - start) * 1000)

        if proc.returncode != 0:
            return {
                "success": False,
                "html": "",
                "raw_html": "",
                "status_code": 0,
                "error": f"Playwright scroll process failed: {proc.stderr[:500]}",
                "latency_ms": latency_ms,
                "scrolled": False,
            }

        json_line = None
        for line in proc.stdout.strip().split("\n"):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                json_line = line

        if not json_line:
            return {
                "success": False,
                "html": "",
                "raw_html": "",
                "status_code": 0,
                "error": "No JSON in Playwright scroll output",
                "latency_ms": latency_ms,
                "scrolled": False,
            }

        res = json.loads(json_line)

        if res.get("error"):
            return {
                "success": False,
                "html": "",
                "raw_html": "",
                "status_code": res.get("status", 0),
                "error": res["error"],
                "latency_ms": latency_ms,
                "scrolled": False,
            }

        raw_html = res.get("html", "")
        cleaned = _clean_html(raw_html)

        return {
            "success": True,
            "html": cleaned,
            "raw_html": raw_html[:1_000_000],
            "status_code": res.get("status", 200),
            "error": None,
            "latency_ms": latency_ms,
            "scrolled": res.get("scrolled", True),
            "scroll_stats": res.get("scroll_stats", {}),
        }

    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "html": "",
            "raw_html": "",
            "status_code": 0,
            "error": "DOM scroll fetch timed out (90s)",
            "latency_ms": int((time.monotonic() - start) * 1000),
            "scrolled": False,
        }
    except Exception as e:
        return {
            "success": False,
            "html": "",
            "raw_html": "",
            "status_code": 0,
            "error": str(e),
            "latency_ms": int((time.monotonic() - start) * 1000),
            "scrolled": False,
        }


async def score_dom_relevance(
    cleaned_html: str,
    url: str,
    intent: dict,
    extraction_schema: dict,
    ai_config: dict,
) -> Dict[str, Any]:
    """
    AI-powered DOM relevance scoring.

    Analyzes the cleaned HTML against the user intent and returns:
        {
            "confidence": int (0-100),
            "validation_summary": str,
            "relevant_signals": list[str],
            "recommendation": "VALID" | "RETRY" | "SKIP",
            "extracted_preview": dict (lightweight preview of found data),
        }
    """
    # Build field descriptions from extraction schema
    fields_desc = []
    for field, defn in extraction_schema.get("properties", {}).items():
        if field in ("source_url", "source_site"):
            continue
        fields_desc.append(f"- {field} ({defn.get('type', 'string')}): {defn.get('description', '')}")
    fields_text = "\n".join(fields_desc)

    system_prompt = (
        "You are a DOM relevance validator for a web scraping system.\n"
        "You are given:\n"
        "1. The cleaned HTML of a web page\n"
        "2. The user's parsed intent (what they are looking for)\n"
        "3. The extraction schema (what fields are needed)\n\n"
        "Your job is to analyze the HTML and determine HOW RELEVANT this page is "
        "to the user's query.\n\n"
        "Evaluation criteria:\n"
        "- Does the page contain the TYPE of content requested? (e.g., movie list vs. single movie)\n"
        "- Does the content match the LANGUAGE filter? (e.g., Hindi, Tamil)\n"
        "- Does the content match the YEAR filter?\n"
        "- Does the page have ENOUGH items? (if user asked for 'top 25', does the page have 25+?)\n"
        "- Are the REQUIRED FIELDS present? (title, year, rating, etc.)\n"
        "- Is this a content page or an error/search-form/login page?\n\n"
        "Scoring guide:\n"
        "- 90-100: Perfect match — all filters satisfied, enough items, rich data\n"
        "- 75-89: Good match — most filters satisfied, some items found\n"
        "- 50-74: Partial match — some relevant content but missing key filters\n"
        "- 25-49: Weak match — page exists but content doesn't match intent well\n"
        "- 0-24: No match — error page, login page, or completely irrelevant\n\n"
        f"Extraction schema fields:\n{fields_text}\n\n"
        "Respond with JSON:\n"
        "{\n"
        '  "dom_insights_chain_of_thought": "<Step-by-step analysis: what data is actually visible in this HTML? How does it map to the user query?>",\n'
        '  "confidence": <0-100>,\n'
        '  "validation_summary": "<one sentence explaining your assessment based on the DOM insights>",\n'
        '  "relevant_signals": ["<matching entity/title/year found IN THE DOM>", ...],\n'
        '  "recommendation": "VALID" | "RETRY" | "SKIP",\n'
        '  "items_found": <number of matching items visible on page>,\n'
        '  "missing_filters": ["<filter not satisfied by the DOM>", ...],\n'
        '  "page_type": "list" | "detail" | "search_form" | "error" | "other"\n'
        "}"
    )

    user_message = (
        f"URL: {url}\n"
        f"User Intent: {json.dumps(intent, indent=2)}\n\n"
        f"HTML content (first 8000 chars):\n{cleaned_html[:8000]}"
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
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
        parsed = json.loads(content)

        confidence = int(parsed.get("confidence", 0))

        # Determine recommendation based on confidence
        if confidence >= 90:
            recommendation = "VALID"
        elif confidence >= 50:
            recommendation = "RETRY"
        else:
            recommendation = "SKIP"

        return {
            "confidence": confidence,
            "validation_summary": parsed.get("validation_summary", ""),
            "relevant_signals": parsed.get("relevant_signals", []),
            "recommendation": recommendation,
            "items_found": parsed.get("items_found", 0),
            "missing_filters": parsed.get("missing_filters", []),
            "page_type": parsed.get("page_type", "other"),
        }

    except Exception as e:
        logger.warning("DOM relevance scoring failed: %s", e)
        return {
            "confidence": 0,
            "validation_summary": f"AI scoring failed: {str(e)}",
            "relevant_signals": [],
            "recommendation": "RETRY",
            "items_found": 0,
            "missing_filters": [],
            "page_type": "error",
        }
