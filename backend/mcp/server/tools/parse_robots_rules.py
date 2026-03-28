"""
MCP Tool — parse_robots_rules (Step 7b)

Parse raw robots.txt content into structured rules.
"""

import logging
import re
from typing import Any, Dict

logger = logging.getLogger("umsa.tools.parse_robots_rules")


async def execute(context, db_pool) -> Dict[str, Any]:
    """Parse robots.txt content into structured rules."""
    input_data = context.input_data
    raw_content = input_data.get("raw_content", "")
    site_domain = input_data.get("site_domain", "")

    if not raw_content:
        return {
            "site_domain": site_domain,
            "rules": {"user_agents": {}, "sitemaps": []},
            "status": "no_content",
            "fully_blocked": False,
        }

    rules = _parse_robots_txt(raw_content)

    # Determine if fully blocked
    wildcard_rules = rules.get("user_agents", {}).get("*", {})
    disallowed = wildcard_rules.get("disallow", [])
    allowed = wildcard_rules.get("allow", [])

    fully_blocked = "/" in disallowed and not allowed

    return {
        "site_domain": site_domain,
        "rules": rules,
        "status": "blocked" if fully_blocked else "compliant",
        "fully_blocked": fully_blocked,
    }


def _parse_robots_txt(content: str) -> dict:
    """Parse robots.txt into structured rules."""
    rules = {"user_agents": {}, "sitemaps": []}
    current_ua = None

    for line in content.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Handle Sitemap
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip() if ":" in line else ""
            if url:
                rules["sitemaps"].append(url)
            continue

        # Handle User-agent
        match = re.match(r"^user-agent:\s*(.+)$", line, re.IGNORECASE)
        if match:
            current_ua = match.group(1).strip()
            if current_ua not in rules["user_agents"]:
                rules["user_agents"][current_ua] = {"allow": [], "disallow": []}
            continue

        if current_ua is None:
            continue

        # Handle Allow
        match = re.match(r"^allow:\s*(.*)$", line, re.IGNORECASE)
        if match:
            path = match.group(1).strip()
            if path:
                rules["user_agents"][current_ua]["allow"].append(path)
            continue

        # Handle Disallow
        match = re.match(r"^disallow:\s*(.*)$", line, re.IGNORECASE)
        if match:
            path = match.group(1).strip()
            if path:
                rules["user_agents"][current_ua]["disallow"].append(path)
            continue

    return rules
