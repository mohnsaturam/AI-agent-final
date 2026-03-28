"""
MCP Server — Policy Engine (Section 7.2, Step 5)

Runs domain-specific policy checks loaded dynamically from domain modules.
The engine itself is domain-agnostic — it delegates to loaded policy rules.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse
import re

logger = logging.getLogger("umsa.policy_engine")


@dataclass
class PolicyResult:
    """Result of a policy pre-condition check."""
    allowed: bool
    reason: Optional[str] = None


# Security: rejected schemes and patterns (Section 16)
BLOCKED_SCHEMES = {"file", "ftp", "data"}
BLOCKED_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
    "[::1]",
}
IP_PATTERN = re.compile(
    r"^(\d{1,3}\.){3}\d{1,3}$"  # IPv4
    r"|^\[?[0-9a-fA-F:]+\]?$"   # IPv6
)


class PolicyEngine:
    """
    Domain-agnostic policy enforcement.
    Loads domain-specific rules dynamically; enforces security baseline.
    """

    def __init__(self):
        self._domain_rules: Dict[str, Callable] = {}

    def load_rules(self, domain: str, check_fn: Callable) -> None:
        """
        Register a domain-specific policy check function.
        The function signature must be: async def check(context) -> PolicyResult
        """
        self._domain_rules[domain] = check_fn
        logger.info("Loaded policy rules for domain: %s", domain)

    async def check(self, context: Any) -> PolicyResult:
        """
        Run all policy checks:
        1. Global security baseline
        2. Domain-specific rules (if registered)
        """
        # ── Global security checks (Section 16) ──
        security_result = self._check_security(context)
        if not security_result.allowed:
            return security_result

        # ── Domain-specific checks ──
        domain = getattr(context, "domain", None)
        if domain and domain in self._domain_rules:
            try:
                domain_result = await self._domain_rules[domain](context)
                if not domain_result.allowed:
                    return domain_result
            except Exception as e:
                logger.error("Domain policy check failed for %s: %s", domain, e)
                return PolicyResult(
                    allowed=False,
                    reason=f"Domain policy error: {str(e)}",
                )

        return PolicyResult(allowed=True)

    def _check_security(self, context: Any) -> PolicyResult:
        """Enforce Section 16 security requirements."""
        input_data = getattr(context, "input_data", {})

        # Check URLs in input data
        for key in ("url", "site_url", "site_domain"):
            url_value = input_data.get(key, "")
            if not url_value:
                continue

            # Add scheme if missing for parsing
            parse_url = url_value
            if not parse_url.startswith(("http://", "https://", "file://", "ftp://")):
                parse_url = f"https://{parse_url}"

            parsed = urlparse(parse_url)

            # Reject blocked schemes
            if parsed.scheme in BLOCKED_SCHEMES:
                return PolicyResult(
                    allowed=False,
                    reason=f"Blocked scheme: {parsed.scheme}://",
                )

            # Reject non-HTTPS (allow omitted scheme — defaults to https)
            if parsed.scheme and parsed.scheme != "https":
                # Allow bare domain names without scheme
                if "://" in url_value and parsed.scheme != "https":
                    return PolicyResult(
                        allowed=False,
                        reason=f"Only HTTPS allowed, got: {parsed.scheme}://",
                    )

            # Reject localhost and loopback
            hostname = parsed.hostname or ""
            if hostname in BLOCKED_HOSTS:
                return PolicyResult(
                    allowed=False,
                    reason=f"Blocked host: {hostname}",
                )

            # Reject IP literals
            if IP_PATTERN.match(hostname):
                return PolicyResult(
                    allowed=False,
                    reason=f"IP literal URLs not allowed: {hostname}",
                )

        return PolicyResult(allowed=True)

    def unload_rules(self, domain: str) -> None:
        """Unload domain-specific rules."""
        self._domain_rules.pop(domain, None)
