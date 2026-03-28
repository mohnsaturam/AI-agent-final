"""
Domain Registry — In-Memory Domain Module Cache

Maintains loaded domain modules for fast access.
Validates domain status against the database.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from domains.loader import DomainLoader, DomainLoadError

logger = logging.getLogger("umsa.domains.registry")


class DomainRegistry:
    """
    In-memory registry of loaded domain modules.
    Backed by the umsa_core.domains table for status validation.
    """

    def __init__(self):
        self._domains: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    async def load_all(self, db_pool) -> None:
        """Load all active domains from DB and import their modules."""
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, schema_version, status, allowed_sites, config
                FROM umsa_core.domains
                WHERE status = 'active'
                """
            )

        self._domains = {}
        for row in rows:
            domain_name = row["name"]
            try:
                module = DomainLoader.load(domain_name)
                raw_config = row["config"]
                if isinstance(raw_config, str):
                    try:
                        raw_config = json.loads(raw_config)
                    except json.JSONDecodeError:
                        raw_config = {}
                        
                module["db_config"] = {
                    "schema_version": row["schema_version"],
                    "allowed_sites": row["allowed_sites"],
                    "config": raw_config,
                }
                self._domains[domain_name] = module
                logger.info("Registered domain: %s", domain_name)
            except DomainLoadError as e:
                logger.error("Failed to load domain '%s': %s", domain_name, e)

        self._loaded = True
        logger.info("Domain registry loaded: %d active domains", len(self._domains))

    def get(self, domain_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve a loaded domain module by name."""
        return self._domains.get(domain_name)

    def list_active(self) -> List[str]:
        """List names of all active domains."""
        return list(self._domains.keys())

    def list_all(self) -> List[Dict[str, Any]]:
        """List all domain info dicts."""
        return [
            {
                "name": name,
                "schema_version": module.get("schema_version"),
                "status": "active",
            }
            for name, module in self._domains.items()
        ]

    async def validate_domain(self, domain_name: str, db_pool) -> bool:
        """Validate that a domain exists and is active in the database."""
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT status FROM umsa_core.domains
                WHERE name = $1
                """,
                domain_name,
            )
        if row is None:
            return False
        return row["status"] == "active"

    async def refresh(self, db_pool) -> None:
        """Reload all domains."""
        await self.load_all(db_pool)
