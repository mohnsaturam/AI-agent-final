"""
MCP Server — Tool Registry Manager (Section 8)

Loads tool definitions from umsa_core.tool_registry.
Validates caller, domain scope, and input schema at runtime.
Zero domain-specific imports.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import jsonschema

logger = logging.getLogger("umsa.tool_registry")


class ToolRegistryManager:
    """
    In-memory cache of tool definitions loaded from PostgreSQL.
    Provides validation methods used by the gateway enforcement sequence.
    """

    def __init__(self):
        self._tools: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    async def load(self, db_pool) -> None:
        """Load all tool definitions from umsa_core.tool_registry."""
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM umsa_core.tool_registry")

        self._tools = {}
        for row in rows:
            tool = dict(row)
            # Parse input_schema from JSON if needed
            if isinstance(tool.get("input_schema"), str):
                tool["input_schema"] = json.loads(tool["input_schema"])
            self._tools[tool["tool_name"]] = tool

        self._loaded = True
        logger.info("Loaded %d tools from registry", len(self._tools))

    async def get_tool(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve a tool definition by name. Returns None if not found."""
        if not self._loaded:
            logger.warning("Tool registry not loaded yet")
            return None
        return self._tools.get(tool_name)

    def validate_caller(self, tool_def: Dict[str, Any], caller: str) -> bool:
        """
        Step 2: Validate that the caller is in allowed_callers.
        """
        allowed = tool_def.get("allowed_callers", [])
        if "*" in allowed:
            return True
        return caller in allowed

    def validate_domain(self, tool_def: Dict[str, Any], domain: str) -> bool:
        """
        Step 3: Validate that the domain is in domain_scope.
        """
        scope = tool_def.get("domain_scope", [])
        if "*" in scope:
            return True
        return domain in scope

    def validate_input(
        self, tool_def: Dict[str, Any], input_data: dict
    ) -> Optional[str]:
        """
        Step 4: Validate input_data against the tool's declared input_schema.
        Returns None on success, error message on failure.
        """
        schema = tool_def.get("input_schema", {})
        if not schema:
            return None  # No schema → no validation

        try:
            jsonschema.validate(instance=input_data, schema=schema)
            return None
        except jsonschema.ValidationError as e:
            return f"{e.path}: {e.message}" if e.path else e.message
        except jsonschema.SchemaError as e:
            logger.error("Invalid tool schema for %s: %s", tool_def["tool_name"], e)
            return f"Internal schema error: {e.message}"

    def list_tools(self) -> List[Dict[str, Any]]:
        """List all registered tools."""
        return list(self._tools.values())

    async def refresh(self, db_pool) -> None:
        """Reload tools from the database."""
        await self.load(db_pool)
