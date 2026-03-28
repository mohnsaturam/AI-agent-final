"""
Domain Loader — Dynamic Domain Module Import

Dynamically loads domain modules by name.
Returns a standardized dict with all domain components.
Fails fast if a module is missing or malformed.
"""

import importlib
import logging
from typing import Any, Dict

logger = logging.getLogger("umsa.domains.loader")

# Required exports from every domain module
REQUIRED_MODULES = [
    "intent_schema",
    "extraction_schema",
    "policy_rules",
]

# Optional modules (loaded if present, graceful fallback if not)
OPTIONAL_MODULES = [
    "url_patterns",
]

REQUIRED_ATTRIBUTES = {
    "intent_schema": ["SCHEMA_VERSION", "INTENT_SCHEMA"],
    "extraction_schema": ["SCHEMA_VERSION", "EXTRACTION_SCHEMA"],
    "policy_rules": ["check"],
}


class DomainLoadError(Exception):
    """Raised when a domain module cannot be loaded or is malformed."""
    pass


class DomainLoader:
    """Dynamically imports and validates domain modules."""

    @staticmethod
    def load(domain_name: str) -> Dict[str, Any]:
        """
        Load a domain module by name.

        Returns:
            {
                "name": str,
                "schema_version": str,
                "intent_schema": dict,
                "extraction_schema": dict,
                "policy_rules": module (with .check() callable),
                "url_patterns": module,
                "intent_guidance": str (optional),
            }

        Raises:
            DomainLoadError: if module is missing or required attributes are absent.
        """
        base_package = f"domains.{domain_name}"
        loaded = {}

        for module_name in REQUIRED_MODULES:
            full_path = f"{base_package}.{module_name}"
            try:
                mod = importlib.import_module(full_path)
            except ImportError as e:
                raise DomainLoadError(
                    f"Cannot import domain module '{full_path}': {e}"
                )

            # Validate required attributes
            required_attrs = REQUIRED_ATTRIBUTES.get(module_name, [])
            for attr in required_attrs:
                if not hasattr(mod, attr):
                    raise DomainLoadError(
                        f"Module '{full_path}' missing required attribute '{attr}'"
                    )

            loaded[module_name] = mod

        # Load optional modules (graceful fallback)
        for module_name in OPTIONAL_MODULES:
            full_path = f"{base_package}.{module_name}"
            try:
                mod = importlib.import_module(full_path)
                loaded[module_name] = mod
            except ImportError:
                logger.info("Optional module '%s' not found — skipping", full_path)

        # Extract standardized components
        intent_mod = loaded["intent_schema"]
        extraction_mod = loaded["extraction_schema"]

        result = {
            "name": domain_name,
            "schema_version": intent_mod.SCHEMA_VERSION,
            "intent_schema": intent_mod.INTENT_SCHEMA,
            "extraction_schema": extraction_mod.EXTRACTION_SCHEMA,
            "policy_rules": loaded["policy_rules"],
        }

        # Optional: url_patterns (stub or real)
        if "url_patterns" in loaded:
            result["url_patterns"] = loaded["url_patterns"]

        logger.info(
            "Loaded domain '%s' (schema_version=%s)",
            domain_name,
            result["schema_version"],
        )
        return result

    @staticmethod
    def validate_schema_version(domain_module: Dict[str, Any], expected: str) -> bool:
        """Check that the loaded module's schema version matches expected."""
        return domain_module.get("schema_version") == expected
