"""
Movie Domain — Extraction Schema

Defines the target data structure for movie extraction.
All extraction results MUST conform to this schema.
Tier-2 unification output is validated against this schema.

Field tiers:
  - core: Present on ~95% of movie detail pages (title, year, rating, etc.)
  - common: Present on ~60-70% of pages (cast, genres, runtime, etc.)
  - rare: Present on ~10-30% of pages (budget, box_office, trailer, etc.)
  - meta: System-injected fields, never from extraction (source_url, source_site)
"""

SCHEMA_VERSION = "movie_v1"

EXTRACTION_SCHEMA = {
    "type": "object",
    "description": "Unified movie data extraction schema",
    "required": ["title"],
    "properties": {
        "title": {
            "type": "string",
            "description": "Official movie title",
            "tier": "core",
        },
        "original_title": {
            "type": "string",
            "description": "Original language title if different from English",
            "tier": "rare",
        },
        "year": {
            "type": "integer",
            "description": "Release year",
            "tier": "core",
        },
        "release_date": {
            "type": "string",
            "description": "Full release date (YYYY-MM-DD format)",
            "tier": "common",
        },
        "director": {
            "type": "string",
            "description": "Director name",
            "tier": "core",
        },
        "directors": {
            "type": "array",
            "items": {"type": "string"},
            "description": "All directors (for multi-director films)",
            "tier": "rare",
        },
        "cast": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Main cast members",
            "maxItems": 20,
            "tier": "common",
        },
        "genres": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Genre classifications",
            "tier": "common",
        },
        "rating": {
            "type": "number",
            "minimum": 0,
            "maximum": 10,
            "description": "Aggregated rating (normalized to 0–10 scale)",
            "tier": "core",
        },
        "rating_source": {
            "type": "string",
            "description": "Source of the rating (e.g., 'imdb', 'rottentomatoes')",
            "tier": "rare",
        },
        "rating_count": {
            "type": "integer",
            "description": "Number of ratings/votes",
            "tier": "rare",
        },
        "critic_score": {
            "type": "integer",
            "minimum": 0,
            "maximum": 100,
            "description": "Critic score (percentage or Metascore)",
            "tier": "rare",
        },
        "audience_score": {
            "type": "integer",
            "minimum": 0,
            "maximum": 100,
            "description": "Audience score (percentage)",
            "tier": "rare",
        },
        "runtime_minutes": {
            "type": "integer",
            "description": "Runtime in minutes",
            "tier": "common",
        },
        "synopsis": {
            "type": "string",
            "description": "Plot synopsis / description",
            "tier": "core",
        },
        "language": {
            "type": "string",
            "description": "Primary language",
            "tier": "common",
        },
        "languages": {
            "type": "array",
            "items": {"type": "string"},
            "description": "All spoken languages",
            "tier": "rare",
        },
        "country": {
            "type": "string",
            "description": "Primary country of origin",
            "tier": "rare",
        },
        "certification": {
            "type": "string",
            "description": "Content rating (e.g., PG-13, R, U/A)",
            "tier": "common",
        },
        "poster_url": {
            "type": "string",
            "format": "uri",
            "description": "URL to movie poster image",
            "tier": "core",
        },
        "trailer_url": {
            "type": "string",
            "format": "uri",
            "description": "URL to official trailer",
            "tier": "rare",
        },
        "box_office": {
            "type": "string",
            "description": "Box office gross (formatted string)",
            "tier": "rare",
        },
        "budget": {
            "type": "string",
            "description": "Production budget (formatted string)",
            "tier": "rare",
        },
        "production_companies": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Production companies",
            "tier": "rare",
        },
        "source_url": {
            "type": "string",
            "format": "uri",
            "description": "URL of the source page",
            "tier": "meta",
        },
        "source_site": {
            "type": "string",
            "description": "Domain of the source site",
            "tier": "meta",
        },
    },
}


def get_fields_by_tier(tier: str) -> list:
    """Return field names for a given tier."""
    return [
        name for name, props in EXTRACTION_SCHEMA["properties"].items()
        if props.get("tier") == tier
    ]


def get_core_field_count() -> int:
    """Return the number of core-tier fields (minimum expected denominator)."""
    return len(get_fields_by_tier("core"))


def get_extractable_fields() -> list:
    """Return all field names that can come from extraction (core + common + rare)."""
    return [
        name for name, props in EXTRACTION_SCHEMA["properties"].items()
        if props.get("tier") in ("core", "common", "rare")
    ]
