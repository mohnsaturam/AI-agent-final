"""
Movie Domain — Intent Schema (REVISED)

Defines the structured intent format for movie queries.
SCHEMA_VERSION is used for cache invalidation and idempotency key generation.

Enhanced with rich intent fields:
- primary_goal, secondary_goal: for complex multi-objective queries
- filters: structured filter objects with operators
- ranking_strategy: how results should be ranked
- conditional_constraints: conditions that must be met
"""

SCHEMA_VERSION = "movie_v1"

INTENT_SCHEMA = {
    "type": "object",
    "description": "Structured intent for movie search queries",
    "required": ["query_type"],
    "properties": {
        "query_type": {
            "type": "string",
            "enum": ["search", "details", "list", "comparison", "discovery"],
            "description": "Type of movie query",
        },
        # ═══ NEW: Rich intent fields ═══
        "primary_goal": {
            "type": "string",
            "enum": [
                "find_movies", "get_details", "compare_movies",
                "rank_movies", "discover_movies", "find_recommendations",
            ],
            "description": "Primary objective of the query",
        },
        "secondary_goal": {
            "type": "string",
            "enum": [
                "rank_results", "filter_results", "compare_sources",
                "get_statistics", "find_similar",
            ],
            "description": "Secondary objective (optional)",
        },
        "entity": {
            "type": "string",
            "default": "movie",
            "description": "Entity type being queried",
        },
        "filters": {
            "type": "object",
            "description": "Structured filters with operators",
            "properties": {
                "genre": {"type": "string"},
                "genres": {"type": "array", "items": {"type": "string"}},
                "release_year": {
                    "type": "object",
                    "properties": {
                        "operator": {"type": "string", "enum": ["=", ">", "<", ">=", "<=", "between"]},
                        "value": {"type": "integer"},
                        "value_end": {"type": "integer", "description": "End value for 'between' operator"},
                    },
                },
                "rating": {
                    "type": "object",
                    "properties": {
                        "operator": {"type": "string", "enum": ["=", ">", "<", ">=", "<="]},
                        "value": {"type": "number"},
                    },
                },
                "duration": {
                    "type": "object",
                    "properties": {
                        "operator": {"type": "string", "enum": [">", "<", ">=", "<="]},
                        "value": {"type": "integer"},
                        "unit": {"type": "string", "default": "minutes"},
                    },
                },
                "language": {"type": "string"},
                "director": {"type": "string"},
                "actor": {"type": "string"},
                "country": {"type": "string"},
            },
        },
        "ranking_strategy": {
            "type": "string",
            "enum": [
                "imdb_rating", "rotten_tomatoes_score", "metacritic_score",
                "popularity", "release_date", "box_office", "user_votes",
                "rating",
            ],
            "description": "How results should be ranked/sorted",
        },
        "conditional_constraints": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Conditions that must be met (e.g., 'only include if rating > 8')",
        },
        "requested_fields": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Specific data fields the user is asking about. "
                "Maps to extraction_schema field names. "
                "Examples: 'box_office collection' → ['box_office'], "
                "'cast' → ['cast'], 'rating' → ['rating'], "
                "'who directed' → ['director']. "
                "Empty if user wants all available data."
            ),
        },
        # ═══ Original fields (backward compatible) ═══
        "title": {
            "type": "string",
            "description": "Movie title (exact or partial)",
        },
        "titles": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Multiple movie titles for comparison queries",
        },
        "year": {
            "type": "integer",
            "description": "Release year filter",
        },
        "year_range": {
            "type": "object",
            "properties": {
                "from": {"type": "integer"},
                "to": {"type": "integer"},
            },
            "description": "Release year range filter",
        },
        "language": {
            "type": "string",
            "description": "Primary language filter (e.g., 'tamil', 'english')",
        },
        "genre": {
            "type": "string",
            "description": "Genre filter (e.g., 'action', 'drama')",
        },
        "genres": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Multiple genre filters",
        },
        "director": {
            "type": "string",
            "description": "Director name filter",
        },
        "actor": {
            "type": "string",
            "description": "Actor/cast name filter",
        },
        "sort_by": {
            "type": "string",
            "enum": ["rating", "year", "popularity", "title", "box_office"],
            "description": "Sort criterion",
        },
        "sort_order": {
            "type": "string",
            "enum": ["asc", "desc"],
            "default": "desc",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 500,
            "default": 10,
            "description": "Maximum number of results",
        },
        "min_rating": {
            "type": "number",
            "minimum": 0,
            "maximum": 10,
            "description": "Minimum rating threshold",
        },
        "comparison": {
            "type": "boolean",
            "default": False,
            "description": "Whether this is a cross-site comparison query",
        },
    },
}

# NOTE: INTENT_GUIDANCE has been moved to DB config (umsa_core.domains.config.intent_guidance)
# The DB is the sole source of truth for AI prompt guidance.
# This file only contains INTENT_SCHEMA (the validation contract).

