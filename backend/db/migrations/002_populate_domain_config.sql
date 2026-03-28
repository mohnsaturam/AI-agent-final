-- ============================================================
-- Migration 002: Populate umsa_core.domains.config JSONB
-- Date: 2026-03-02
-- Purpose: Move domain-specific knowledge from Python code
--          into the database config column (MCP remediation v3)
--
-- ONLY legitimate domain config — no site-specific hardcoding:
--   - jsonld_type_map: what JSON-LD types map to extraction schema
--   - intent_guidance: how to parse domain-specific queries
--
-- REMOVED (not legitimate config):
--   - css_selectors: discovered dynamically from live DOM
--   - heuristic_keywords: scoring is AI-based with COT
--   - ai_url_patterns: AI figures out URL patterns itself
--   - site_trust_weights: should be derived from success rates
-- ============================================================

UPDATE umsa_core.domains
SET config = '{
  "jsonld_type_map": {
    "target_types": ["Movie", "Film", "CreativeWork"],
    "field_map": {
      "title": ["name"],
      "year": ["datePublished", "dateCreated"],
      "rating": ["aggregateRating.ratingValue"],
      "director": ["director.name", "director"],
      "genre": ["genre"],
      "synopsis": ["description"],
      "poster_url": ["image", "thumbnailUrl"],
      "runtime_minutes": ["duration"]
    }
  },
  "intent_guidance": "You are an intent understanding agent for movie queries.\nParse the user''s natural language query into a structured intent.\n\nRules:\n- Default query_type is ''search'' unless the query clearly asks for details of a specific movie\n- Use ''discovery'' for broad exploration queries (top/best/trending)\n- Extract year from phrases like 2024 movies, movies from 2023\n- Recognize language names: Tamil, Telugu, Hindi, English, Korean, Japanese, etc.\n- Recognize common genre terms: action, drama, comedy, thriller, horror, sci-fi, romance\n- Default limit to 10 if not specified\n- Default sort_by to ''rating'' for list queries, ''popularity'' for search queries\n\nRich intent fields:\n- Set primary_goal to describe the main objective (find_movies, get_details, compare_movies, etc.)\n- Set secondary_goal if the query has a secondary objective (rank_results, filter_results, etc.)\n- Use filters object for structured filters with operators when the query has specific conditions\n  e.g., rating > 8 -> filters.rating = {\"operator\": \">\", \"value\": 8}\n  e.g., released after 2022 -> filters.release_year = {\"operator\": \">\", \"value\": 2022}\n- Set ranking_strategy when user specifies how to rank (ranked by IMDB -> imdb_rating)\n- Set conditional_constraints for conditions (only if rating > 8)\n\nExamples:\n- top 5 tamil movies 2024 -> query_type=list, primary_goal=find_movies, language=tamil, year=2024, limit=5, sort_by=rating\n- inception movie details -> query_type=details, primary_goal=get_details, title=inception\n- best action movies -> query_type=search, primary_goal=find_movies, genre=action, sort_by=rating"
}'::jsonb,
    updated_at = now()
WHERE name = 'movie';
