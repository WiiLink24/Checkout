import config
import random
from utils.utils import _build_serial_filter, _run_query


def find_game_recommendation(serial_prefixes):
    if not serial_prefixes:
        return None

    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)

    # Get user's played genres
    profile_query = f"""
        SELECT t.genre, t.developer
        FROM recommendations r
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE t.game_id = r.game_id OR SUBSTRING(t.game_id, 1, 4) = SUBSTRING(r.game_id, 1, 4)
            ORDER BY LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        WHERE {where_clause} AND r.recommendation_percent > 50
        LIMIT 100
    """
    profile_rows = _run_query(profile_query, params, config.db_url)

    if not profile_rows:
        return None

    # Count genre and developer preferences
    genre_count = {}
    developer_count = {}

    for row in profile_rows:
        genre = (row.get("genre") or "").strip()
        if genre:
            for g in genre.split(","):
                g = g.strip()
                if g:
                    genre_count[g] = genre_count.get(g, 0) + 1

        dev = (row.get("developer") or "").strip()
        if dev:
            developer_count[dev] = developer_count.get(dev, 0) + 1

    # Get already played games
    played_query = f"""
        SELECT game_id
        FROM time_played
        WHERE {where_clause}
    """
    played_rows = _run_query(played_query, params, config.db_url)
    played_ids = {row.get("game_id") for row in played_rows if row.get("game_id")}

    # Fetch candidate games and score them
    candidates_query = """
        WITH preferred AS (
            SELECT SUBSTRING(game_id, 1, 4) AS prefix, MIN(game_id) AS disc_id
            FROM titles
            WHERE LENGTH(game_id) = 6
            GROUP BY SUBSTRING(game_id, 1, 4)
        )
        SELECT t.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer,
               t.game_type, t.release_year, t.rating_type, t.rating_value, t.region,
               COALESCE(AVG(r.recommendation_percent), 50) AS avg_rating, COUNT(r.game_id) AS rating_count
        FROM titles t
        LEFT JOIN recommendations r
            ON (LENGTH(r.game_id) = 4
                AND t.game_id = COALESCE(
                    (SELECT disc_id FROM preferred WHERE prefix = r.game_id),
                    r.game_id
                ))
            OR (LENGTH(r.game_id) > 4 AND r.game_id = t.game_id)
        WHERE t.display_name IS NOT NULL AND t.genre IS NOT NULL
        GROUP BY t.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer,
                 t.game_type, t.release_year, t.rating_type, t.rating_value, t.region
        ORDER BY rating_count DESC LIMIT 100
    """
    candidates = _run_query(candidates_query, [], config.db_url)

    best_score = -1
    best_game = None
    top_genre = None

    for candidate in candidates:
        game_id = candidate.get("game_id")
        if not game_id or game_id in played_ids:
            continue

        # Skip if game_id matches any played game_id
        if any(
            game_id.startswith(pid) or pid.startswith(game_id) for pid in played_ids
        ):
            continue

        # How many times this game's genre appears in user games
        genres = [g.strip() for g in (candidate.get("genre") or "").split(",")]
        genre_match = sum(genre_count.get(g, 0) for g in genres)

        # How many times this game's developer appears in user games
        dev = (candidate.get("developer") or "").strip()
        dev_match = developer_count.get(dev, 0)

        # Normalize rating
        rating = float(candidate.get("avg_rating") or 50)
        rating_score = (rating - 50) / 50

        # Add randomness
        score = (
            (genre_match * 0.6)
            + (dev_match * 0.2)
            + (rating_score * 0.2)
            + random.uniform(0, 0.1)
        )

        # If better than current best, update
        if score > best_score:
            best_score = score
            best_game = candidate
            top_genre = (
                max(genres, key=lambda g: genre_count.get(g, 0))
                if genres
                else "Unknown"
            )

    if best_game:
        best_game["reason"] = {
            "genres": [
                g
                for g in (best_game.get("genre") or "").split(",")
                if genre_count.get(g.strip(), 0) > 0
            ][:3],
            "matched_genre": top_genre,
            "score": round(best_score, 2),
        }
        return best_game

    return None
