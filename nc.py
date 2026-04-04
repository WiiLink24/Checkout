import config
from utils import _build_serial_filter, _run_query

# Validation functions


def serial_has_bookmarks(serial_prefixes):
    """Check if a serial has any bookmarks in the database."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return False

    query = f"SELECT 1 FROM bookmarks WHERE {where_clause} LIMIT 1"
    result = _run_query(query, params, config.db_url)
    return bool(result)


def serial_has_recommendations(serial_prefixes):
    """Check if a serial has any recommendations in the database."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return False

    query = f"SELECT 1 FROM recommendations WHERE {where_clause} LIMIT 1"
    result = _run_query(query, params, config.db_url)
    return bool(result)


def serial_has_time_played(serial_prefixes):
    """Check if a serial has any time played entries in the database."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return False

    query = f"SELECT 1 FROM time_played WHERE {where_clause} LIMIT 1"
    result = _run_query(query, params, config.db_url)
    return bool(result)


# Count functions for pagination


def count_bookmarks(serial_prefixes):
    """Count total bookmarked games for given serial prefixes."""
    where_clause, params = _build_serial_filter("b.serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = f"SELECT COUNT(DISTINCT b.game_id) AS count FROM bookmarks b WHERE {where_clause}"
    result = _run_query(query, params, config.db_url)
    return result[0].get("count", 0) if result else 0


def count_recommendations(serial_prefixes):
    """Count total recommendations for given serial prefixes."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = f"SELECT COUNT(DISTINCT game_id) AS count FROM recommendations WHERE {where_clause}"
    result = _run_query(query, params, config.db_url)
    return result[0].get("count", 0) if result else 0


def count_time_played(serial_prefixes):
    """Count total time played entries for given serial prefixes."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = (
        f"SELECT COUNT(DISTINCT game_id) AS count FROM time_played WHERE {where_clause}"
    )
    result = _run_query(query, params, config.db_url)
    return result[0].get("count", 0) if result else 0


# Bookmarks


def fetch_favorites(serial_prefixes, limit=30, offset=0):
    """Fetch user's bookmarked favorite games from the bookmarks table."""
    where_clause, params = _build_serial_filter("b.serial_number", serial_prefixes)
    if not where_clause:
        return []

    query = (
        "WITH latest_bookmarks AS ("
        "    SELECT DISTINCT ON (b.game_id) "
        "    b.id, b.serial_number, b.game_id "
        "    FROM bookmarks b "
        f"    WHERE {where_clause} "
        "    ORDER BY b.game_id, b.id DESC"
        ") "
        "SELECT "
        "lb.id AS bookmark_id, lb.serial_number, lb.game_id AS bookmarked_game_id, "
        "COALESCE(stats.favorite_count, 0) AS favorite_count, "
        "COALESCE(stats.user_count, 0) AS user_count, "
        "t.* "
        "FROM latest_bookmarks lb "
        "LEFT JOIN LATERAL ("
        "    SELECT "
        "    COUNT(*) AS favorite_count, "
        "    COUNT(DISTINCT b2.serial_number) AS user_count "
        "    FROM bookmarks b2 "
        "    WHERE b2.game_id = lb.game_id OR b2.game_id LIKE lb.game_id || '%%'"
        ") stats ON true "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id = lb.game_id OR t.game_id LIKE lb.game_id || '%%' "
        "    ORDER BY CASE WHEN t.game_id = lb.game_id THEN 0 ELSE 1 END, t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "ORDER BY lb.id DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    rows = _run_query(query, params, config.db_url)

    favorites = []
    for row in rows:
        game_id = row.get("bookmarked_game_id") or row.get("game_id")
        title_value = (
            row.get("display_name")
            or row.get("title")
            or row.get("title_en")
            or game_id
        )
        synopsis_value = row.get("synopsis") or row.get("synopsis_en")

        normalized = dict(row)
        normalized["game_id"] = game_id
        normalized["title"] = title_value
        normalized["title_en"] = row.get("title_en") or row.get("title")
        normalized["synopsis_en"] = row.get("synopsis_en") or synopsis_value
        favorites.append(normalized)

    return favorites


def fetch_top_favorites(limit=30):
    """Fetch top games by total bookmark count across all users."""
    query = (
        "SELECT "
        "b.game_id, "
        "COALESCE(t.display_name, t.title_en, b.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region, "
        "COUNT(*) AS favorite_count, "
        "COUNT(DISTINCT b.serial_number) AS user_count "
        "FROM bookmarks b "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE b.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "GROUP BY b.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, t.release_year, t.rating_type, t.rating_value, t.region "
        "ORDER BY favorite_count DESC, user_count DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, [], config.db_url)


# Recommendations


def fetch_recommendations(
    serial_prefixes, sort_by="recommendation_percent", limit=30, offset=0
):
    """Fetch recommendations for a given serial number"""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return []

    if sort_by == "last_recommended":
        order_by = "r.id DESC, t.title_en NULLS LAST, r.game_id"
    else:
        order_by = "r.recommendation_percent DESC, t.title_en NULLS LAST, r.game_id"

    query = (
        "SELECT "
        "r.id, r.serial_number, r.game_id, r.gender, r.age, "
        "r.recommendation_percent, r.appeal, r.gaming_mood, r.friend_or_alone, "
        "COALESCE(t.display_name, t.title_en, r.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.region, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value "
        "FROM recommendations r "
        "JOIN ("
        "    SELECT game_id, MAX(id) AS latest_id "
        "    FROM recommendations "
        f"    WHERE {where_clause} "
        "    GROUP BY game_id"
        ") latest ON latest.latest_id = r.id "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"ORDER BY {order_by} "
        f"LIMIT {limit} OFFSET {offset}"
    )
    return _run_query(query, params, config.db_url)


def fetch_recommendation_averages(game_id, gender=None, age_min=None, age_max=None):
    """Fetch average recommendation stats for a game, optionally filtered by demographics"""
    conditions = ["game_id = %s"]
    params = [game_id]
    if gender in (1, 2):
        conditions.append("gender = %s")
        params.append(gender)
    if isinstance(age_min, int):
        conditions.append("age >= %s")
        params.append(age_min)
    if isinstance(age_max, int):
        conditions.append("age <= %s")
        params.append(age_max)
    where_clause = " AND ".join(conditions)
    query = (
        "SELECT "
        "COUNT(*) AS total, "
        "AVG(recommendation_percent) AS avg_score, "
        "AVG(appeal) AS avg_appeal, "
        "AVG(gaming_mood) AS avg_mood, "
        "AVG(friend_or_alone) AS avg_friend "
        "FROM recommendations "
        f"WHERE {where_clause}"
    )
    rows = _run_query(query, params, config.db_url)
    return rows[0] if rows else None


def fetch_top_best_games(limit=30):
    """Fetch top games with confidence-weighted ranking by score and reviewer count."""
    query = (
        "WITH global_stats AS ("
        "    SELECT AVG(recommendation_percent)::numeric AS global_avg "
        "    FROM recommendations"
        "), per_game AS ("
        "    SELECT "
        "    r.game_id, "
        "    ROUND(AVG(r.recommendation_percent)::numeric, 2) AS avg_recommendation, "
        "    COUNT(DISTINCT r.serial_number) AS reviewer_count "
        "    FROM recommendations r "
        "    GROUP BY r.game_id"
        ") "
        "SELECT "
        "pg.game_id, "
        "COALESCE(t.display_name, t.title_en, pg.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region, "
        "pg.avg_recommendation, "
        "pg.reviewer_count "
        "FROM per_game pg "
        "CROSS JOIN global_stats gs "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE pg.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "ORDER BY "
        "((pg.reviewer_count::numeric / (pg.reviewer_count + 20)::numeric) * pg.avg_recommendation) + "
        "((20::numeric / (pg.reviewer_count + 20)::numeric) * gs.global_avg) DESC, "
        "pg.reviewer_count DESC, "
        "pg.avg_recommendation DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, [], config.db_url)


# Time Played


def fetch_time_played(serial_prefixes, sort_by="time_played", limit=30, offset=0):
    """Fetch time played data for a given serial number"""
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []

    if sort_by == "times_played":
        sort_expr = "lp.times_played DESC, lp.time_played DESC, lp.id DESC"
    elif sort_by == "last_played":
        sort_expr = "lp.id DESC"
    else:
        sort_expr = "lp.time_played DESC, lp.times_played DESC, lp.id DESC"

    query = (
        "WITH filtered AS ("
        "    SELECT tp.* "
        "    FROM time_played tp "
        f"    WHERE {where_clause} "
        "), latest_per_game AS ("
        "    SELECT DISTINCT ON (f.game_id) "
        "    f.id, f.serial_number, f.game_id, f.times_played, f.time_played "
        "    FROM filtered f "
        "    ORDER BY f.game_id, f.id DESC"
        "), ranked AS ("
        "    SELECT "
        "    lp.id, lp.serial_number, lp.game_id, lp.times_played, lp.time_played, "
        "    ROW_NUMBER() OVER (ORDER BY " + sort_expr + ") AS sort_rank "
        "    FROM latest_per_game lp"
        "), detailed_games AS ("
        "    SELECT "
        "    r.id, r.serial_number, r.game_id, r.times_played, r.time_played, "
        "    COALESCE(t.display_name, t.title_en, r.game_id) AS title, "
        "    t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "    t.release_year, t.rating_type, t.rating_value, t.region, "
        "    r.sort_rank "
        "    FROM ranked r "
        "    LEFT JOIN LATERAL ("
        "        SELECT * FROM titles t "
        "        WHERE t.game_id LIKE r.game_id || '%%' "
        "        ORDER BY t.game_id "
        "        LIMIT 1"
        "    ) t ON true"
        ") "
        "SELECT * FROM detailed_games "
        "ORDER BY sort_rank "
        f"LIMIT {limit} OFFSET {offset}"
    )
    return _run_query(query, params, config.db_url)


def fetch_time_played_stats(game_id):
    """Fetch time played stats for a given game"""
    query = (
        "SELECT "
        "COUNT(DISTINCT serial_number) AS total_players, "
        "SUM(time_played) AS total_minutes, "
        "ROUND(AVG(time_played)::numeric, 2) AS avg_minutes_per_player "
        "FROM time_played "
        "WHERE game_id = %s"
    )
    rows = _run_query(query, [game_id], config.db_url)
    return rows[0] if rows else None


def fetch_top_most_played(limit=30):
    """Fetch top games by total time played across all users"""
    query = (
        "SELECT "
        "tp.game_id, "
        "COALESCE(t.display_name, t.title_en, tp.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region, "
        "SUM(tp.time_played) AS total_time_played, "
        "COUNT(DISTINCT tp.serial_number) AS player_count, "
        "ROUND(AVG(tp.time_played)::numeric, 2) AS avg_time_per_player "
        "FROM time_played tp "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE tp.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "GROUP BY tp.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, t.release_year, t.rating_type, t.rating_value, t.region "
        "ORDER BY total_time_played DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, [], config.db_url)


# User Latest Activity


def fetch_user_latest_games(serial_prefixes, limit=5):
    """Fetch user's most recently played games."""
    games = fetch_time_played(serial_prefixes, sort_by="last_played")
    return games[:limit]


def fetch_user_latest_reviews(serial_prefixes, limit=5):
    """Fetch user's most recent game recommendations/reviews."""
    reviews = fetch_recommendations(serial_prefixes, sort_by="last_recommended")
    return reviews[:limit]


def fetch_user_stats(serial_prefixes):
    """Fetch user's aggregate statistics (total playtime and review count)."""
    if not serial_prefixes:
        return {"total_minutes": 0, "total_reviews": 0}

    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)

    # Total playtime
    playtime_query = (
        f"SELECT COALESCE(SUM(tp.time_played), 0) AS total_minutes "
        f"FROM time_played tp "
        f"WHERE {where_clause}"
    )
    playtime_result = _run_query(playtime_query, params, config.db_url)
    total_minutes = playtime_result[0]["total_minutes"] if playtime_result else 0

    # Total reviews/recommendations
    reviews_where_clause, reviews_params = _build_serial_filter(
        "r.serial_number", serial_prefixes
    )
    reviews_query = (
        f"SELECT COUNT(*) AS total_reviews "
        f"FROM recommendations r "
        f"WHERE {reviews_where_clause}"
    )
    reviews_result = _run_query(reviews_query, reviews_params, config.db_url)
    total_reviews = reviews_result[0]["total_reviews"] if reviews_result else 0

    return {"total_minutes": total_minutes, "total_reviews": total_reviews}
