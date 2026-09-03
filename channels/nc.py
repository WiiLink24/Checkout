import config
from datetime import date
from utils.utils import (
    _build_serial_filter,
    _run_query,
    resolve_serial,
    cache,
)

# Validation functions


def serial_has_time_played(serial_prefixes):
    """Check if a serial has any time played entries in the database."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return False

    query = f"""
        SELECT 1
        FROM time_played
        WHERE {where_clause}
        LIMIT 1
    """
    result = _run_query(query, params, config.db_url)
    return bool(result)


# Count functions for pagination


def count_bookmarks(serial_prefixes, use_cache=True):
    """Count total bookmarked games for given serial prefixes."""
    where_clause, params = _build_serial_filter("b.serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = f"""
        SELECT COUNT(DISTINCT b.game_id) AS count
        FROM bookmarks b
        WHERE {where_clause}
    """
    result = _run_query(query, params, config.db_url, use_cache=use_cache)
    return result[0].get("count", 0) if result else 0


def count_recommendations(serial_prefixes, use_cache=True):
    """Count total recommendations for given serial prefixes."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = f"""
        SELECT COUNT(DISTINCT game_id) AS count
        FROM recommendations
        WHERE {where_clause}
    """
    result = _run_query(query, params, config.db_url, use_cache=use_cache)
    return result[0].get("count", 0) if result else 0


def count_time_played(serial_prefixes, use_cache=True):
    """Count total time played entries for given serial prefixes."""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return 0

    query = f"""
        SELECT COUNT(DISTINCT game_id) AS count
        FROM time_played
        WHERE {where_clause}
    """
    result = _run_query(query, params, config.db_url, use_cache=use_cache)
    return result[0].get("count", 0) if result else 0


# Bookmarks


def fetch_favorites(serial_prefixes, limit=30, offset=0, serial_to_wii=None):
    """Fetch user's bookmarked favorite games from the bookmarks table."""
    where_clause, params = _build_serial_filter("b.serial_number", serial_prefixes)
    if not where_clause:
        return []

    query = f"""
        WITH latest_bookmarks AS (
            SELECT DISTINCT ON (b.game_id)
                b.id, b.serial_number, b.game_id
            FROM bookmarks b
            WHERE {where_clause}
            ORDER BY b.game_id, b.id DESC
        )
        SELECT
            lb.id AS bookmark_id,
            lb.serial_number,
            lb.game_id AS bookmarked_game_id,
            COALESCE(stats.favorite_count, 0) AS favorite_count,
            COALESCE(stats.user_count, 0) AS user_count,
            t.*, t.input_players
        FROM latest_bookmarks lb
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) AS favorite_count,
                COUNT(DISTINCT b2.serial_number) AS user_count
            FROM bookmarks b2
            WHERE SUBSTRING(b2.game_id, 1, 3) = SUBSTRING(lb.game_id, 1, 3)
        ) stats ON true
        LEFT JOIN LATERAL (
            SELECT *
            FROM titles t
            WHERE SUBSTRING(t.game_id, 1, 4) = SUBSTRING(lb.game_id, 1, 4)
            ORDER BY LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY lb.id DESC
        LIMIT {limit} OFFSET {offset}
    """
    rows = _run_query(query, params, config.db_url)

    favorites = []
    for row in rows:
        game_id = row.get("game_id")
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
        normalized["title_en"] = row.get("title_en")
        normalized["synopsis_en"] = row.get("synopsis_en")
        normalized["wii_number"] = resolve_serial(
            row.get("serial_number"), serial_to_wii
        )
        favorites.append(normalized)

    return favorites


def fetch_top_favorites(limit=30):
    """Fetch top games by total bookmark count across all users."""
    query = f"""
        WITH bookmark_groups AS (
            SELECT
                LEFT(b.game_id, 3) AS title_prefix,
                COUNT(*) AS favorite_count,
                COUNT(DISTINCT b.serial_number) AS user_count
            FROM bookmarks b
            GROUP BY LEFT(b.game_id, 3)
        )
        SELECT
            COALESCE(t.game_id, bg.title_prefix) AS game_id,
            COALESCE(t.display_name, t.title_en, bg.title_prefix) AS title,
            t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type,
            t.release_year, t.rating_type, t.rating_value, t.region, t.input_controls, t.wifi_players, t.input_players,
            bg.favorite_count,
            bg.user_count
        FROM bookmark_groups bg
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE LEFT(t.game_id, 3) = bg.title_prefix
            ORDER BY CASE
                WHEN SUBSTRING(t.game_id, 4, 1) = 'E' THEN 1
                WHEN SUBSTRING(t.game_id, 4, 1) = 'P' THEN 2
                WHEN SUBSTRING(t.game_id, 4, 1) = 'J' THEN 3
                WHEN SUBSTRING(t.game_id, 4, 1) = 'K' THEN 4
                WHEN SUBSTRING(t.game_id, 4, 1) = 'C' THEN 5
            END, LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY favorite_count DESC, user_count DESC
        LIMIT {limit}
    """
    return _run_query(query, [], config.db_url)


# Recommendations


def fetch_recommendations(
    serial_prefixes,
    sort_by="recommendation_percent",
    limit=30,
    offset=0,
    serial_to_wii=None,
):
    """Fetch recommendations for a given serial number"""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return []

    if sort_by == "last_recommended":
        order_by = "r.id DESC, t.title_en NULLS LAST, r.game_id"
    else:
        order_by = "r.recommendation_percent DESC, t.title_en NULLS LAST, r.game_id"

    query = f"""
        SELECT
            r.id, r.serial_number, r.gender, r.age,
            r.recommendation_percent, r.appeal, r.gaming_mood, r.friend_or_alone,
            COALESCE(t.game_id, r.game_id) AS game_id,
            COALESCE(t.display_name, t.title_en, r.game_id) AS title,
            t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.region, t.game_type,
            t.release_year, t.rating_type, t.rating_value, t.input_controls, t.wifi_players, t.input_players,
            (SELECT COUNT(*) FROM bookmarks bf WHERE bf.game_id = r.game_id) AS favorite_count
        FROM recommendations r
        JOIN (
            SELECT game_id, MAX(id) AS latest_id
            FROM recommendations
            WHERE {where_clause}
            GROUP BY game_id
        ) latest ON latest.latest_id = r.id
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE t.game_id = r.game_id OR SUBSTRING(t.game_id, 1, 4) = SUBSTRING(r.game_id, 1, 4)
            ORDER BY LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY {order_by}
        LIMIT {limit} OFFSET {offset}
    """
    results = _run_query(query, params, config.db_url)

    for row in results:
        row["wii_number"] = resolve_serial(row.get("serial_number"), serial_to_wii)
    return results


def fetch_recommendation_averages(game_id, gender=None, age_min=None, age_max=None):
    """Fetch aggregated recommendation and favorite stats for a title family."""
    conditions = ["LEFT(game_id, 3) = %s"]
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
    query = f"""
        SELECT
            COUNT(*) AS total,
            AVG(recommendation_percent) AS avg_score,
            AVG(appeal) AS avg_appeal,
            AVG(gaming_mood) AS avg_mood,
            AVG(friend_or_alone) AS avg_friend,
            (SELECT COUNT(*) FROM bookmarks WHERE LEFT(game_id, 3) = %s) AS favorite_count
        FROM recommendations
        WHERE {where_clause}
    """
    rows = _run_query(query, [game_id] + params, config.db_url)
    return rows[0] if rows else None


def fetch_top_best_games(limit=30):
    """Fetch top games with confidence-weighted ranking by score and reviewer count."""
    query = f"""
        WITH global_stats AS (
            SELECT AVG(recommendation_percent)::numeric AS global_avg
            FROM recommendations
        ), per_game AS (
            SELECT
                LEFT(r.game_id, 3) AS title_prefix,
                ROUND(AVG(r.recommendation_percent)::numeric, 2) AS avg_recommendation,
                COUNT(DISTINCT r.serial_number) AS reviewer_count
            FROM recommendations r
            GROUP BY LEFT(r.game_id, 3)
        )
        SELECT
            COALESCE(t.game_id, pg.title_prefix) AS game_id,
            COALESCE(t.display_name, t.title_en, pg.title_prefix) AS title,
            t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type,
            t.release_year, t.rating_type, t.rating_value, t.region, t.input_controls, t.wifi_players, t.input_players,
            pg.avg_recommendation,
            pg.reviewer_count,
            (SELECT COUNT(*) FROM bookmarks bf WHERE LEFT(bf.game_id, 3) = pg.title_prefix) AS favorite_count
        FROM per_game pg
        CROSS JOIN global_stats gs
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE LEFT(t.game_id, 3) = pg.title_prefix
            ORDER BY CASE
                WHEN SUBSTRING(t.game_id, 4, 1) = 'E' THEN 1
                WHEN SUBSTRING(t.game_id, 4, 1) = 'P' THEN 2
                WHEN SUBSTRING(t.game_id, 4, 1) = 'J' THEN 3
                WHEN SUBSTRING(t.game_id, 4, 1) = 'K' THEN 4
                WHEN SUBSTRING(t.game_id, 4, 1) = 'C' THEN 5
            END, LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY
            ((pg.reviewer_count::numeric / (pg.reviewer_count + 20)::numeric) * pg.avg_recommendation) +
            ((20::numeric / (pg.reviewer_count + 20)::numeric) * gs.global_avg) DESC,
            pg.reviewer_count DESC,
            pg.avg_recommendation DESC
        LIMIT {limit}
    """
    return _run_query(query, [], config.db_url)


# Time Played


def fetch_time_played(
    serial_prefixes, sort_by="time_played", limit=30, offset=0, serial_to_wii=None
):
    """Fetch time played data for a given serial number"""
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []

    if sort_by == "times_played":
        sort_expr = "spg.times_played DESC, spg.time_played DESC"
    elif sort_by == "last_played":
        sort_expr = "spg.latest_date DESC NULLS LAST, spg.time_played DESC, spg.times_played DESC"
    else:
        sort_expr = "spg.time_played DESC, spg.times_played DESC"
    query = f"""
        WITH filtered AS (
            SELECT tp.*
            FROM time_played tp
            WHERE {where_clause}
        ), summed_per_game AS (
            SELECT
                f.game_id,
                SUM(f.times_played) AS times_played,
                SUM(f.time_played) AS time_played,
                MAX(f.date_played) AS latest_date,
                STRING_AGG(DISTINCT LEFT(f.serial_number, 12), ',') AS serials
            FROM filtered f
            GROUP BY f.game_id
        ), ranked AS (
            SELECT
                spg.game_id, spg.times_played, spg.time_played, spg.latest_date,
                spg.serials,
                ROW_NUMBER() OVER (ORDER BY {sort_expr}) AS sort_rank
            FROM summed_per_game spg
        ), detailed_games AS (
            SELECT
                r.times_played, r.time_played, r.serials,
                r.latest_date,
                COALESCE(t.game_id, r.game_id) AS game_id,
                COALESCE(t.display_name, t.title_en, r.game_id) AS title,
                t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type,
                t.release_year, t.rating_type, t.rating_value, t.region, t.input_controls, t.wifi_players, t.input_players,
                (SELECT COUNT(*) FROM bookmarks bf WHERE bf.game_id = r.game_id) AS favorite_count,
                r.sort_rank
            FROM ranked r
            LEFT JOIN LATERAL (
                SELECT * FROM titles t
                WHERE t.game_id = r.game_id OR SUBSTRING(t.game_id, 1, 4) = SUBSTRING(r.game_id, 1, 4)
                ORDER BY LENGTH(t.game_id) DESC, t.game_id
                LIMIT 1
            ) t ON true
        )
        SELECT * FROM detailed_games
        ORDER BY sort_rank
        LIMIT {limit} OFFSET {offset}
    """
    rows = _run_query(query, params, config.db_url)

    for row in rows:
        serials = (row.get("serials") or "").split(",")
        row["wii_numbers"] = [
            resolve_serial(serial, serial_to_wii) for serial in serials if serial
        ]
    return rows


def fetch_time_played_calendar(serial_prefixes, year, month, serial_to_wii=None):
    """Per-day, per-title play data for one month (calendar view)."""
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []

    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

    query = f"""
        WITH day_games AS (
            SELECT
                tp.date_played AS day,
                LEFT(tp.game_id, 4) AS game_prefix,
                SUM(tp.times_played) AS times_played,
                SUM(tp.time_played) AS time_played,
                STRING_AGG(DISTINCT LEFT(tp.serial_number, 12), ',') AS serials
            FROM time_played tp
            WHERE ({where_clause})
              AND tp.date_played >= %s AND tp.date_played < %s
            GROUP BY tp.date_played, LEFT(tp.game_id, 4)
        )
        SELECT
            dg.day, dg.times_played, dg.time_played, dg.serials,
            COALESCE(t.game_id, dg.game_prefix) AS game_id,
            COALESCE(t.display_name, t.title_en, dg.game_prefix) AS title,
            t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type,
            t.release_year, t.rating_type, t.rating_value, t.region, t.input_controls, t.wifi_players, t.input_players
        FROM day_games dg
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE t.game_id = dg.game_prefix OR SUBSTRING(t.game_id, 1, 4) = dg.game_prefix
            ORDER BY LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY dg.day, dg.time_played DESC
    """
    rows = _run_query(query, [*params, start, end], config.db_url)

    for row in rows:
        serials = (row.get("serials") or "").split(",")
        row["wii_numbers"] = [
            resolve_serial(serial, serial_to_wii) for serial in serials if serial
        ]
    return rows


def fetch_time_played_latest_date(serial_prefixes):
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return None

    rows = _run_query(
        f"""
        SELECT MAX(tp.date_played) AS latest
        FROM time_played tp
        WHERE {where_clause}
        """,
        params,
        config.db_url,
    )
    return rows[0]["latest"] if rows else None


def fetch_time_played_active_months(serial_prefixes):
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []

    rows = _run_query(
        f"""
        SELECT DISTINCT
            EXTRACT(YEAR FROM tp.date_played)::int AS year,
            EXTRACT(MONTH FROM tp.date_played)::int AS month
        FROM time_played tp
        WHERE ({where_clause}) AND tp.date_played IS NOT NULL
        ORDER BY year, month
        """,
        params,
        config.db_url,
    )
    return [(row["year"], row["month"]) for row in rows]


def fetch_time_played_stats(game_id):
    """Fetch aggregated time played stats for a title family."""
    query = """
        SELECT
            COUNT(DISTINCT serial_number) AS total_players,
            SUM(time_played) AS total_minutes,
            ROUND(AVG(time_played)::numeric, 2) AS avg_minutes_per_player
        FROM time_played
        WHERE LEFT(game_id, 3) = %s
    """
    rows = _run_query(query, [game_id], config.db_url)
    return rows[0] if rows else None


def fetch_top_most_played(limit=30):
    """Fetch top games by total time played across all users"""
    query = f"""
        WITH played_groups AS (
            SELECT
                LEFT(tp.game_id, 3) AS title_prefix,
                SUM(tp.time_played) AS total_time_played,
                COUNT(DISTINCT tp.serial_number) AS player_count,
                ROUND(AVG(tp.time_played)::numeric, 2) AS avg_time_per_player
            FROM time_played tp
            GROUP BY LEFT(tp.game_id, 3)
        )
        SELECT
            COALESCE(t.game_id, pg.title_prefix) AS game_id,
            COALESCE(t.display_name, t.title_en, pg.title_prefix) AS title,
            t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type,
            t.release_year, t.rating_type, t.rating_value, t.region, t.input_controls, t.wifi_players, t.input_players,
            pg.total_time_played,
            pg.player_count,
            pg.avg_time_per_player,
            (SELECT COUNT(*) FROM bookmarks bf WHERE LEFT(bf.game_id, 3) = pg.title_prefix) AS favorite_count
        FROM played_groups pg
        LEFT JOIN LATERAL (
            SELECT * FROM titles t
            WHERE LEFT(t.game_id, 3) = pg.title_prefix
            ORDER BY CASE
                WHEN SUBSTRING(t.game_id, 4, 1) = 'E' THEN 1
                WHEN SUBSTRING(t.game_id, 4, 1) = 'P' THEN 2
                WHEN SUBSTRING(t.game_id, 4, 1) = 'J' THEN 3
                WHEN SUBSTRING(t.game_id, 4, 1) = 'K' THEN 4
                WHEN SUBSTRING(t.game_id, 4, 1) = 'C' THEN 5
            END, LENGTH(t.game_id) DESC, t.game_id
            LIMIT 1
        ) t ON true
        ORDER BY total_time_played DESC
        LIMIT {limit}
    """
    return _run_query(query, [], config.db_url)


# User Latest Activity


def fetch_user_latest_games(serial_prefixes, limit=5, serial_to_wii=None):
    """Fetch user's most recently played games."""
    games = fetch_time_played(
        serial_prefixes, sort_by="last_played", serial_to_wii=serial_to_wii
    )
    return games[:limit]


def fetch_user_latest_reviews(serial_prefixes, limit=5, serial_to_wii=None):
    """Fetch user's most recent game recommendations/reviews."""
    reviews = fetch_recommendations(
        serial_prefixes, sort_by="last_recommended", serial_to_wii=serial_to_wii
    )
    return reviews[:limit]


def fetch_user_stats(serial_prefixes, use_cache=True):
    """Fetch user's aggregate statistics (total playtime and review count)."""
    if not serial_prefixes:
        return {"total_minutes": 0, "total_reviews": 0}

    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)

    # Total playtime
    playtime_query = f"""
        SELECT COALESCE(SUM(tp.time_played), 0) AS total_minutes
        FROM time_played tp
        WHERE {where_clause}
    """
    playtime_result = _run_query(
        playtime_query, params, config.db_url, use_cache=use_cache
    )
    total_minutes = playtime_result[0]["total_minutes"] if playtime_result else 0

    # Total reviews/recommendations
    reviews_where_clause, reviews_params = _build_serial_filter(
        "r.serial_number", serial_prefixes
    )
    reviews_query = f"""
        SELECT COUNT(*) AS total_reviews
        FROM recommendations r
        WHERE {reviews_where_clause}
    """
    reviews_result = _run_query(
        reviews_query, reviews_params, config.db_url, use_cache=use_cache
    )
    total_reviews = reviews_result[0]["total_reviews"] if reviews_result else 0

    return {"total_minutes": total_minutes, "total_reviews": total_reviews}


def fetch_metrics_per_wii(serial_prefixes, use_cache=True):
    """Per-serial aggregates: {serial_prefix: {"minutes","reviews","favorites","games"}}."""
    if not serial_prefixes:
        return {}

    tp_where, tp_params = _build_serial_filter("tp.serial_number", serial_prefixes)
    tp_rows = _run_query(
        f"""
        SELECT tp.serial_number AS serial,
               COALESCE(SUM(tp.time_played), 0) AS minutes,
               COUNT(DISTINCT tp.game_id) AS games
        FROM time_played tp
        WHERE {tp_where}
        GROUP BY tp.serial_number
        """,
        tp_params,
        config.db_url,
        use_cache=use_cache,
    )

    r_where, r_params = _build_serial_filter("r.serial_number", serial_prefixes)
    r_rows = _run_query(
        f"""
        SELECT r.serial_number AS serial, COUNT(*) AS reviews
        FROM recommendations r
        WHERE {r_where}
        GROUP BY r.serial_number
        """,
        r_params,
        config.db_url,
        use_cache=use_cache,
    )

    b_where, b_params = _build_serial_filter("b.serial_number", serial_prefixes)
    b_rows = _run_query(
        f"""
        SELECT b.serial_number AS serial, COUNT(DISTINCT b.game_id) AS favorites
        FROM bookmarks b
        WHERE {b_where}
        GROUP BY b.serial_number
        """,
        b_params,
        config.db_url,
        use_cache=use_cache,
    )

    metrics = {}
    for row in tp_rows:
        metrics.setdefault(row["serial"][:12], {}).update(
            minutes=row["minutes"], games=row["games"]
        )
    for row in r_rows:
        metrics.setdefault(row["serial"][:12], {}).update(reviews=row["reviews"])
    for row in b_rows:
        metrics.setdefault(row["serial"][:12], {}).update(favorites=row["favorites"])
    return metrics


def fetch_time_played_per_wii(serial_prefixes, use_cache=True):
    """Per-serial-prefix, per-game playtime rows for a set of serial prefixes."""
    if not serial_prefixes:
        return []
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    return _run_query(
        f"""
        SELECT LEFT(tp.serial_number, 12) AS serial, tp.game_id,
               SUM(tp.time_played) AS time_played, SUM(tp.times_played) AS times_played
        FROM time_played tp
        WHERE {where_clause}
        GROUP BY LEFT(tp.serial_number, 12), tp.game_id
        """,
        params,
        config.db_url,
        use_cache=use_cache,
    )
