import psycopg2
import config
import re

def get_serial_prefixes(user_info):
    serials = user_info.get("serial")
    if isinstance(serials, str):
        serials = [serials]
    if not isinstance(serials, list) or not serials:
        return []
    prefixes = []
    for serial in serials:
        match = re.match(r"^([A-Z]{2}\d{9})", serial)
        if match:
            prefixes.append(match.group(1))
    return prefixes

def _build_serial_filter(column_name, serial_prefixes):
    if not serial_prefixes:
        return "", []
    clauses = " OR ".join([f"{column_name} LIKE %s" for _ in serial_prefixes])
    params = [f"{prefix}%" for prefix in serial_prefixes]
    return clauses, params

def _run_query(query, params):
    conn = psycopg2.connect(config.db_url)
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def fetch_recommendations(serial_prefixes):
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return []
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
        "ORDER BY r.recommendation_percent DESC, t.title_en NULLS LAST, r.game_id"
    )
    return _run_query(query, params)

def fetch_time_played(serial_prefixes, sort_by="time_played"):
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []
    
    if sort_by == "times_played":
        sort_expr = "MAX(tp.times_played) DESC, MAX(tp.time_played) DESC"
    elif sort_by == "last_played":
        sort_expr = "MAX(tp.id) DESC"
    else:
        sort_expr = "MAX(tp.time_played) DESC, MAX(tp.times_played) DESC"
    
    query = (
        "WITH game_stats AS ("
        "    SELECT "
        "    tp.game_id,"
        "    MAX(tp.times_played) as max_times_played,"
        "    MAX(tp.time_played) as max_time_played,"
        "    MAX(tp.id) as max_id,"
        "    ROW_NUMBER() OVER (ORDER BY " + sort_expr + ") as sort_rank "
        "    FROM time_played tp "
        f"    WHERE {where_clause} "
        "    GROUP BY tp.game_id"
        "), "
        "detailed_games AS ("
        "    SELECT "
        "    tp.id, tp.serial_number, tp.game_id, tp.times_played, tp.time_played, "
        "    COALESCE(t.display_name, t.title_en, tp.game_id) AS title, "
        "    t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "    t.release_year, t.rating_type, t.rating_value, t.region, "
        "    gs.sort_rank, "
        "    ROW_NUMBER() OVER (PARTITION BY tp.game_id ORDER BY tp.id DESC) as rn "
        "    FROM game_stats gs "
        "    JOIN time_played tp ON tp.game_id = gs.game_id "
        "    LEFT JOIN LATERAL ("
        "        SELECT * FROM titles t "
        "        WHERE t.game_id LIKE tp.game_id || '%%' "
        "        ORDER BY t.game_id "
        "        LIMIT 1"
        "    ) t ON true"
        ") "
        "SELECT * FROM detailed_games "
        "WHERE rn = 1 "
        "ORDER BY sort_rank"
    )
    return _run_query(query, params)

def fetch_recommendation_averages(game_id, gender=None, age_min=None, age_max=None):
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
    rows = _run_query(query, params)
    return rows[0] if rows else None

def fetch_time_played_stats(game_id):
    query = (
        "SELECT "
        "COUNT(DISTINCT serial_number) AS total_players, "
        "SUM(time_played) AS total_minutes, "
        "ROUND(AVG(time_played)::numeric, 2) AS avg_minutes_per_player "
        "FROM time_played "
        "WHERE game_id = %s"
    )
    rows = _run_query(query, [game_id])
    return rows[0] if rows else None
