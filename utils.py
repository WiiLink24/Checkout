import psycopg2
import config
import re
import requests
import hashlib
import math
import random
from collections import defaultdict


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


def fetch_recommendations(serial_prefixes, sort_by="recommendation_percent"):
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
        f"ORDER BY {order_by}"
    )
    return _run_query(query, params)


def fetch_time_played(serial_prefixes, sort_by="time_played"):
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
        "ORDER BY sort_rank"
    )
    return _run_query(query, params)


def fetch_favorites(serial_prefixes, limit=30):
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
        f"LIMIT {limit}"
    )
    rows = _run_query(query, params)

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
    rows = _run_query(query, params)
    return rows[0] if rows else None


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
    rows = _run_query(query, [game_id])
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
    return _run_query(query, [])


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
    return _run_query(query, [])


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
    return _run_query(query, [])


def fetch_authentik_user(uid):
    """Fetch user details from Authentik API"""
    import requests
    import config

    url = f"{config.authentik_api_url}/core/users/{uid}/"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("attributes", {})
        return None
    except Exception as e:
        print(f"Error fetching user {uid}: {e}")
        return None


def fetch_authentik_users():
    """
    Fetch all Authentik users that contain the 'serial' key inside their attributes JSON.
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/?page_size=30&attributes=%7B%22serial__isnull%22%3A+false%7D"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }

    users = []

    try:
        while url:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            users.extend(data.get("results", []))
            url = data.get("pagination", {}).get("next")

    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
        return []

    return users


def find_user_by_serial(serial):
    """Find an Authentik user by their serial number"""
    users = fetch_authentik_users()

    # Normalize the search serial (handle if it's a list)
    if isinstance(serial, list):
        serial = serial[0] if serial else None

    if not serial:
        return None

    # Search for user with matching serial
    for user in users:
        user_serials = user.get("attributes", {}).get("serial", [])
        if isinstance(user_serials, str):
            user_serials = [user_serials]

        if serial in user_serials:
            return user


def find_user_by_wii_number(wii_number):
    """Find an Authentik user by their Wii number (friend code)"""
    users = fetch_authentik_users()

    # Normalize the Wii number (handle if it's a list)
    if isinstance(wii_number, list):
        wii_number = wii_number[0] if wii_number else None

    if not wii_number:
        return None

    # Search for user with matching Wii number
    for user in users:
        wii_numbers = user.get("attributes", {}).get("wiis", [])
        if isinstance(wii_numbers, str):
            wii_numbers = [wii_numbers]

        if wii_number in wii_numbers:
            return user
    return None


def find_user_by_serial(serial):
    """Find an Authentik user by their console serial number"""
    users = fetch_authentik_users()

    # Normalize the serial
    serial = normalize_serial(serial) if serial else None

    if not serial:
        return None

    # Search for user with matching serial
    for user in users:
        user_serials = user.get("attributes", {}).get("serial", [])
        if isinstance(user_serials, str):
            user_serials = [user_serials]

        # Check if the serial matches any of the user's serials
        for user_serial in user_serials:
            if normalize_serial(user_serial) == serial:
                return user

    return None


def normalize_serial(serial):
    """Normalize serial input by stripping quotes and brackets"""
    return serial.strip("[]'\" ")


def extract_serial_prefix(serial):
    """Extract serial prefix (first 11 characters: 2 letters + 9 digits)"""
    match = re.match(r"^([A-Z]{2}\d{9})", serial)
    return [match.group(1)] if match else [serial]


def generate_gravatar_url(email):
    """Generate Gravatar URL from email address"""
    if not email:
        return "https://www.gravatar.com/avatar/default?d=identicon&s=128"
    hash_digest = hashlib.sha256(email.encode()).hexdigest()
    return f"https://www.gravatar.com/avatar/{hash_digest}?d=identicon&s=128"


def build_viewed_user_info(authentik_user):
    """Build viewed_user info dict from an Authentik user object"""
    username = authentik_user.get("username")
    email = authentik_user.get("email", "")
    picture_url = generate_gravatar_url(email)

    # Get Wii numbers from Authentik attributes
    wii_numbers = authentik_user.get("attributes", {}).get("wiis", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    return {
        "username": username,
        "profile_picture": picture_url,
        "linked_wii_no": wii_numbers,
        "serial": wii_numbers,
    }


def build_unclaimed_user_info(serial, logged_in_user_picture):
    """Build viewed_user info dict for an unclaimed serial"""
    return {
        "username": serial,
        "profile_picture": logged_in_user_picture,
        "linked_wii_no": [serial],
        "serial": serial,
    }


def _split_genres(genre_value):
    """Split a comma-separated genre string into clean tokens."""
    if not genre_value:
        return []
    return [g.strip() for g in genre_value.split(",") if g.strip()]


def _normalize_scores(score_map):
    """Normalize scores to 0..1 while preserving relative ordering."""
    if not score_map:
        return {}
    max_value = max(score_map.values()) or 1.0
    return {k: (v / max_value) for k, v in score_map.items()}


def _build_discover_profile(where_clause, params):
    """Build user taste profile from recommendation history."""
    query = (
        "SELECT "
        "r.game_id, r.recommendation_percent, "
        "t.genre, t.developer, t.publisher, t.game_type "
        "FROM recommendations r "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"WHERE {where_clause}"
    )
    rows = _run_query(query, params)
    if not rows:
        return None

    genre_scores = defaultdict(float)
    developer_scores = defaultdict(float)
    publisher_scores = defaultdict(float)
    game_type_scores = defaultdict(float)
    reviewed_ids = set()

    for row in rows:
        reviewed_ids.add(row.get("game_id"))

        recommendation_percent = float(row.get("recommendation_percent") or 0)
        # Map recommendation score into a positive preference weight (0..1).
        preference_weight = max(0.0, min(1.0, (recommendation_percent - 40.0) / 60.0))
        if preference_weight <= 0:
            continue

        for genre in _split_genres(row.get("genre"))[:3]:
            genre_scores[genre] += preference_weight

        developer = (row.get("developer") or "").strip()
        if developer:
            developer_scores[developer] += preference_weight * 0.65

        publisher = (row.get("publisher") or "").strip()
        if publisher:
            publisher_scores[publisher] += preference_weight * 0.45

        game_type = (row.get("game_type") or "").strip()
        if game_type:
            game_type_scores[game_type] += preference_weight * 0.35

    top_genres = [
        g
        for g, _ in sorted(
            genre_scores.items(), key=lambda item: item[1], reverse=True
        )[:5]
    ]
    return {
        "reviewed_ids": reviewed_ids,
        "genre_scores": _normalize_scores(genre_scores),
        "developer_scores": _normalize_scores(developer_scores),
        "publisher_scores": _normalize_scores(publisher_scores),
        "game_type_scores": _normalize_scores(game_type_scores),
        "top_genres": top_genres,
        "total_rated": len(reviewed_ids),
    }


def _is_already_seen(candidate_game_id, seen_game_ids):
    """Exclude candidates already seen by the user (played or reviewed)."""
    if candidate_game_id in seen_game_ids:
        return True
    for seen_id in seen_game_ids:
        if not seen_id:
            continue
        if candidate_game_id.startswith(seen_id) or seen_id.startswith(
            candidate_game_id
        ):
            return True
    return False


def _score_discover_candidate(candidate, profile):
    """Score a candidate game using weighted user affinity and community signal."""
    genres = _split_genres(candidate.get("genre"))
    genre_scores = profile["genre_scores"]

    if genres:
        genre_match_best = max((genre_scores.get(g, 0.0) for g in genres), default=0.0)
        genre_match_sum = sum((genre_scores.get(g, 0.0) for g in genres))
        genre_score = genre_match_best + (0.30 * genre_match_sum)
    else:
        genre_match_best = 0.0
        genre_score = 0.0

    developer_score = profile["developer_scores"].get(
        (candidate.get("developer") or "").strip(), 0.0
    )
    publisher_score = profile["publisher_scores"].get(
        (candidate.get("publisher") or "").strip(), 0.0
    )
    game_type_score = profile["game_type_scores"].get(
        (candidate.get("game_type") or "").strip(), 0.0
    )

    avg_recommendation = float(candidate.get("avg_recommendation") or 50.0)
    rating_count = int(candidate.get("rating_count") or 0)
    confidence = (
        min(1.0, math.log1p(rating_count) / math.log(25)) if rating_count > 0 else 0.0
    )
    community_score = ((avg_recommendation - 50.0) / 50.0) * confidence

    # Small jitter keeps discover results from feeling static while preserving quality.
    exploration_jitter = random.uniform(0.0, 0.03)

    total_score = (
        (0.55 * genre_score)
        + (0.18 * developer_score)
        + (0.12 * publisher_score)
        + (0.05 * game_type_score)
        + (0.10 * community_score)
        + exploration_jitter
    )

    matched_genre = "Unknown"
    if genres:
        matched_genre = max(genres, key=lambda g: genre_scores.get(g, 0.0))

    return total_score, matched_genre


def find_game_recommendation(serial_prefixes):
    """Find a discover recommendation using weighted taste matching."""
    if not serial_prefixes:
        return None

    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)

    profile = _build_discover_profile(where_clause, params)
    if not profile:
        return None

    played_rows = _run_query(
        f"SELECT DISTINCT game_id FROM time_played WHERE {where_clause}",
        params,
    )
    played_ids = {row.get("game_id") for row in played_rows if row.get("game_id")}
    seen_game_ids = set(profile["reviewed_ids"]) | played_ids

    candidates = _run_query(
        (
            "SELECT "
            "t.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, "
            "t.game_type, t.release_year, t.rating_type, t.rating_value, t.region, "
            "COALESCE(stats.avg_recommendation, 50) AS avg_recommendation, "
            "COALESCE(stats.rating_count, 0) AS rating_count "
            "FROM titles t "
            "LEFT JOIN LATERAL ("
            "    SELECT AVG(r.recommendation_percent) AS avg_recommendation, COUNT(*) AS rating_count "
            "    FROM recommendations r "
            "    WHERE t.game_id LIKE r.game_id || '%%'"
            ") stats ON true "
            "WHERE t.display_name IS NOT NULL "
            "AND t.genre IS NOT NULL "
            "ORDER BY COALESCE(stats.rating_count, 0) DESC, COALESCE(stats.avg_recommendation, 50) DESC "
            "LIMIT 1200"
        ),
        [],
    )

    scored_candidates = []
    for candidate in candidates:
        candidate_game_id = candidate.get("game_id")
        if not candidate_game_id or _is_already_seen(candidate_game_id, seen_game_ids):
            continue

        score, matched_genre = _score_discover_candidate(candidate, profile)
        scored_candidates.append((score, matched_genre, candidate))

    if not scored_candidates:
        return None

    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    best_score, matched_genre, best_game = scored_candidates[0]

    best_game["reason"] = {
        "genres": (
            ", ".join(profile["top_genres"][:2])
            if profile["top_genres"]
            else "your recent likes"
        ),
        "matched_genre": matched_genre,
        "total_rated": profile["total_rated"],
        "score": round(best_score, 3),
    }
    return best_game

    return None


def fetch_user_latest_games(serial_prefixes, limit=5):
    """Fetch latest games played by the user"""
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []

    query = (
        "WITH latest_per_game AS ("
        "    SELECT DISTINCT ON (tp.game_id) "
        "    tp.id, tp.game_id, tp.serial_number, tp.time_played "
        "    FROM time_played tp "
        f"    WHERE {where_clause} "
        "    ORDER BY tp.game_id, tp.id DESC"
        ") "
        "SELECT "
        "lp.game_id, lp.serial_number, lp.time_played, "
        "COALESCE(t.display_name, t.title_en, lp.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region "
        "FROM latest_per_game lp "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE lp.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "ORDER BY lp.id DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, params)


def fetch_user_latest_reviews(serial_prefixes, limit=5):
    """Fetch latest game reviews (recommendations) by the user"""
    where_clause, params = _build_serial_filter("r.serial_number", serial_prefixes)
    if not where_clause:
        return []

    query = (
        "WITH latest_per_game AS ("
        "    SELECT DISTINCT ON (r.game_id) "
        "    r.id, r.game_id, r.serial_number, r.recommendation_percent "
        "    FROM recommendations r "
        f"    WHERE {where_clause} "
        "    ORDER BY r.game_id, r.id DESC"
        ") "
        "SELECT "
        "lp.game_id, lp.serial_number, lp.recommendation_percent, "
        "COALESCE(t.display_name, t.title_en, lp.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region "
        "FROM latest_per_game lp "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE lp.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "ORDER BY lp.id DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, params)


def fetch_global_stats():
    """Fetch global statistics: total time played and total reviews"""
    query_time = (
        "SELECT COALESCE(SUM(time_played), 0) AS total_minutes FROM time_played"
    )
    query_reviews = "SELECT COUNT(*) AS total_reviews FROM recommendations"

    time_result = _run_query(query_time, [])
    reviews_result = _run_query(query_reviews, [])

    total_minutes = time_result[0]["total_minutes"] if time_result else 0
    total_reviews = reviews_result[0]["total_reviews"] if reviews_result else 0

    return {"total_minutes": int(total_minutes), "total_reviews": int(total_reviews)}


def fetch_user_stats(serial_prefixes):
    """Fetch user-specific statistics: total time played and total reviews"""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return {"total_minutes": 0, "total_reviews": 0}

    # Get total time played
    time_query = f"SELECT COALESCE(SUM(time_played), 0) AS total_minutes FROM time_played WHERE {where_clause}"
    time_result = _run_query(time_query, params)
    total_minutes = int(time_result[0]["total_minutes"]) if time_result else 0

    # Get total reviews
    reviews_query = (
        f"SELECT COUNT(*) AS total_reviews FROM recommendations WHERE {where_clause}"
    )
    reviews_result = _run_query(reviews_query, params)
    total_reviews = int(reviews_result[0]["total_reviews"]) if reviews_result else 0

    return {"total_minutes": total_minutes, "total_reviews": total_reviews}
