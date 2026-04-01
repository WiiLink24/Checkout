import psycopg2
import config
import re
import requests
import hashlib

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
    """Fetch top games by average recommendation percent across all users"""
    query = (
        "SELECT "
        "r.game_id, "
        "COALESCE(t.display_name, t.title_en, r.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region, "
        "ROUND(AVG(r.recommendation_percent)::numeric, 2) AS avg_recommendation, "
        "COUNT(DISTINCT r.serial_number) AS reviewer_count "
        "FROM recommendations r "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        "GROUP BY r.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, t.release_year, t.rating_type, t.rating_value, t.region "
        "ORDER BY avg_recommendation DESC "
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
        "Authorization": f"Bearer {config.authentik_service_account_token}"
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
    Fetch all Authentik users that contain the 'serial' key
    inside their attributes JSON.
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/?page_size=100&attributes=%7B%22serial__isnull%22%3A+false%7D"
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

    print(f"Fetched {len(users)} users with 'serial' attribute")
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


def find_game_recommendation(serial_prefixes):
    """Find a game recommendation based on user's highly-rated games"""
    if not serial_prefixes:
        return None
    
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    
    # Get genres from user's top-rated games
    query = (
        "SELECT DISTINCT t.genre "
        "FROM recommendations r "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"WHERE {where_clause} AND t.genre IS NOT NULL "
        "LIMIT 5"
    )
    
    genre_results = _run_query(query, params)
    if not genre_results:
        return None
    
    # Get count of games the user has rated
    count_query = f"SELECT COUNT(DISTINCT game_id) as total FROM recommendations WHERE {where_clause}"
    count_result = _run_query(count_query, params)
    total_rated = count_result[0]['total'] if count_result else 0
    
    # Get games the user HAS recommended
    recommended_games = _run_query(
        f"SELECT DISTINCT game_id FROM recommendations WHERE {where_clause}",
        params
    )
    recommended_game_ids = [g["game_id"] for g in recommended_games]
    
    # Build genre list - extract first genre from comma-separated list
    genres_to_search = []
    for g in genre_results:
        if g['genre']:
            first_genre = g['genre'].split(',')[0].strip()
            if first_genre:
                genres_to_search.append(first_genre)
    
    if not genres_to_search:
        return None
    
    # Build SQL with proper parameter handling
    genre_placeholders = " OR ".join([f"t.genre ILIKE %s"] * len(genres_to_search))
    genre_patterns = [f"%{g}%" for g in genres_to_search]
    
    exclude_clause = ""
    if recommended_game_ids:
        placeholders = ",".join(["%s"] * len(recommended_game_ids))
        exclude_clause = f"AND t.game_id NOT IN ({placeholders})"
        genre_patterns.extend(recommended_game_ids)
    
    query = (
        "SELECT "
        "t.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type "
        "FROM titles t "
        f"WHERE ({genre_placeholders}) {exclude_clause} "
        "AND t.display_name IS NOT NULL "
        "ORDER BY RANDOM() "
        "LIMIT 1"
    )
    
    result = _run_query(query, genre_patterns)
    if result:
        game = result[0]
        
        # Add recommendation explanation
        matched_genre = game.get('genre', '').split(',')[0].strip() if game.get('genre') else 'Unknown'
        game["reason"] = {
            "genres": ", ".join(genres_to_search[:2]),  # Show top 2 genres user likes
            "matched_genre": matched_genre,
            "total_rated": total_rated
        }
        
        return game
    
    return None


def fetch_user_latest_games(serial_prefixes, limit=5):
    """Fetch latest games played by the user"""
    where_clause, params = _build_serial_filter("tp.serial_number", serial_prefixes)
    if not where_clause:
        return []
    
    query = (
        "SELECT DISTINCT ON (tp.game_id) "
        "tp.game_id, tp.serial_number, tp.time_played, "
        "COALESCE(t.display_name, t.title_en, tp.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region "
        "FROM time_played tp "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE tp.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"WHERE {where_clause} "
        "ORDER BY tp.game_id, tp.id DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, params)


def fetch_user_latest_reviews(serial_prefixes, limit=5):
    """Fetch latest game reviews (recommendations) by the user"""
    where_clause, params = _build_serial_filter("r.serial_number", serial_prefixes)
    if not where_clause:
        return []
    
    query = (
        "SELECT DISTINCT ON (r.game_id) "
        "r.game_id, r.serial_number, r.recommendation_percent, "
        "COALESCE(t.display_name, t.title_en, r.game_id) AS title, "
        "t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer, t.publisher, t.game_type, "
        "t.release_year, t.rating_type, t.rating_value, t.region "
        "FROM recommendations r "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"WHERE {where_clause} "
        "ORDER BY r.game_id, r.id DESC "
        f"LIMIT {limit}"
    )
    return _run_query(query, params)


def fetch_global_stats():
    """Fetch global statistics: total time played and total reviews"""
    query_time = "SELECT COALESCE(SUM(time_played), 0) AS total_minutes FROM time_played"
    query_reviews = "SELECT COUNT(*) AS total_reviews FROM recommendations"
    
    time_result = _run_query(query_time, [])
    reviews_result = _run_query(query_reviews, [])
    
    total_minutes = time_result[0]['total_minutes'] if time_result else 0
    total_reviews = reviews_result[0]['total_reviews'] if reviews_result else 0
    
    return {
        'total_minutes': int(total_minutes),
        'total_reviews': int(total_reviews)
    }


def fetch_user_stats(serial_prefixes):
    """Fetch user-specific statistics: total time played and total reviews"""
    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)
    if not where_clause:
        return {
            'total_minutes': 0,
            'total_reviews': 0
        }
    
    # Get total time played
    time_query = f"SELECT COALESCE(SUM(time_played), 0) AS total_minutes FROM time_played WHERE {where_clause}"
    time_result = _run_query(time_query, params)
    total_minutes = int(time_result[0]['total_minutes']) if time_result else 0
    
    # Get total reviews
    reviews_query = f"SELECT COUNT(*) AS total_reviews FROM recommendations WHERE {where_clause}"
    reviews_result = _run_query(reviews_query, params)
    total_reviews = int(reviews_result[0]['total_reviews']) if reviews_result else 0
    
    return {
        'total_minutes': total_minutes,
        'total_reviews': total_reviews
    }