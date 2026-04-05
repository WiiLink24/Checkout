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


def _run_query(query, params, db_url=None):
    """Execute a query against a specified database (defaults to config.db_url)."""
    if db_url is None:
        db_url = config.db_url
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]


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


def find_user_by_wii_number(wii_number):
    """
    Find an Authentik user by their Wii number (friend code).
    Returns the first matching user or None (there can only be one).
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f'{base_url}/core/users/?page_size=30&attributes=%7B%22wiis__contains%22%3A+"{wii_number}"%7D'
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None

    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
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


def search_authentik_users_by_name(search_query):
    """
    Search Authentik users by username.
    Returns all matching users that contain the search query in their username and have wiis linked.
    """
    if not search_query or not search_query.strip():
        return []

    base_url = config.authentik_api_url.rstrip("/")
    # Use search parameter for username search
    url = f"{base_url}/core/users/?page_size=50&search={search_query}"
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
            results = data.get("results", [])
            users.extend(
                [user for user in results if user.get("attributes", {}).get("wiis")]
            )
            url = data.get("pagination", {}).get("next")

    except requests.RequestException as e:
        print(f"Authentik API error searching for '{search_query}': {e}")
        return []

    return users


def find_user_by_serial(serial):
    """
    Find an Authentik user by their serial number.
    Returns the first matching user or None (there can only be one).
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f'{base_url}/core/users/?page_size=30&attributes=%7B%22serial__icontains%22%3A+"{serial}"%7D'
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None

    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
        return None


def normalize_serial(serial):
    return serial.strip("[]'\" ").replace("-", "") if serial else serial


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
    # Handle case where authentik_user is unexpectedly a list
    if isinstance(authentik_user, list):
        authentik_user = authentik_user[0] if authentik_user else {}

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


def fetch_user_latest_games(serial_prefixes, limit=5):
    """Fetch user's most recently played games."""
    from nc import fetch_time_played

    games = fetch_time_played(serial_prefixes, sort_by="last_played")
    return games[:limit]


def fetch_user_latest_reviews(serial_prefixes, limit=5):
    """Fetch user's most recent game recommendations/reviews."""
    from nc import fetch_recommendations

    reviews = fetch_recommendations(serial_prefixes, sort_by="last_recommended")
    return reviews[:limit]


def fetch_global_stats():
    """Fetch global statistics: total time played and total reviews"""
    query_time = (
        "SELECT COALESCE(SUM(time_played), 0) AS total_minutes FROM time_played"
    )
    query_reviews = "SELECT COUNT(*) AS total_reviews FROM recommendations"

    time_result = _run_query(query_time, [], config.db_url)
    reviews_result = _run_query(query_reviews, [], config.db_url)

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
    time_result = _run_query(time_query, params, config.db_url)
    total_minutes = int(time_result[0]["total_minutes"]) if time_result else 0

    # Get total reviews
    reviews_query = (
        f"SELECT COUNT(*) AS total_reviews FROM recommendations WHERE {where_clause}"
    )
    reviews_result = _run_query(reviews_query, params, config.db_url)
    total_reviews = int(reviews_result[0]["total_reviews"]) if reviews_result else 0

    return {"total_minutes": total_minutes, "total_reviews": total_reviews}
