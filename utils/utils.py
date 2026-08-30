import psycopg2
import re
import config
import requests
import hashlib
import threading
import copy
import time
from flask_caching import Cache


def extract_linked_wiis(attributes):
    """(serial_prefixes, wii_numbers) for every linked Wii in an attributes dict."""
    serial_prefixes, wii_numbers = [], []
    wiis = (attributes or {}).get("wiis")
    if isinstance(wiis, list):
        for wii in wiis:
            if not isinstance(wii, dict):
                continue
            serial = wii.get("serial_number")
            wii_number = wii.get("wii_number")
            if serial:
                serial_prefixes.append(serial[:12])
            if wii_number:
                wii_numbers.append(wii_number)
    return serial_prefixes, wii_numbers


def get_serial_prefixes(user_info):
    return extract_linked_wiis(user_info)[0]


def _build_serial_filter(column_name, serial_prefixes):
    if not serial_prefixes:
        return "", []
    clauses = " OR ".join([f"{column_name} LIKE %s" for _ in serial_prefixes])
    params = [f"{prefix}%" for prefix in serial_prefixes]
    return clauses, params


_connections = threading.local()
_connection_registry = {}
_registry_lock = threading.Lock()

_QUERY_CACHE_TTL = 3 * 60 * 60
cache = Cache()


def _get_connection(db_url):
    """Return a cached persistent connection for the given db_url, opening one on first use."""
    conn = getattr(_connections, db_url, None)
    if conn is None or conn.closed:
        conn = psycopg2.connect(db_url)
        setattr(_connections, db_url, conn)
        with _registry_lock:
            _connection_registry.setdefault(db_url, set()).add(conn)
    return conn


def close_db_connections():
    """Close all open database connections (called on server shutdown)."""
    with _registry_lock:
        for conns in _connection_registry.values():
            for conn in conns:
                try:
                    if not conn.closed:
                        conn.close()
                except Exception:
                    pass
        _connection_registry.clear()


def _execute_query(query, params, db_url):
    conn = _get_connection(db_url)
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        conn.commit()
    finally:
        cur.close()
    return [dict(zip(columns, row)) for row in rows]


def _drop_connection(db_url):
    """Close and remove the cached connection for db_url so a fresh one is opened next use."""
    conn = getattr(_connections, db_url, None)
    if conn is not None:
        try:
            if not conn.closed:
                conn.close()
        except Exception:
            pass
        delattr(_connections, db_url)
        with _registry_lock:
            conns = _connection_registry.get(db_url)
            if conns:
                conns.discard(conn)
                if not conns:
                    del _connection_registry[db_url]


def _short_query(query):
    return " ".join(query.split())[:80]


def _query_cache_key(db_url, query, params_tuple):
    raw = f"{db_url}|{query}|{params_tuple}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _sanitize_for_cache(value):
    """Convert non-picklable types (e.g. psycopg2 memoryview) into picklable ones."""
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, dict):
        return {k: _sanitize_for_cache(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_cache(v) for v in value]
    return value


def _run_query(query, params, db_url=None, use_cache=True):
    """Execute a query, optionally bypassing the result cache."""
    if db_url is None:
        db_url = config.db_url
    params_tuple = tuple(params or [])
    cache_key = _query_cache_key(db_url, query, params_tuple)
    start = time.perf_counter()

    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            print(
                f"[DB] CACHE HIT ({time.perf_counter() - start:.3f}s): {_short_query(query)}"
            )
            return copy.deepcopy(cached)

    try:
        result = _execute_query(query, params, db_url)
    except psycopg2.OperationalError:
        _drop_connection(db_url)
        result = _execute_query(query, params, db_url)

    result = _sanitize_for_cache(result)
    if use_cache:
        cache.set(cache_key, result, timeout=_QUERY_CACHE_TTL)
    print(f"[DB] QUERY ({time.perf_counter() - start:.3f}s): {_short_query(query)}")
    return copy.deepcopy(result)


def _run_query_one(query, params, db_url=None, use_cache=True):
    rows = _run_query(query, params, db_url, use_cache=use_cache)
    return rows[0] if rows else None


def _execute(query, params, db_url=None):
    if db_url is None:
        db_url = config.db_url
    conn = _get_connection(db_url)
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        rowcount = cur.rowcount
        conn.commit()
    finally:
        cur.close()
    return rowcount


def find_user_by_wii_number(wii_number, attempt=0):
    """
    Find an Authentik user by their Wii number (friend code).
    Returns the first matching user or None (there can only be one).
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f'{base_url}/core/users/?page_size=30&attributes=%7B%22wiis__{attempt}__wii_number%22%3A+"{wii_number}"%7D'
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if (
            not results and attempt < 10
        ):  # Honestly fuck you if you have more than 9 Wiis.
            return find_user_by_wii_number(wii_number, attempt=attempt + 1)
        return results[0] if results else None
    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
        return None


def _normalize_serial_prefix(serial):
    """Console identity: strip the device id (everything from the first
    '+' or space separator). Serials are 11 or 12 chars before it, so a
    plain [:12] cut is not enough."""
    return re.split("[ +]", serial or "", 1)[0]

def build_serial_to_wii_mapping(attributes):
    """We resolve the serial locally once the user object is in hand."""
    mapping = {}
    wiis = (attributes or {}).get("wiis")
    if isinstance(wiis, list):
        for wii in wiis:
            if not isinstance(wii, dict):
                continue
            serial = wii.get("serial_number")
            wii_number = wii.get("wii_number")
            if serial and wii_number:
                mapping[_normalize_serial_prefix(serial)] = wii_number
    return mapping


def resolve_serial(serial, serial_to_wii=None):
    return serial_to_wii.get(_normalize_serial_prefix(serial or ""))

def fetch_authentik_users():
    """
    Fetch all Authentik users that have their profile set to public.
    """
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/?page_size=30&attributes=%7B%22public_profile%22%3A+true%7D"
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
            next_url = data.get("pagination", {}).get("next")

            if isinstance(next_url, str) and (
                next_url.startswith("http://") or next_url.startswith("https://")
            ):
                url = next_url
            else:
                url = None

    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
        return []

    return users


def fetch_all_authentik_users():
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/?page_size=50"
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
            next_url = data.get("pagination", {}).get("next")

            if isinstance(next_url, str) and (
                next_url.startswith("http://") or next_url.startswith("https://")
            ):
                url = next_url
            else:
                url = None

    except requests.RequestException as e:
        print(f"Authentik API error: {e}")
        return []

    return users


def get_authentik_user(user):
    """Fetch a single Authentik user's fresh data (detail endpoint is keyed by pk)."""
    user_id = user.get("pk") or user.get("uuid")
    if not user_id:
        raise ValueError("User has neither pk nor uuid")
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/{user_id}/"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()


def fetch_authentik_user_by_username(username):
    """Fetch a single Authentik user by username."""
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }
    response = requests.get(
        url, headers=headers, params={"username": username, "page_size": 1}, timeout=15
    )
    response.raise_for_status()
    results = response.json().get("results", [])
    return results[0] if results else None


def update_user_attributes(user, attributes):
    user_id = user.get("pk") or user.get("uuid")
    if not user_id:
        raise ValueError("User has neither pk nor uuid")
    base_url = config.authentik_api_url.rstrip("/")
    url = f"{base_url}/core/users/{user_id}/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.authentik_service_account_token}",
    }
    response = requests.patch(
        url, json={"attributes": attributes}, headers=headers, timeout=15
    )
    response.raise_for_status()
    return response.json()


def update_user_achievements(user, payload):
    fresh_user = get_authentik_user(user)
    if not fresh_user:
        raise ValueError("User not found")
    attributes = (fresh_user.get("attributes") or {}).copy()
    attributes["achievements"] = payload
    return update_user_attributes(fresh_user, attributes)


def search_authentik_users_by_name(search_query):
    """
    Search Authentik users by username.
    Returns all matching users that contain the search query in their username and have wiis linked.
    """
    if not search_query or not search_query.strip():
        return []

    base_url = config.authentik_api_url.rstrip("/")
    # Use search parameter for username search
    url = f"{base_url}/core/users/?page_size=50&search={search_query}&attributes=%7B%22public_profile%22%3A+true%7D"
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
            for user in results:
                email = user.get("email", "")
                user["avatar"] = generate_gravatar_url(email)
            users.extend(
                [user for user in results if user.get("attributes", {}).get("wiis")]
            )
            next_url = data.get("pagination", {}).get("next")
            if isinstance(next_url, str) and (
                next_url.startswith("http://") or next_url.startswith("https://")
            ):
                url = next_url
            else:
                url = None

    except requests.RequestException as e:
        print(f"Authentik API error searching for '{search_query}': {e}")
        return []

    return users


def normalize_serial(serial):
    return serial.strip("[]'\" ").replace("-", "") if serial else serial


def extract_serial_prefix(serial):
    return [serial[:12]]


def generate_gravatar_url(email):
    """Generate Gravatar URL from email address"""
    if not email:
        return "https://www.gravatar.com/avatar/default?d=identicon&s=128"
    hash_digest = hashlib.sha256(email.encode()).hexdigest()
    return f"https://www.gravatar.com/avatar/{hash_digest}?d=identicon&s=128"


def build_viewed_user_info(authentik_user):
    """Build viewed_user info dict from an Authentik user object"""
    from utils.achievements import parse_achievements

    if isinstance(authentik_user, list):
        authentik_user = authentik_user[0] if authentik_user else {}

    username = authentik_user.get("username")
    email = authentik_user.get("email", "")
    picture_url = generate_gravatar_url(email)

    wiis = authentik_user.get("attributes", {}).get("wiis") or authentik_user.get(
        "wiis", []
    )
    wii_numbers = []

    if isinstance(wiis, list):
        for wii in wiis:
            if isinstance(wii, dict):
                wii_number = wii.get("wii_number")
                if wii_number:
                    wii_numbers.append(wii_number)
    return {
        "username": username,
        "profile_picture": picture_url,
        "linked_wii_no": wii_numbers,
        "serial_number": wii_numbers,
        "achievements": parse_achievements(authentik_user.get("attributes", {})),
    }


def build_unclaimed_user_info(serial, logged_in_user_picture):
    """Build viewed_user info dict for an unclaimed serial"""
    return {
        "username": serial,
        "profile_picture": logged_in_user_picture,
        "linked_wii_no": [serial],
        "serial_number": serial,
    }


def format_serial(s):
    """Format serial number with dashes every 4 characters"""
    s = str(s)
    return "-".join([s[i : i + 4] for i in range(0, len(s), 4)])


def format_playtime(minutes):
    """Format minutes as years, days, hours, minutes"""
    if not minutes:
        return "0m"
    minutes = int(minutes)
    years = minutes // (365 * 24 * 60)
    remaining = minutes % (365 * 24 * 60)
    days = remaining // (24 * 60)
    remaining = remaining % (24 * 60)
    hours = remaining // 60
    mins = remaining % 60

    parts = []
    if years > 0:
        parts.append(f"{years}y")
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if mins > 0 or not parts:
        parts.append(f"{mins}m")
    return " ".join(parts)
