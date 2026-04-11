import config
from utils.utils import _run_query
import requests
from io import BytesIO
from urllib.parse import urlencode


def render_mii_to_url(mii_data):
    if not mii_data:
        return None

    try:
        files = {"data": ("file.miigx", BytesIO(mii_data), "application/octet-stream")}
        data = {"platform": "wii"}

        response = requests.post(
            "https://miicontestp.wii.rc24.xyz/cgi-bin/studio.cgi",
            files=files,
            data=data,
            timeout=10,
        )

        if not response.ok:
            return None

        response_data = response.json()
        mii_render_data = response_data.get("mii")

        if not mii_render_data:
            return None

        params = {
            "data": mii_render_data,
            "type": "face_only",
            "expression": "normal",
            "width": "270",
            "bgColor": "FFFFFF00",
        }

        return f"https://studio.mii.nintendo.com/miis/image.png?{urlencode(params)}"

    except Exception as e:
        print(f"Error rendering Mii: {e}")
        return None


def get_artisan_id_from_wii_number(wii_number, db_url=None):
    """Get artisan ID from a Wii number in the cmoc database."""
    if db_url is None:
        db_url = getattr(config, "cmoc_db_url", None)
    if not db_url or not wii_number:
        return None

    query = "SELECT artisan_id FROM artisans WHERE wii_number = %s LIMIT 1"
    result = _run_query(query, [wii_number], db_url)

    if result:
        return result[0].get("artisan_id")
    return None


def get_artisan_ids_from_wii_number(wii_number, db_url=None):
    """Get all artisan IDs, names, and stats from a Wii number in the cmoc database."""
    if db_url is None:
        db_url = getattr(config, "cmoc_db_url", None)
    if not db_url or not wii_number:
        return []

    query = "SELECT artisan_id, name, number_of_posts, total_likes FROM artisans WHERE wii_number = %s ORDER BY name"
    result = _run_query(query, [wii_number], db_url)
    return result if result else []


def fetch_contest_submissions(wii_numbers, db_url=None, limit=None, offset=None):
    """Fetch contest submissions (Miis) for given Wii numbers."""
    if db_url is None:
        db_url = getattr(config, "cmoc_db_url", None)
    if not db_url:
        return []

    if not wii_numbers:
        return []

    # Handle both single string and list of wii_numbers
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    # Build WHERE clause for Wii numbers
    placeholders = ",".join(["%s"] * len(wii_numbers))
    where_clause = f"cm.wii_number IN ({placeholders})"

    query = f"""
    SELECT 
        cm.contest_id,
        cm.artisan_id,
        cm.likes,
        cm.rank,
        cm.entry_id,
        cm.mii_data,
        c.english_name,
        c.status,
        c.open_time,
        c.close_time,
        c.has_special_award,
        c.has_souvenir,
        a.name
    FROM contest_miis cm
    JOIN contests c ON cm.contest_id = c.contest_id
    JOIN artisans a ON cm.artisan_id = a.artisan_id
    WHERE {where_clause}
    ORDER BY c.close_time DESC, cm.contest_id DESC
    """

    if limit is not None and offset is not None:
        query += f" LIMIT {limit} OFFSET {offset}"

    result = _run_query(query, wii_numbers, db_url)
    return result if result else []


def count_contest_submissions(wii_numbers, db_url=None):
    """Count total contest submissions for given Wii numbers."""
    if db_url is None:
        db_url = getattr(config, "cmoc_db_url", None)
    if not db_url:
        return 0

    if not wii_numbers:
        return 0

    # Handle both single string and list of wii_numbers
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    # Build WHERE clause for Wii numbers
    placeholders = ",".join(["%s"] * len(wii_numbers))
    where_clause = f"wii_number IN ({placeholders})"

    query = f"SELECT COUNT(*) AS count FROM contest_miis WHERE {where_clause}"
    result = _run_query(query, wii_numbers, db_url)
    return result[0].get("count", 0) if result else 0
