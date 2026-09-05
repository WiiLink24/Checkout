import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests

import config
from utils.utils import _run_query, cache

logger = logging.getLogger(__name__)

CMOC_CONTESTS_URL = "https://miicontest.wiilink.ca/api/contests"
CMOC_CONTEST_THUMB = "https://mcc-panel.wiilink.ca//assets/contest/{}/thumbnail.jpg"
CMOC_CONTEST_URL = "https://miicontest.wiilink.ca/"
EVC_POLLS_URL = "https://evc.wiilink.ca/api/polls"
NEWS_RSS_URL = "https://wiilink.ca/rss.xml"

REQUEST_TIMEOUT = 10
CACHE_TIMEOUT = 60 * 60
OPEN_DAYS = 7


def _parse_dt(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def fetch_open_contests():
    cached = cache.get("whats_new:contests")
    if cached is not None:
        return cached

    open_contests = []
    try:
        response = requests.get(CMOC_CONTESTS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        now = datetime.now(timezone.utc)
        for contest in response.json():
            opened = _parse_dt(contest.get("open_time"))
            if opened is None or not opened <= now < opened + timedelta(days=OPEN_DAYS):
                continue
            open_contests.append(
                {
                    "contest_id": contest["contest_id"],
                    "name": contest.get("english_name") or "",
                    "thumbnail": CMOC_CONTEST_THUMB.format(contest["contest_id"]),
                    "url": CMOC_CONTEST_URL,
                }
            )
        open_contests.sort(key=lambda c: c["contest_id"], reverse=True)
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        logger.warning("Failed to fetch CMOC contests: %s", e)
        return []
    cache.set("whats_new:contests", open_contests, timeout=CACHE_TIMEOUT)
    return open_contests


def fetch_open_polls():
    cached = cache.get("whats_new:open_polls")
    if cached is not None:
        return cached

    latest, open_count = None, 0
    try:
        response = requests.get(
            EVC_POLLS_URL,
            params={"page": 1, "limit": 20, "type": "all", "language": "english"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        now = datetime.now(timezone.utc)
        for poll in response.json().get("data") or []:
            published = _parse_dt(poll.get("date"))
            if published is None or not published <= now < published + timedelta(
                days=OPEN_DAYS
            ):
                continue
            open_count += 1
            if latest is None:
                latest = poll
    except (requests.RequestException, ValueError) as e:
        logger.warning("Failed to fetch EVC polls: %s", e)
        return None, 0
    result = (latest, open_count)
    cache.set("whats_new:open_polls", result, timeout=CACHE_TIMEOUT)
    return result


def fetch_latest_poll():
    return fetch_open_polls()[0]


def _first_image(html):
    if not html:
        return ""
    match = re.search(r'<img[^>]+src="([^"]+)"', html)
    return match.group(1) if match else ""


def fetch_latest_news():
    cached = cache.get("whats_new:news")
    if cached is not None:
        return cached

    news = None
    try:
        response = requests.get(NEWS_RSS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        channel = ET.fromstring(response.content).find("channel")
        item = channel.find("item") if channel is not None else None
        if item is not None:
            encoded = item.find("{http://purl.org/rss/1.0/modules/content/}encoded")
            news = {
                "title": (item.findtext("title") or "").strip(),
                "link": (item.findtext("link") or "").strip(),
                "date": (item.findtext("pubDate") or "").strip(),
                "description": (item.findtext("description") or "").strip(),
                "image": _first_image(encoded.text if encoded is not None else ""),
            }
    except (requests.RequestException, ET.ParseError) as e:
        logger.warning("Failed to fetch WiiLink news: %s", e)
        return None
    cache.set("whats_new:news", news, timeout=CACHE_TIMEOUT)
    return news


def fetch_latest_banners(limit=3):
    try:
        rows = (
            _run_query(
                "SELECT id, name_english FROM banners ORDER BY id DESC LIMIT %s",
                [limit],
                config.db_url,
            )
            or []
        )
    except Exception as e:
        logger.warning("Failed to fetch Nintendo Channel banners: %s", e)
        return []
    return [
        {
            "id": row["id"],
            "name": row.get("name_english") or "",
        }
        for row in rows
    ]
