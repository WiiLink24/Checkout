from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set
from utils.utils import (
    cache,
    fetch_all_authentik_users,
    fetch_authentik_user_by_username,
    get_authentik_user,
    update_user_attributes,
)

_ACHIEVEMENTS_VERSION = 1
_ACHIEVEMENTS_REFRESH_HOURS = 2
_GLOBAL_TALLY_TTL = 24 * 60 * 60
_ACHIEVEMENT_POINTS = 50


@dataclass(frozen=True)
class Achievement:
    id: str
    name: str
    description: str
    icon: str
    condition: Callable[[Dict], bool]


ACHIEVEMENTS = [
    Achievement(
        "first_review",
        "First Review",
        "Leave your first recommendation",
        "achievement_recommendations_1",
        lambda m: m["reviews"] >= 1,
    ),
    Achievement(
        "review_veteran",
        "Review Veteran",
        "Leave 25 recommendations",
        "achievement_recommendations_2",
        lambda m: m["reviews"] >= 25,
    ),
    Achievement(
        "review_master",
        "Review Master",
        "Leave 100 recommendations",
        "achievement_recommendations_3",
        lambda m: m["reviews"] >= 100,
    ),
    Achievement(
        "first_play",
        "First Play",
        "Play your first game",
        "achievement_playtime_1",
        lambda m: m["games_played"] >= 1,
    ),
    Achievement(
        "dedicated_player",
        "Dedicated Player",
        "Play 50 different games",
        "achievement_playtime_2",
        lambda m: m["games_played"] >= 50,
    ),
    Achievement(
        "marathoner",
        "Marathoner",
        "Log 1000 hours of playtime",
        "achievement_playtime_3",
        lambda m: m["total_minutes"] >= 60000,
    ),
    Achievement(
        "first_poll",
        "First Poll",
        "Vote in your first poll",
        "achievement_polls_1",
        lambda m: m["polls"] >= 1,
    ),
    Achievement(
        "poll_addict",
        "Poll Addict",
        "Vote in 50 polls",
        "achievement_polls_2",
        lambda m: m["polls"] >= 50,
    ),
    Achievement(
        "first_contest",
        "First Contest Entry",
        "Enter your first contest",
        "achievement_contests_1",
        lambda m: m["contest_submissions"] >= 1,
    ),
    Achievement(
        "contest_regular",
        "Contest Regular",
        "Enter 10 contests",
        "achievement_contests_2",
        lambda m: m["contest_submissions"] >= 10,
    ),
]

_ACHIEVEMENT_BY_ID = {ach.id: ach for ach in ACHIEVEMENTS}


def collect_metrics(serial_prefixes=None, wii_numbers=None, use_cache=True):
    from channels.nc import count_recommendations, count_time_played, fetch_user_stats
    from channels.evc import count_user_polls
    from channels.cmoc import count_contest_submissions

    serial_prefixes = serial_prefixes or []
    wii_numbers = wii_numbers or []

    user_stats = (
        fetch_user_stats(serial_prefixes, use_cache=use_cache)
        if serial_prefixes
        else {}
    )

    contest_wins = 0
    contest_ranks = {1: 0, 2: 0, 3: 0}
    if wii_numbers:
        from channels.cmoc import fetch_contest_submissions

        for submission in fetch_contest_submissions(wii_numbers, use_cache=use_cache):
            rank = submission.get("rank")
            if str(rank) in ("1", "2", "3"):
                contest_ranks[int(rank)] += 1
            if str(rank) == "1":
                contest_wins += 1

    return {
        "reviews": (
            count_recommendations(serial_prefixes, use_cache=use_cache)
            if serial_prefixes
            else 0
        ),
        "games_played": (
            count_time_played(serial_prefixes, use_cache=use_cache)
            if serial_prefixes
            else 0
        ),
        "total_minutes": (
            (user_stats or {}).get("total_minutes", 0) if serial_prefixes else 0
        ),
        "polls": (
            count_user_polls(wii_numbers, use_cache=use_cache) if wii_numbers else 0
        ),
        "contest_submissions": (
            count_contest_submissions(wii_numbers, use_cache=use_cache)
            if wii_numbers
            else 0
        ),
        "contest_wins": contest_wins,
        "contest_rank_1": contest_ranks[1],
        "contest_rank_2": contest_ranks[2],
        "contest_rank_3": contest_ranks[3],
    }


def evaluate(metrics) -> Set[str]:
    return {ach.id for ach in ACHIEVEMENTS if ach.condition(metrics)}


def _achievement_ids(payload):
    return {
        item.get("id")
        for item in (payload or {}).get("achievements", [])
        if item.get("achieved")
    }


def _build_points(metrics, achieved_ids, previous):
    old_points = (previous or {}).get("points") or {}
    old_milestones = old_points.get("milestones")
    if not isinstance(old_milestones, dict) or old_points.get("earned", 0) == 0:
        earned = (
            metrics.get("total_minutes", 0) // 60
            + metrics.get("reviews", 0) * 5
            + metrics.get("polls", 0) * 5
            + metrics.get("contest_submissions", 0) * 10
            + metrics.get("contest_rank_1", 0) * 50
            + metrics.get("contest_rank_2", 0) * 40
            + metrics.get("contest_rank_3", 0) * 30
            + len(achieved_ids) * _ACHIEVEMENT_POINTS
        )
        return {
            "earned": earned,
            "spent": old_points.get("spent", 0),
            "balance": max(0, earned - old_points.get("spent", 0)),
            "milestones": {
                "total_minutes": metrics.get("total_minutes", 0),
                "reviews": metrics.get("reviews", 0),
                "polls": metrics.get("polls", 0),
                "contest_submissions": metrics.get("contest_submissions", 0),
                "contest_rank_1": metrics.get("contest_rank_1", 0),
                "contest_rank_2": metrics.get("contest_rank_2", 0),
                "contest_rank_3": metrics.get("contest_rank_3", 0),
                "achievements": list(achieved_ids),
            },
        }

    previous_achievements = set(old_milestones.get("achievements", []))
    new_achievements = set(achieved_ids) - previous_achievements
    play_minutes = max(
        0, metrics.get("total_minutes", 0) - old_milestones.get("total_minutes", 0)
    )
    new_reviews = max(0, metrics.get("reviews", 0) - old_milestones.get("reviews", 0))
    new_polls = max(0, metrics.get("polls", 0) - old_milestones.get("polls", 0))
    new_contests = max(
        0,
        metrics.get("contest_submissions", 0)
        - old_milestones.get("contest_submissions", 0),
    )
    rank_points = sum(
        max(
            0,
            metrics.get(f"contest_rank_{rank}", 0)
            - old_milestones.get(f"contest_rank_{rank}", 0),
        )
        * points
        for rank, points in ((1, 50), (2, 40), (3, 30))
    )
    earned = old_points.get("earned", 0) + (
        (play_minutes // 60)
        + new_reviews * 5
        + new_polls * 5
        + new_contests * 10
        + rank_points
        + len(new_achievements) * _ACHIEVEMENT_POINTS
    )
    spent = old_points.get("spent", 0)
    return {
        "earned": earned,
        "spent": spent,
        "balance": max(0, earned - spent),
        "milestones": {
            **old_milestones,
            "total_minutes": old_milestones.get("total_minutes", 0)
            + (play_minutes // 60) * 60,
            "reviews": metrics.get("reviews", 0),
            "polls": metrics.get("polls", 0),
            "contest_submissions": metrics.get("contest_submissions", 0),
            "contest_rank_1": metrics.get("contest_rank_1", 0),
            "contest_rank_2": metrics.get("contest_rank_2", 0),
            "contest_rank_3": metrics.get("contest_rank_3", 0),
            "achievements": list(previous_achievements | set(achieved_ids)),
        },
    }


def build_payload(
    achieved_ids, achievement_counts, total_users, metrics=None, previous=None
) -> Dict:
    """Build the JSON payload stored in the user's Authentik attributes."""

    def percent(count):
        return round(count / total_users * 100, 1) if total_users else 0.0

    metrics = metrics or {}
    previous = previous or {}
    points = _build_points(metrics, achieved_ids, previous)
    themes = previous.get("themes") or {"unlocked": [], "active": None}
    return {
        "version": _ACHIEVEMENTS_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_users": total_users,
        "points": points,
        "themes": themes,
        "achievements": [
            {
                "id": ach.id,
                "name": ach.name,
                "description": ach.description,
                "icon": ach.icon,
                "achieved": ach.id in achieved_ids,
                "percent": percent(achievement_counts.get(ach.id, 0)),
            }
            for ach in ACHIEVEMENTS
        ],
    }


def parse_achievements(attributes) -> Optional[Dict]:
    if not isinstance(attributes, dict):
        return None
    payload = attributes.get("achievements")
    if not isinstance(payload, dict) or payload.get("version") != _ACHIEVEMENTS_VERSION:
        return None
    return payload


def _extract_user_identifiers(attributes):
    """Extract serial prefixes and wii numbers from a user's attributes."""
    serial_prefixes, wii_numbers = [], []
    wiis = (attributes or {}).get("wiis")
    if isinstance(wiis, list):
        for wii in wiis:
            if not isinstance(wii, dict):
                continue
            serial = wii.get("serial_number")
            if serial:
                serial_prefixes.append(serial[:12])
            wii_number = wii.get("wii_number")
            if wii_number:
                wii_numbers.append(wii_number)
    return serial_prefixes, wii_numbers


def is_fresh(payload) -> bool:
    """True if the payload was generated within the refresh window."""
    if not isinstance(payload, dict):
        return False
    try:
        generated_at = datetime.fromisoformat(payload.get("generated_at"))
    except (TypeError, ValueError):
        return False
    return datetime.now() - generated_at < timedelta(hours=_ACHIEVEMENTS_REFRESH_HOURS)


def _get_global_tally() -> Dict:
    """Per-achievement unlock counts across all linked-Wii users (cached 24h).

    This is the single source of truth for percentages, so every refresh in the
    same window shares identical numbers and they can never diverge per user.
    """
    cache_key = "achievements:global_tally:v1"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    counts = Counter()
    eligible = 0
    try:
        for user in fetch_all_authentik_users():
            serial_prefixes, wii_numbers = _extract_user_identifiers(
                (user.get("attributes") or {})
            )
            if not serial_prefixes and not wii_numbers:
                continue
            eligible += 1
            try:
                achieved = evaluate(collect_metrics(serial_prefixes, wii_numbers))
            except Exception as e:
                print(f"[ACHIEVEMENTS] Tally error for {user.get('username')}: {e}")
                continue
            for ach_id in achieved:
                counts[ach_id] += 1
    except Exception as e:
        print(f"[ACHIEVEMENTS] Tally computation failed: {e}")

    tally = {"counts": dict(counts), "eligible": eligible}
    # Only cache a meaningful tally so a failed/empty sweep isn't frozen for 24h
    if eligible:
        cache.set(cache_key, tally, timeout=_GLOBAL_TALLY_TTL)
    return tally


def _percentages(tally) -> Dict:
    """Percentage per achievement from the global tally."""
    eligible = tally["eligible"]
    return {
        ach_id: round(count / eligible * 100, 1) if eligible else 0.0
        for ach_id, count in tally["counts"].items()
    }


def _build_refresh_payload(achieved_ids, metrics=None, previous=None) -> Dict:
    tally = _get_global_tally()
    pcts = _percentages(tally)
    metrics = metrics or {}
    previous = previous or {}
    points = _build_points(metrics, achieved_ids, previous)
    themes = previous.get("themes") or {"unlocked": [], "active": None}
    return {
        "version": _ACHIEVEMENTS_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_users": tally["eligible"],
        "points": points,
        "themes": themes,
        "achievements": [
            {
                "id": ach.id,
                "name": ach.name,
                "description": ach.description,
                "icon": ach.icon,
                "achieved": ach.id in achieved_ids,
                "percent": pcts.get(ach.id, 0.0),
            }
            for ach in ACHIEVEMENTS
        ],
    }


def refresh_achievements_for_user(user, force=False):
    """Refresh one user's achievements when stale, or immediately when forced.

    Returns (payload, wrote): the payload to display, and whether a write happened.
    """
    print(
        f"[ACHIEVEMENTS] Refreshing payload for user {user.get('username')} ({user.get('uuid')})"
    )
    try:
        fresh_user = get_authentik_user(user)
    except Exception as e:
        print(f"[ACHIEVEMENTS] Could not fetch {user.get('username')}: {e}")
        return parse_achievements((user.get("attributes") or {})), False

    attributes = (fresh_user or {}).get("attributes") or {}
    previous = parse_achievements(attributes)
    if (
        previous
        and not force
        and is_fresh(previous)
        and "points" in previous
        and "themes" in previous
    ):
        return previous, False

    serial_prefixes, wii_numbers = _extract_user_identifiers(attributes)
    if not serial_prefixes and not wii_numbers:
        return previous, False

    try:
        metrics = collect_metrics(serial_prefixes, wii_numbers, use_cache=not force)
        achieved = evaluate(metrics)
    except Exception as e:
        print(f"[ACHIEVEMENTS] Refresh failed for {user.get('username')}: {e}")
        return previous, False

    payload = _build_refresh_payload(achieved, metrics, previous)
    if previous:
        current_data = {
            key: value for key, value in payload.items() if key != "generated_at"
        }
        previous_data = {
            key: value for key, value in previous.items() if key != "generated_at"
        }
        if current_data == previous_data:
            return previous, False

    try:
        attributes["achievements"] = payload
        update_user_attributes(user, attributes)
        return payload, True
    except Exception as e:
        print(f"[ACHIEVEMENTS] Failed to update {user.get('username')}: {e}")
        return payload, False


def sync_achievements():
    import os

    import config

    restricted_username = os.environ.get("ACHIEVEMENTS_SYNC_USERNAME") or getattr(
        config, "achievements_sync_username", None
    )

    if restricted_username:
        user = fetch_authentik_user_by_username(restricted_username)
        users = [user] if user else []
        print(f"[ACHIEVEMENTS] Test mode: syncing only {restricted_username}")
    else:
        users = fetch_all_authentik_users()

    achieved_by_user = {}
    metrics_by_user = {}
    achievement_counts = Counter()
    eligible = 0

    for user in users:
        uuid = user.get("uuid")
        if not uuid:
            continue

        serial_prefixes, wii_numbers = _extract_user_identifiers(
            (user.get("attributes") or {})
        )

        if not serial_prefixes and not wii_numbers:
            continue

        eligible += 1
        try:
            metrics = collect_metrics(serial_prefixes, wii_numbers)
            achieved = evaluate(metrics)
        except Exception as e:
            print(f"[ACHIEVEMENTS] Error for {user.get('username')}: {e}")
            continue

        achieved_by_user[uuid] = achieved
        metrics_by_user[uuid] = metrics
        for ach_id in achieved:
            achievement_counts[ach_id] += 1

    if not eligible:
        print("[ACHIEVEMENTS] No eligible users")
        return

    updated = 0
    for user in users:
        uuid = user.get("uuid")
        if uuid not in achieved_by_user:
            continue
        try:
            fresh_user = get_authentik_user(user)
        except Exception as e:
            print(f"[ACHIEVEMENTS] Could not re-fetch {user.get('username')}: {e}")
            continue

        fresh_attributes = (fresh_user or {}).get("attributes") or {}
        if not isinstance(fresh_attributes, dict) or not fresh_attributes.get("wiis"):
            print(
                f"[ACHIEVEMENTS] Skipping {user.get('username')}: no linked Wiis anymore"
            )
            continue

        attributes = dict(fresh_attributes)
        previous = parse_achievements(fresh_attributes)
        attributes["achievements"] = build_payload(
            achieved_by_user[uuid],
            achievement_counts,
            eligible,
            metrics_by_user[uuid],
            previous,
        )
        try:
            update_user_attributes(user, attributes)
            updated += 1
        except Exception as e:
            print(f"[ACHIEVEMENTS] Failed to update {user.get('username')}: {e}")

    print(
        f"[ACHIEVEMENTS] Synced {updated}/{len(achieved_by_user)} users "
        f"({eligible} eligible, {sum(achievement_counts.values())} total unlocks)"
    )
