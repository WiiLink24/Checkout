from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set
from channels.nc import count_recommendations, count_time_played, fetch_user_stats
from channels.evc import count_user_polls
from channels.cmoc import count_contest_submissions, fetch_contest_submissions
from utils.utils import (
    cache,
    extract_linked_wiis,
    get_authentik_user,
    update_user_attributes,
)

_ACHIEVEMENTS_VERSION = 1
_ACHIEVEMENTS_REFRESH_HOURS = 2
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


def collect_metrics(serial_prefixes=None, wii_numbers=None, use_cache=True):

    serial_prefixes = serial_prefixes or []
    wii_numbers = wii_numbers or []

    user_stats = (
        fetch_user_stats(serial_prefixes, use_cache=use_cache)
        if serial_prefixes
        else {}
    )

    contest_wins = 0
    contest_ranks = {10: 0, 9: 0, 8: 0}
    if wii_numbers:
        for submission in fetch_contest_submissions(wii_numbers, use_cache=use_cache):
            rank = submission.get("rank")
            if str(rank) in ("10", "9", "8"):
                contest_ranks[int(rank)] += 1
            if str(rank) == "10":
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
        "contest_rank_10": contest_ranks[10],
        "contest_rank_9": contest_ranks[9],
        "contest_rank_8": contest_ranks[8],
    }


def evaluate(metrics) -> Set[str]:
    return {ach.id for ach in ACHIEVEMENTS if ach.condition(metrics)}


def _build_points(metrics, achieved_ids, previous):
    old_points = (previous or {}).get("points") or {}
    old_milestones = old_points.get("milestones")
    if not isinstance(old_milestones, dict) or old_points.get("earned", 0) == 0:
        earned = (
            metrics.get("total_minutes", 0) // 60
            + metrics.get("reviews", 0) * 5
            + metrics.get("polls", 0) * 5
            + metrics.get("contest_submissions", 0) * 10
            + metrics.get("contest_rank_10", 0) * 50
            + metrics.get("contest_rank_9", 0) * 40
            + metrics.get("contest_rank_8", 0) * 30
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
                "contest_rank_10": metrics.get("contest_rank_10", 0),
                "contest_rank_9": metrics.get("contest_rank_9", 0),
                "contest_rank_8": metrics.get("contest_rank_8", 0),
                "achievements": list(achieved_ids),
            },
        }

    previous_achievements = set(old_milestones.get("achievements", []))
    new_achievements = set(achieved_ids) - previous_achievements
    live_minutes = metrics.get("total_minutes", 0)
    old_minutes = old_milestones.get("total_minutes", 0)
    play_minutes = max(0, live_minutes - old_minutes)
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
        for rank, points in ((10, 50), (9, 40), (8, 30))
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
            # Live totals can drop below the stored milestone in certain scenarios.
            "total_minutes": (
                live_minutes
                if live_minutes < old_minutes
                else old_minutes + (play_minutes // 60) * 60
            ),
            "reviews": metrics.get("reviews", 0),
            "polls": metrics.get("polls", 0),
            "contest_submissions": metrics.get("contest_submissions", 0),
            "contest_rank_10": metrics.get("contest_rank_10", 0),
            "contest_rank_9": metrics.get("contest_rank_9", 0),
            "contest_rank_8": metrics.get("contest_rank_8", 0),
            "achievements": list(previous_achievements | set(achieved_ids)),
        },
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
    return extract_linked_wiis(attributes)


def is_fresh(payload) -> bool:
    """True if the payload was generated within the refresh window."""
    if not isinstance(payload, dict):
        return False
    try:
        generated_at = datetime.fromisoformat(payload.get("generated_at"))
    except (TypeError, ValueError):
        return False
    return datetime.now() - generated_at < timedelta(hours=_ACHIEVEMENTS_REFRESH_HOURS)

def _build_refresh_payload(achieved_ids, metrics=None, previous=None) -> Dict:
    metrics = metrics or {}
    previous = previous or {}
    points = _build_points(metrics, achieved_ids, previous)
    themes = previous.get("themes") or {"unlocked": [], "active": None}
    return {
        "version": _ACHIEVEMENTS_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "points": points,
        "themes": themes,
        "achievements": [
            {
                "id": ach.id,
                "name": ach.name,
                "description": ach.description,
                "icon": ach.icon,
                "achieved": ach.id in achieved_ids,
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