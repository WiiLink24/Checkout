import config
from utils.utils import (
    _execute,
    _run_query,
    find_user_by_wii_number,
    format_serial,
    generate_gravatar_url,
)


def _db_url(db_url=None):
    return db_url or getattr(config, "checkout_db_url", None)


def follow(follower_wii_number, followed_wii_number, db_url=None):
    """Record that follower_wii_number follows followed_wii_number."""
    db_url = _db_url(db_url)
    if not db_url or not follower_wii_number or not followed_wii_number:
        return False
    return (
        _execute(
            """
            INSERT INTO friends (follower_wii_number, followed_wii_number)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            [follower_wii_number, followed_wii_number],
            db_url,
        )
        == 1
    )


def unfollow(follower_wii_number, followed_wii_number, db_url=None):
    """Remove a follow, returns True if a row was deleted."""
    db_url = _db_url(db_url)
    if not db_url or not follower_wii_number or not followed_wii_number:
        return False
    return (
        _execute(
            """
            DELETE FROM friends
            WHERE follower_wii_number = %s AND followed_wii_number = %s
            """,
            [follower_wii_number, followed_wii_number],
            db_url,
        )
        > 0
    )


def is_following(follower_wii_number, followed_wii_number, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not follower_wii_number or not followed_wii_number:
        return False
    rows = _run_query(
        """
        SELECT 1 FROM friends
        WHERE follower_wii_number = %s AND followed_wii_number = %s
        LIMIT 1
        """,
        [follower_wii_number, followed_wii_number],
        db_url,
        use_cache=False,
    )
    return bool(rows)


def fetch_following(wii_number, db_url=None):
    """Wii numbers this wii follows, most recent first."""
    db_url = _db_url(db_url)
    if not db_url or not wii_number:
        return []
    rows = _run_query(
        """
        SELECT followed_wii_number AS code
        FROM friends
        WHERE follower_wii_number = %s
        ORDER BY created_at DESC
        """,
        [wii_number],
        db_url,
    )
    return [row["code"] for row in rows]


def fetch_followers(wii_number, db_url=None):
    """Wii numbers following this wii, most recent first."""
    db_url = _db_url(db_url)
    if not db_url or not wii_number:
        return []
    rows = _run_query(
        """
        SELECT follower_wii_number AS code
        FROM friends
        WHERE followed_wii_number = %s
        ORDER BY created_at DESC
        """,
        [wii_number],
        db_url,
    )
    return [row["code"] for row in rows]


def count_following(wii_number, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not wii_number:
        return 0
    rows = _run_query(
        "SELECT COUNT(*) AS count FROM friends WHERE follower_wii_number = %s",
        [wii_number],
        db_url,
    )
    return rows[0]["count"] if rows else 0


def count_followers(wii_number, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not wii_number:
        return 0
    rows = _run_query(
        "SELECT COUNT(*) AS count FROM friends WHERE followed_wii_number = %s",
        [wii_number],
        db_url,
    )
    return rows[0]["count"] if rows else 0


def resolve_user_cards(codes):
    """Resolve wii numbers into profile cards."""
    cards = []
    for code in codes:
        user = find_user_by_wii_number(code)
        if not user:
            continue
        cards.append(
            {
                "username": user.get("username", "Unknown"),
                "avatar": generate_gravatar_url(user.get("email", "")),
                "wii_number": code,
                "code": format_serial(code),
                "profile_url": f"/{code}/",
            }
        )
    return cards
