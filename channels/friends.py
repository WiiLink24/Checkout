from sqlalchemy import func
from utils.db import Friend, db
from utils.utils import (
    find_user_by_wii_number,
    format_serial,
    generate_gravatar_url,
)


def follow(follower_wii_number, followed_wii_number):
    """Record that follower_wii_number follows followed_wii_number."""
    if not follower_wii_number or not followed_wii_number:
        return False
    if (
        db.session.execute(
            db.select(Friend.follower_wii_number).filter(
                Friend.follower_wii_number == follower_wii_number,
                Friend.followed_wii_number == followed_wii_number,
            )
        ).first()
        is not None
    ):
        return False
    db.session.add(
        Friend(
            follower_wii_number=follower_wii_number,
            followed_wii_number=followed_wii_number,
        )
    )
    db.session.commit()
    return True


def unfollow(follower_wii_number, followed_wii_number):
    """Remove a follow; returns True if a row was deleted."""
    if not follower_wii_number or not followed_wii_number:
        return False
    deleted = db.session.execute(
        db.delete(Friend).filter(
            Friend.follower_wii_number == follower_wii_number,
            Friend.followed_wii_number == followed_wii_number,
        )
    )
    db.session.commit()
    return deleted.rowcount > 0


def is_following(follower_wii_number, followed_wii_number):
    if not follower_wii_number or not followed_wii_number:
        return False
    return (
        db.session.execute(
            db.select(Friend.follower_wii_number).filter(
                Friend.follower_wii_number == follower_wii_number,
                Friend.followed_wii_number == followed_wii_number,
            )
        ).first()
        is not None
    )


def fetch_following(wii_number):
    """Wii numbers this wii follows, most recent first."""
    if not wii_number:
        return []
    return (
        db.session.execute(
            db.select(Friend.followed_wii_number)
            .filter(Friend.follower_wii_number == wii_number)
            .order_by(Friend.created_at.desc())
        )
        .scalars()
        .all()
    )


def fetch_followers(wii_number):
    """Wii numbers following this wii, most recent first."""
    if not wii_number:
        return []
    return (
        db.session.execute(
            db.select(Friend.follower_wii_number)
            .filter(Friend.followed_wii_number == wii_number)
            .order_by(Friend.created_at.desc())
        )
        .scalars()
        .all()
    )


def count_following(wii_number):
    if not wii_number:
        return 0
    return (
        db.session.execute(
            db.select(func.count())
            .select_from(Friend)
            .filter(Friend.follower_wii_number == wii_number)
        ).scalar()
        or 0
    )


def count_followers(wii_number):
    if not wii_number:
        return 0
    return (
        db.session.execute(
            db.select(func.count())
            .select_from(Friend)
            .filter(Friend.followed_wii_number == wii_number)
        ).scalar()
        or 0
    )


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
