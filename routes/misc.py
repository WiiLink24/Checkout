import hmac
from flask import Blueprint, jsonify, render_template, request, redirect, url_for
from urllib.parse import urlparse
import random
from sqlalchemy import func
from utils.db import PushSubscription, db
from utils.push import CATEGORIES, send_push_to_all, send_push_to_user


import config
from utils.utils import (
    search_authentik_users_by_name,
    fetch_authentik_users,
    _run_query,
)

MAX_SUBSCRIPTIONS_PER_USER = 10

misc_routes_bp = Blueprint("misc_routes", __name__, url_prefix="")


def get_logged_in_user_info():
    from app import get_logged_in_user_info as get_user

    return get_user()


def get_oidc():
    from app import oidc

    return oidc


def search_games_by_title(search_query, offset=0, limit=15):
    """Search games by title or game_id with pagination"""
    query = """
        SELECT t.game_id, t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer,
               t.publisher, t.rating_type, t.rating_value, t.release_year, t.release_month, t.release_day,
             t.region, t.input_controls, t.wifi_players,
               COALESCE(b.favorite_count, 0) AS favorite_count,
               COALESCE(b.user_count, 0) AS user_count
        FROM titles t
        LEFT JOIN (
            SELECT game_id, COUNT(*) AS favorite_count, COUNT(DISTINCT serial_number) AS user_count
            FROM bookmarks
            GROUP BY game_id
        ) b ON t.game_id LIKE b.game_id || '%%'
        WHERE LOWER(t.title_en) LIKE %s OR LOWER(t.game_id) LIKE %s
        ORDER BY favorite_count DESC
        LIMIT %s OFFSET %s
    """
    search_param = f"%{search_query}%"
    return (
        _run_query(query, [search_param, search_param, limit, offset], config.db_url)
        or []
    )


def count_games_by_title(search_query):
    """Count total games matching title search"""
    query = """
        SELECT COUNT(*) AS total
        FROM titles t
        WHERE LOWER(t.title_en) LIKE %s OR LOWER(t.game_id) LIKE %s
    """
    search_param = f"%{search_query}%"
    result = _run_query(query, [search_param, search_param], config.db_url)
    return result[0].get("total", 0) if result else 0


def search_games_by_publisher(search_query, offset=0, limit=15):
    """Search games by publisher with pagination"""
    query = """
        SELECT t.game_id, t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer,
               t.publisher, t.rating_type, t.rating_value, t.release_year, t.release_month, t.release_day,
             t.region, t.input_controls, t.wifi_players, t.input_players,
               COALESCE(b.favorite_count, 0) AS favorite_count,
               COALESCE(b.user_count, 0) AS user_count
        FROM titles t
        LEFT JOIN (
            SELECT game_id, COUNT(*) AS favorite_count, COUNT(DISTINCT serial_number) AS user_count
            FROM bookmarks
            GROUP BY game_id
        ) b ON t.game_id LIKE b.game_id || '%%'
        WHERE LOWER(t.publisher) LIKE %s
        ORDER BY favorite_count DESC
        LIMIT %s OFFSET %s
    """
    search_param = f"%{search_query}%"
    return _run_query(query, [search_param, limit, offset], config.db_url) or []


def count_games_by_publisher(search_query):
    """Count total games matching publisher search"""
    query = """
        SELECT COUNT(*) AS total
        FROM titles t
        WHERE LOWER(t.publisher) LIKE %s
    """
    search_param = f"%{search_query}%"
    result = _run_query(query, [search_param], config.db_url)
    return result[0].get("total", 0) if result else 0


def search_games_by_developer(search_query, offset=0, limit=15):
    """Search games by developer with pagination"""
    query = """
        SELECT t.game_id, t.title_en, t.display_name, t.synopsis_en, t.genre, t.developer,
               t.publisher, t.rating_type, t.rating_value, t.release_year, t.release_month, t.release_day,
             t.region, t.input_controls, t.wifi_players, t.input_players,
               COALESCE(b.favorite_count, 0) AS favorite_count,
               COALESCE(b.user_count, 0) AS user_count
        FROM titles t
        LEFT JOIN (
            SELECT game_id, COUNT(*) AS favorite_count, COUNT(DISTINCT serial_number) AS user_count
            FROM bookmarks
            GROUP BY game_id
        ) b ON t.game_id LIKE b.game_id || '%%'
        WHERE LOWER(t.developer) LIKE %s
        ORDER BY favorite_count DESC
        LIMIT %s OFFSET %s
    """
    search_param = f"%{search_query}%"
    return _run_query(query, [search_param, limit, offset], config.db_url) or []


def count_games_by_developer(search_query):
    """Count total games matching developer search"""
    query = """
        SELECT COUNT(*) AS total
        FROM titles t
        WHERE LOWER(t.developer) LIKE %s
    """
    search_param = f"%{search_query}%"
    result = _run_query(query, [search_param], config.db_url)
    return result[0].get("total", 0) if result else 0


@misc_routes_bp.route("/search", endpoint="search")
def search():
    user_info = get_logged_in_user_info()
    search_query = request.args.get("search", "").strip().lower()
    search_type = request.args.get("type", "users").strip().lower()
    page = request.args.get("page", 1, type=int)
    limit = 15
    offset = (page - 1) * limit

    users = []
    games = []
    total_count = 0
    total_pages = 0

    if search_query:
        if search_type == "title":
            total_count = count_games_by_title(search_query)
            games = search_games_by_title(search_query, offset, limit)
        elif search_type == "publisher":
            total_count = count_games_by_publisher(search_query)
            games = search_games_by_publisher(search_query, offset, limit)
        elif search_type == "developer":
            total_count = count_games_by_developer(search_query)
            games = search_games_by_developer(search_query, offset, limit)
        else:  # Default to users
            users = search_authentik_users_by_name(search_query)
            users = [
                user
                for user in users
                if search_query in user.get("username", "").lower()
                or any(
                    search_query in wii.get("wii_number", "").lower()
                    or search_query in wii.get("serial_number", "").lower()
                    for wii in user.get("attributes", {}).get("wiis", [])
                    if isinstance(wii, dict)
                )
            ]
            # Paginate users
            total_count = len(users)
            users = users[offset : offset + limit]

    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 0
    random.shuffle(users)
    return render_template(
        "search.html",
        users=users,
        games=games,
        search_query=search_query,
        search_type=search_type,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        user_info=user_info,
    )


def _shared_secret_authorized():
    auth = request.headers.get("Authorization", "")
    expected = f"Bearer {config.notifications_shared_secret}"
    return bool(config.notifications_shared_secret) and hmac.compare_digest(
        auth, expected
    )


@misc_routes_bp.route(
    "/notifications/test", methods=["POST"], endpoint="test_notification"
)
def test_notification():
    if not _shared_secret_authorized():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username is required"}), 400

    sent = send_push_to_user(
        username,
        title="Checkout Test Notification",
        body="Push notifications are working!",
        url="/",
    )
    return jsonify({"sent": sent})


@misc_routes_bp.route(
    "/api/notifications/send", methods=["POST"], endpoint="send_notification"
)
def send_notification():
    """
    Body: {"username": "...", "category": "...", "title": "...", "body": "...",
           "url": "...", "tag": "...", "icon": "..."}"""
    if not _shared_secret_authorized():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"error": "title is required"}), 400

    category = (data.get("category") or "").strip() or None
    if category and category not in CATEGORIES:
        return jsonify({"error": "unknown category"}), 400

    username = (data.get("username") or "").strip()
    if username:
        sent = send_push_to_user(
            username,
            title=title,
            body=data.get("body", ""),
            url=data.get("url", "/"),
            tag=data.get("tag"),
            category=category,
            icon=data.get("icon"),
        )
    else:
        sent = send_push_to_all(
            title=title,
            body=data.get("body", ""),
            url=data.get("url", "/"),
            tag=data.get("tag"),
            category=category,
            icon=data.get("icon"),
        )
    return jsonify({"sent": sent})


def _safe_return_target():
    target = request.args.get("return", "/")
    if target.startswith("/") and not target.startswith("//"):
        return target

    trusted_origin = _accountmanager_origin()
    if trusted_origin:
        parsed = urlparse(target)
        if parsed.scheme in ("http", "https") and (
            f"{parsed.scheme}://{parsed.netloc}" == trusted_origin
        ):
            return target
    return "/"


def _accountmanager_origin():
    parsed = urlparse(config.accountmanager_url or "")
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return None


@misc_routes_bp.route("/notifications/enable", endpoint="enable_notifications")
def enable_notifications():
    user_info = get_logged_in_user_info()
    if not user_info:
        return redirect(url_for("oidc_auth.login"))

    configured = bool(config.vapid_public_key)
    return render_template(
        "notifications_enable.html",
        public_key=config.vapid_public_key,
        return_url=_safe_return_target(),
        status_message=(
            None
            if configured
            else "Notifications are not configured on this server yet."
        ),
    )


@misc_routes_bp.route("/notifications/disable", endpoint="disable_notifications")
def disable_notifications():
    user_info = get_logged_in_user_info()
    if not user_info:
        return redirect(url_for("oidc_auth.login"))
    return render_template(
        "notifications_disable.html", return_url=_safe_return_target()
    )


@misc_routes_bp.route(
    "/notifications/subscribe", methods=["POST"], endpoint="subscribe_notification"
)
def subscribe_notification():
    user_info = get_logged_in_user_info()
    username = (user_info or {}).get("username", "")
    if not username:
        return jsonify({"error": "Not logged in"}), 401

    data = request.get_json(silent=True) or {}
    endpoint = (data.get("endpoint") or "").strip()
    keys = data.get("keys") or {}
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        return jsonify({"error": "Invalid subscription"}), 400

    existing = db.session.get(PushSubscription, endpoint)
    if existing:
        existing.username = username
        existing.p256dh = p256dh
        existing.auth = auth
    else:
        count = db.session.execute(
            db.select(func.count())
            .select_from(PushSubscription)
            .where(PushSubscription.username == username)
        ).scalar()
        if count >= MAX_SUBSCRIPTIONS_PER_USER:
            oldest = (
                db.session.execute(
                    db.select(PushSubscription)
                    .where(PushSubscription.username == username)
                    .order_by(PushSubscription.created_at)
                    .limit(count - MAX_SUBSCRIPTIONS_PER_USER + 1)
                )
                .scalars()
                .all()
            )
            for row in oldest:
                db.session.delete(row)
        db.session.add(
            PushSubscription(
                endpoint=endpoint, username=username, p256dh=p256dh, auth=auth
            )
        )
    db.session.commit()
    return jsonify({"ok": True})


@misc_routes_bp.route(
    "/notifications/unsubscribe", methods=["POST"], endpoint="unsubscribe_notification"
)
def unsubscribe_notification():
    user_info = get_logged_in_user_info()
    username = (user_info or {}).get("username", "")
    if not username:
        return jsonify({"error": "Not logged in"}), 401

    data = request.get_json(silent=True) or {}
    endpoint = (data.get("endpoint") or "").strip()
    if not endpoint:
        return jsonify({"error": "Invalid subscription"}), 400

    db.session.execute(
        db.delete(PushSubscription).where(
            PushSubscription.endpoint == endpoint,
            PushSubscription.username == username,
        )
    )
    db.session.commit()
    return jsonify({"ok": True})


@misc_routes_bp.route("/logout", endpoint="logout")
def logout():
    """Logout user and redirect to login page"""
    oidc = get_oidc()
    oidc.logout()
    return redirect(config.oidc_logout_url)
