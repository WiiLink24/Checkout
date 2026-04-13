from flask import Blueprint, render_template, request, redirect, url_for
import random
import config
from utils.utils import (
    search_authentik_users_by_name,
    fetch_authentik_users,
    _run_query,
)

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
               t.input_controls, t.wifi_players,
               COALESCE(b.favorite_count, 0) as favorite_count,
               COALESCE(b.user_count, 0) as user_count
        FROM titles t
        LEFT JOIN (
            SELECT game_id, COUNT(*) as favorite_count, COUNT(DISTINCT serial_number) as user_count
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
        SELECT COUNT(*) as total
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
               t.input_controls, t.wifi_players, t.input_players,
               COALESCE(b.favorite_count, 0) as favorite_count,
               COALESCE(b.user_count, 0) as user_count
        ORDER BY favorite_count DESC
        LIMIT %s OFFSET %s
    """
    search_param = f"%{search_query}%"
    return _run_query(query, [search_param, limit, offset], config.db_url) or []


def count_games_by_publisher(search_query):
    """Count total games matching publisher search"""
    query = """
        SELECT COUNT(*) as total
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
               t.input_controls, t.wifi_players, t.input_players,
               COALESCE(b.favorite_count, 0) as favorite_count,
               COALESCE(b.user_count, 0) as user_count
        ORDER BY favorite_count DESC
        LIMIT %s OFFSET %s
    """
    search_param = f"%{search_query}%"
    return _run_query(query, [search_param, limit, offset], config.db_url) or []


def count_games_by_developer(search_query):
    """Count total games matching developer search"""
    query = """
        SELECT COUNT(*) as total
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
    else:
        users = fetch_authentik_users()
        users = [user for user in users if user.get("attributes", {}).get("wiis")]
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


@misc_routes_bp.route("/logout", endpoint="logout")
def logout():
    """Logout user and redirect to login page"""
    oidc = get_oidc()
    oidc.logout()
    return redirect(config.oidc_logout_url)
