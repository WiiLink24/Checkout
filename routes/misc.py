from flask import Blueprint, render_template, request, redirect, url_for
import random
import config
from utils.utils import search_authentik_users_by_name, fetch_authentik_users

misc_routes_bp = Blueprint("misc_routes", __name__, url_prefix="")


def get_logged_in_user_info():
    from app import get_logged_in_user_info as get_user

    return get_user()


def get_oidc():
    from app import oidc

    return oidc


@misc_routes_bp.route("/search", endpoint="search")
def search():
    user_info = get_logged_in_user_info()
    search_query = request.args.get("search", "").strip().lower()

    if search_query:
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
    else:
        users = fetch_authentik_users()
        users = [user for user in users if user.get("attributes", {}).get("wiis")]

    random.shuffle(users)
    return render_template(
        "search.html", users=users, search_query=search_query, user_info=user_info
    )


@misc_routes_bp.route("/logout", endpoint="logout")
def logout():
    """Logout user and redirect to login page"""
    oidc = get_oidc()
    oidc.logout()
    return redirect(config.oidc_logout_url)


@misc_routes_bp.errorhandler(404)
def handle_404(error):
    """Handle 404 Not Found errors"""
    user_info = get_logged_in_user_info()
    return (
        render_template(
            "errors/error.html",
            error_code=404,
            error_title="Page Not Found",
            error_message="The page you're looking for doesn't exist.",
            error_details="Make sure the URL is correct and try again.",
            user_info=user_info,
        ),
        404,
    )


@misc_routes_bp.errorhandler(500)
def handle_500(error):
    """Handle 500 Internal Server errors"""
    user_info = get_logged_in_user_info()
    return (
        render_template(
            "errors/error.html",
            error_code=500,
            error_title="Internal Server Error",
            error_message="Something went wrong on our end.",
            error_details="The server encountered an unexpected error. Please try again later.",
            user_info=user_info,
        ),
        500,
    )


@misc_routes_bp.errorhandler(403)
def handle_403(error):
    """Handle 403 Forbidden errors"""
    user_info = get_logged_in_user_info()
    return (
        render_template(
            "errors/error.html",
            error_code=403,
            error_title="Access Forbidden",
            error_message="You don't have permission to access this resource.",
            error_details="If you believe this is a mistake, please contact an administrator.",
            user_info=user_info,
        ),
        403,
    )


@misc_routes_bp.errorhandler(400)
def handle_400(error):
    """Handle 400 Bad Request errors"""
    user_info = get_logged_in_user_info()
    return (
        render_template(
            "errors/error.html",
            error_code=400,
            error_title="Bad Request",
            error_message="The request was invalid or malformed.",
            error_details="Please check your input and try again.",
            user_info=user_info,
        ),
        400,
    )
