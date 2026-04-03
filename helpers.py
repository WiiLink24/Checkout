from flask import request, abort
from flask_oidc import OpenIDConnect
from nc import fetch_recommendations, fetch_time_played
from utils import (
    extract_serial_prefix,
    find_user_by_wii_number,
    build_viewed_user_info,
    build_unclaimed_user_info,
    normalize_serial,
)
from auth import build_user_info, get_user_profile

# Get oidc instance imported from app through utils/config
from flask import current_app


def get_oidc():
    return current_app.extensions.get("oidc")


def parse_int(value):
    """Parse string to int, return None if invalid"""
    return int(value) if value.isdigit() else None


def create_serial_page_context(friend_code, template_name):
    """Create context for friend-code-based recommendation/time_played pages
    We will only return serial search data if the number is unclaimed, otherwise the search will return 404
    as it will need the friend code to search, in order to protect the user's S/N.
    """
    friend_code = normalize_serial(friend_code)

    user_info = None
    if get_oidc() and get_oidc().user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)

    # Look up user by Wii number (friend code)
    authentik_user = find_user_by_wii_number(friend_code)

    # Return 404 if user not found
    if not authentik_user:
        abort(404)

    user_serial = authentik_user.get("attributes", {}).get("serial")
    if isinstance(user_serial, list):
        user_serial = user_serial[0] if user_serial else friend_code
    serial_prefixes = extract_serial_prefix(user_serial)

    # Fetch data from database
    if template_name == "recommendations.html":
        sort_by = request.args.get("sort", "recommendation_percent")
        if sort_by not in ("recommendation_percent", "last_recommended"):
            sort_by = "recommendation_percent"
        results = fetch_recommendations(serial_prefixes, sort_by=sort_by)
    else:  # time_played.html
        sort_by = request.args.get("sort", "time_played")
        if sort_by not in ("time_played", "times_played", "last_played"):
            sort_by = "time_played"
        results = fetch_time_played(serial_prefixes, sort_by=sort_by)

    viewed_user = build_viewed_user_info(authentik_user)

    context = {
        (
            "recommendations"
            if template_name == "recommendations.html"
            else "time_played"
        ): results,
        "user_info": user_info,
        "viewed_user": viewed_user,
        "is_unclaimed": False,
        "base_url": f"/{friend_code}",
    }

    if template_name in ("time_played.html", "recommendations.html"):
        context["sort_by"] = sort_by

    return context


def create_unclaimed_serial_context(serial, template_name):
    """Create context for unlinked serial pages (recommendations/time_played)"""
    serial = normalize_serial(serial)

    user_info = None
    logged_in_user_picture = None
    if get_oidc() and get_oidc().user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
        logged_in_user_picture = profile.get("picture")

    # Look up user by Wii number
    authentik_user = find_user_by_wii_number(serial)

    # Return 404 if user is linked (claimed)
    if authentik_user:
        abort(404)

    # Serial is unclaimed - use it directly
    serial_prefixes = extract_serial_prefix(serial)

    # Fetch data from database
    if template_name == "recommendations.html":
        sort_by = request.args.get("sort", "recommendation_percent")
        if sort_by not in ("recommendation_percent", "last_recommended"):
            sort_by = "recommendation_percent"
        results = fetch_recommendations(serial_prefixes, sort_by=sort_by)
    else:  # time_played.html
        sort_by = request.args.get("sort", "time_played")
        if sort_by not in ("time_played", "times_played", "last_played"):
            sort_by = "time_played"
        results = fetch_time_played(serial_prefixes, sort_by=sort_by)

    # Build unclaimed user info
    viewed_user = build_unclaimed_user_info(serial, logged_in_user_picture)

    context = {
        (
            "recommendations"
            if template_name == "recommendations.html"
            else "time_played"
        ): results,
        "user_info": user_info,
        "viewed_user": viewed_user,
        "unclaimed_serial": serial,
        "is_unclaimed": True,
        "base_url": f"/{serial}",
    }

    if template_name in ("time_played.html", "recommendations.html"):
        context["sort_by"] = sort_by

    return context
