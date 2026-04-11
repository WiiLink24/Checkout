from flask import Blueprint, render_template, abort, send_file, request
from utils.helpers import parse_int, is_public_profile
from utils.utils import (
    find_user_by_wii_number,
    normalize_serial,
    extract_serial_prefix,
    build_viewed_user_info,
    build_unclaimed_user_info,
)
from channels.nc import (
    fetch_favorites,
    fetch_recommendations,
    fetch_time_played,
    count_recommendations,
    count_time_played,
    serial_has_time_played,
    count_bookmarks,
)
from channels.evc import fetch_user_polls, fetch_user_suggestions
from channels.cmoc import (
    get_artisan_ids_from_wii_number,
    fetch_contest_submissions,
    count_contest_submissions,
    render_mii_to_url,
)
from channels.tag_generator import generate_user_tag
from channels.nc import (
    fetch_user_latest_games,
    fetch_user_latest_reviews,
    fetch_user_stats,
)

public_routes_bp = Blueprint("public_routes", __name__)


def get_logged_in_user_info():
    from app import get_logged_in_user_info as get_user

    return get_user()


@public_routes_bp.route("/<friend_code>.png", endpoint="friend_code_tag")
def friend_code_tag(friend_code):
    """Generate a dynamic PNG tag for a user showing their stats and recent games"""
    png_io = generate_user_tag(friend_code)

    if png_io is None:
        abort(404)

    return send_file(png_io, mimetype="image/png", as_attachment=False)


@public_routes_bp.route(
    "/<wii_no>/recommendations", endpoint="recommendations_by_serial"
)
def recommendations_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)
    user_serial = None
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )

        wiis = authentik_user.get("attributes", {}).get("wiis") or authentik_user.get(
            "wiis", []
        )
        if isinstance(wiis, list):
            for wii in wiis:
                if isinstance(wii, dict) and wii.get("serial_number"):
                    user_serial = wii.get("serial_number")
                    break

    if authentik_user and user_serial:
        serial_prefixes = extract_serial_prefix(user_serial)
        
        page = parse_int(request.args.get("page", "1"))
        if page < 1:
            page = 1
        per_page = 30
        offset = (page - 1) * per_page
        
        sort_by = request.args.get("sort", "recommendation_percent")
        if sort_by not in ("recommendation_percent", "last_recommended"):
            sort_by = "recommendation_percent"
        
        total_count = count_recommendations(serial_prefixes)
        total_pages = (total_count + per_page - 1) // per_page
        recommendations = fetch_recommendations(
            serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
        )
        
        viewed_user = build_viewed_user_info(authentik_user)
        
        context = {
            "recommendations": recommendations,
            "user_info": user_info,
            "viewed_user": viewed_user,
            "is_unclaimed": False,
            "base_url": f"/{wii_no}",
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
            "sort_by": sort_by,
        }
        return render_template("recommendations.html", **context)

    if authentik_user:
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    serial_prefixes = extract_serial_prefix(wii_no)
    if not serial_has_time_played(serial_prefixes):
        abort(404)

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page
    
    sort_by = request.args.get("sort", "recommendation_percent")
    if sort_by not in ("recommendation_percent", "last_recommended"):
        sort_by = "recommendation_percent"
    
    total_count = count_recommendations(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page
    recommendations = fetch_recommendations(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    
    logged_in_user_picture = user_info.get("profile_picture") if user_info else None
    viewed_user = build_unclaimed_user_info(wii_no, logged_in_user_picture)
    
    context = {
        "recommendations": recommendations,
        "user_info": user_info,
        "viewed_user": viewed_user,
        "is_unclaimed": True,
        "base_url": f"/{wii_no}",
        "page": page,
        "total_pages": total_pages,
        "total_count": total_count,
        "sort_by": sort_by,
    }
    return render_template("recommendations.html", **context)


@public_routes_bp.route("/<wii_no>/time_played", endpoint="time_played_by_serial")
def time_played_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)
    user_serial = None
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )

        wiis = authentik_user.get("attributes", {}).get("wiis") or authentik_user.get(
            "wiis", []
        )
        if isinstance(wiis, list):
            for wii in wiis:
                if isinstance(wii, dict) and wii.get("serial_number"):
                    user_serial = wii.get("serial_number")
                    break
    if authentik_user and user_serial:
        serial_prefixes = extract_serial_prefix(user_serial)
        
        page = parse_int(request.args.get("page", "1"))
        if page < 1:
            page = 1
        per_page = 30
        offset = (page - 1) * per_page
        
        sort_by = request.args.get("sort", "time_played")
        if sort_by not in ("time_played", "times_played", "last_played"):
            sort_by = "time_played"
        
        total_count = count_time_played(serial_prefixes)
        total_pages = (total_count + per_page - 1) // per_page
        time_played = fetch_time_played(
            serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
        )
        
        viewed_user = build_viewed_user_info(authentik_user)
        
        context = {
            "time_played": time_played,
            "user_info": user_info,
            "viewed_user": viewed_user,
            "is_unclaimed": False,
            "base_url": f"/{wii_no}",
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
            "sort_by": sort_by,
        }
        return render_template("time_played.html", **context)

    if authentik_user:
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    serial_prefixes = extract_serial_prefix(wii_no)
    if not serial_has_time_played(serial_prefixes):
        abort(404)

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page
    
    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"
    
    total_count = count_time_played(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page
    time_played = fetch_time_played(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    
    logged_in_user_picture = user_info.get("profile_picture") if user_info else None
    viewed_user = build_unclaimed_user_info(wii_no, logged_in_user_picture)
    
    context = {
        "time_played": time_played,
        "user_info": user_info,
        "viewed_user": viewed_user,
        "is_unclaimed": True,
        "base_url": f"/{wii_no}",
        "page": page,
        "total_pages": total_pages,
        "total_count": total_count,
        "sort_by": sort_by,
    }
    return render_template("time_played.html", **context)


@public_routes_bp.route("/<wii_no>/favorites", endpoint="favorites_by_serial")
def favorites_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)

    user_serial = None
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )

        wiis = authentik_user.get("attributes", {}).get("wiis") or authentik_user.get(
            "wiis", []
        )
        if isinstance(wiis, list):
            for wii in wiis:
                if isinstance(wii, dict) and wii.get("serial_number"):
                    user_serial = wii.get("serial_number")
                    break

    if authentik_user and user_serial:
        serial_prefixes = extract_serial_prefix(user_serial)

        page = parse_int(request.args.get("page", "1"))
        if page < 1:
            page = 1
        per_page = 30
        offset = (page - 1) * per_page

        total_count = count_bookmarks(serial_prefixes)
        total_pages = (total_count + per_page - 1) // per_page

        games = fetch_favorites(serial_prefixes, limit=per_page, offset=offset)
        viewed_user = build_viewed_user_info(authentik_user)
        return render_template(
            "favorites.html",
            games=games,
            user_info=user_info,
            viewed_user=viewed_user,
            is_unclaimed=False,
            base_url=f"/{wii_no}",
            page=page,
            total_pages=total_pages,
            total_count=total_count,
        )

    if authentik_user:
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    serial_prefixes = extract_serial_prefix(wii_no)
    if not serial_has_time_played(serial_prefixes):
        abort(404)

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_bookmarks(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    games = fetch_favorites(serial_prefixes, limit=per_page, offset=offset)
    logged_in_user_picture = user_info.get("profile_picture") if user_info else None
    viewed_user = build_unclaimed_user_info(wii_no, logged_in_user_picture)
    return render_template(
        "favorites.html",
        games=games,
        user_info=user_info,
        viewed_user=viewed_user,
        unclaimed_serial=wii_no,
        is_unclaimed=True,
        base_url=f"/{wii_no}",
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@public_routes_bp.route("/<wii_no>/polls", endpoint="polls_by_serial")
def polls_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )
        viewed_user = build_viewed_user_info(authentik_user)
        polls_data = fetch_user_polls([wii_no], 30)

        context = {
            "polls": polls_data,
            "user_info": user_info,
            "viewed_user": viewed_user,
            "page_title": "Polls",
            "is_unclaimed": False,
            "base_url": f"/{wii_no}",
        }
        return render_template("polls.html", **context)

    abort(404)


@public_routes_bp.route("/<wii_no>/suggestions", endpoint="suggestions_by_serial")
def suggestions_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )

        viewed_user = build_viewed_user_info(authentik_user)
        suggestions_data = fetch_user_suggestions([wii_no], 30)

        context = {
            "suggestions": suggestions_data,
            "user_info": user_info,
            "viewed_user": viewed_user,
            "page_title": "Suggestions",
            "is_unclaimed": False,
            "base_url": f"/{wii_no}",
        }
        return render_template("suggestions.html", **context)

    abort(404)


@public_routes_bp.route(
    "/<wii_no>/contest_submissions", endpoint="contest_submissions_by_serial"
)
def contest_submissions_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )

        viewed_user = build_viewed_user_info(authentik_user)

        page = parse_int(request.args.get("page", "1"))
        if page < 1:
            page = 1
        per_page = 30
        offset = (page - 1) * per_page

        total_count = count_contest_submissions([wii_no])
        total_pages = (total_count + per_page - 1) // per_page

        submissions_data = fetch_contest_submissions(
            [wii_no], limit=per_page, offset=offset
        )

        for submission in submissions_data:
            if submission.get("mii_data"):
                submission["mii_image_url"] = render_mii_to_url(submission["mii_data"])
            else:
                submission["mii_image_url"] = None

        context = {
            "submissions": submissions_data,
            "user_info": user_info,
            "viewed_user": viewed_user,
            "page_title": "Contest Submissions",
            "is_unclaimed": False,
            "base_url": f"/{wii_no}",
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
            "artisan_id": (
                get_artisan_ids_from_wii_number(wii_no)[0]
                if get_artisan_ids_from_wii_number(wii_no)
                else None
            ),
        }
        return render_template("contest_submissions.html", **context)

    abort(404)


@public_routes_bp.route("/<friend_code>/", endpoint="friend_code_home")
def friend_code_home(friend_code):
    """Display a user's home page by friend code"""
    user_info = get_logged_in_user_info()

    friend_code_normalized = normalize_serial(friend_code)
    authentik_user = find_user_by_wii_number(friend_code_normalized)

    if not authentik_user:
        abort(404)

    if not is_public_profile(authentik_user, user_info):
        return (
            render_template("errors/private_profile.html", user_info=user_info),
            400,
        )

    wiis = authentik_user.get("attributes", {}).get("wiis")
    user_serial = None
    if isinstance(wiis, list):
        for wii in wiis:
            if isinstance(wii, dict) and wii.get("serial_number"):
                user_serial = wii.get("serial_number")
                break

    if not user_serial:
        return (
            render_template(
                "errors/not_linked_external.html",
                user_info=user_info,
                friend_code=friend_code_normalized,
            ),
            400,
        )

    serial_prefixes = extract_serial_prefix(user_serial)

    latest_games = (
        fetch_user_latest_games(serial_prefixes, 5) if serial_prefixes else []
    )
    latest_favorites = fetch_favorites(serial_prefixes, 5) if serial_prefixes else []
    latest_reviews = (
        fetch_user_latest_reviews(serial_prefixes, 5) if serial_prefixes else []
    )
    user_stats = (
        fetch_user_stats(serial_prefixes)
        if serial_prefixes
        else {"total_minutes": 0, "total_reviews": 0}
    )

    viewed_user = build_viewed_user_info(authentik_user)

    return render_template(
        "home.html",
        user_info=user_info,
        viewed_user=viewed_user,
        latest_games=latest_games,
        latest_favorites=latest_favorites,
        latest_reviews=latest_reviews,
        user_stats=user_stats,
        is_unclaimed=False,
        base_url=f"/{friend_code}",
    )
