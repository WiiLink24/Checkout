from flask import Blueprint, redirect, url_for, render_template, request, jsonify
from utils.auth import get_user_profile, build_user_info
from utils.helpers import parse_int
from utils.utils import get_serial_prefixes
from channels.nc import (
    fetch_recommendations,
    fetch_time_played,
    fetch_recommendation_averages,
    fetch_time_played_stats,
    fetch_user_latest_games,
    fetch_user_latest_reviews,
    fetch_user_stats,
    fetch_favorites,
    serial_has_time_played,
    count_bookmarks,
    count_recommendations,
    count_time_played,
)
from channels.evc import (
    fetch_user_polls,
    fetch_user_suggestions,
    count_user_polls,
    count_user_suggestions,
)
from channels.cmoc import (
    get_artisan_ids_from_wii_number,
    fetch_contest_submissions,
    count_contest_submissions,
    render_mii_to_url,
)
import config
from channels.discover import find_game_recommendation

auth_routes_bp = Blueprint("auth_routes", __name__)
oidc = None


def set_oidc(oidc_instance):
    global oidc
    oidc = oidc_instance


def get_logged_in_user_info():
    if oidc and oidc.user_loggedin:
        profile = get_user_profile()
        return build_user_info(profile)
    return None


@auth_routes_bp.route("/recommendations", endpoint="recommendations")
def recommendations():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400
    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    sort_by = request.args.get("sort", "recommendation_percent")
    if sort_by not in ("recommendation_percent", "last_recommended"):
        sort_by = "recommendation_percent"

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_recommendations(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    results = fetch_recommendations(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    return render_template(
        "recommendations.html",
        recommendations=results,
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/recommendations/averages", endpoint="recommendation_averages")
def recommendation_averages():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    gender = parse_int(request.args.get("gender", ""))
    age_min = parse_int(request.args.get("age_min", ""))
    age_max = parse_int(request.args.get("age_max", ""))

    averages = fetch_recommendation_averages(
        game_id[:4], gender=gender, age_min=age_min, age_max=age_max
    )
    return jsonify(averages or {"total": 0}), 200


@auth_routes_bp.route("/time_played/stats", endpoint="time_played_stats")
def time_played_stats():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    stats = fetch_time_played_stats(game_id[:4])
    return (
        jsonify(
            stats
            or {"total_players": 0, "total_minutes": 0, "avg_minutes_per_player": 0}
        ),
        200,
    )


@auth_routes_bp.route("/time_played", endpoint="time_played")
def time_played():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400
    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_time_played(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    results = fetch_time_played(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    return render_template(
        "time_played.html",
        time_played=results,
        serial_prefix=", ".join(serial_prefixes),
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
        base_url=None,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/favorites", endpoint="favorites")
def favorites():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)

    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_bookmarks(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    games = fetch_favorites(serial_prefixes, limit=per_page, offset=offset)
    return render_template(
        "favorites.html",
        games=games,
        user_info=user_info,
        viewed_user=user_info,
        is_unclaimed=False,
        base_url=None,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/discover", endpoint="discover")
def discover():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)

    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    game = find_game_recommendation(serial_prefixes)
    return render_template("discover.html", user_info=user_info, game=game)


@auth_routes_bp.route("/polls", endpoint="polls")
def polls():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_user_polls(wii_numbers, db_url=config.evc_db_url)
    total_pages = (total_count + per_page - 1) // per_page

    polls_data = fetch_user_polls(
        wii_numbers, limit=per_page, offset=offset, db_url=config.evc_db_url
    )
    return render_template(
        "polls.html",
        polls=polls_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/suggestions", endpoint="suggestions")
def suggestions():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_user_suggestions(wii_numbers, db_url=config.evc_db_url)
    total_pages = (total_count + per_page - 1) // per_page

    suggestions_data = fetch_user_suggestions(
        wii_numbers, limit=per_page, offset=offset, db_url=config.evc_db_url
    )
    return render_template(
        "suggestions.html",
        suggestions=suggestions_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/contest_submissions", endpoint="contest_submissions")
def contest_submissions():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_contest_submissions(wii_numbers)
    total_pages = (total_count + per_page - 1) // per_page

    submissions_data = fetch_contest_submissions(
        wii_numbers, limit=per_page, offset=offset
    )

    for submission in submissions_data:
        if submission.get("mii_data"):
            submission["mii_image_url"] = render_mii_to_url(submission["mii_data"])
        else:
            submission["mii_image_url"] = None

    artisan_ids = get_artisan_ids_from_wii_number(wii_numbers[0]) if wii_numbers else []

    return render_template(
        "contest_submissions.html",
        submissions=submissions_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        artisan_id=artisan_ids[0] if artisan_ids else None,
    )


@auth_routes_bp.route("/", endpoint="index")
def index():
    if oidc and oidc.user_loggedin:
        profile = get_user_profile()
        user_info = get_logged_in_user_info()
        serial_prefixes = get_serial_prefixes(profile)

        if not serial_prefixes:
            return render_template("errors/not_linked.html", user_info=user_info), 400

        latest_games = fetch_user_latest_games(serial_prefixes, 5)
        latest_favorites = fetch_favorites(serial_prefixes, 5)
        latest_reviews = fetch_user_latest_reviews(serial_prefixes, 5)
        user_stats = fetch_user_stats(serial_prefixes)

        return render_template(
            "home.html",
            user_info=user_info,
            viewed_user=user_info,
            latest_games=latest_games,
            latest_favorites=latest_favorites,
            latest_reviews=latest_reviews,
            user_stats=user_stats,
        )
    else:
        return render_template("login.html", user_info=None)
