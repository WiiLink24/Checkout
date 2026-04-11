from flask import (
    Blueprint,
    redirect,
    url_for,
    render_template,
    request,
    jsonify,
    send_file,
    make_response,
    flash,
    session as flask_session,
)
import csv
import io
import zipfile
from datetime import datetime, timedelta
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
from channels.digi import fetch_orders_by_email

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


def can_export_data():
    """Check if user can export data (once per month limit)"""
    last_export = request.cookies.get("last_takeout_export")
    if not last_export:
        return True, None

    try:
        last_export_time = datetime.fromisoformat(last_export)
        next_export_time = last_export_time + timedelta(days=30)
        if datetime.now() >= next_export_time:
            return True, None
        return False, next_export_time
    except (ValueError, TypeError):
        return True, None


def get_next_export_time():
    """Get the next time user can export data"""
    last_export = request.cookies.get("last_takeout_export")
    if not last_export:
        return None

    try:
        last_export_time = datetime.fromisoformat(last_export)
        next_export_time = last_export_time + timedelta(days=30)
        return next_export_time
    except (ValueError, TypeError):
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


@auth_routes_bp.route("/private/takeout", endpoint="takeout")
def takeout():
    if not (oidc and oidc.user_loggedin):
        return redirect(url_for("index"))

    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(user_info)
    wii_numbers = user_info.get("linked_wii_no", [])

    profile = get_user_profile()
    email = profile.get("email") if profile else None

    can_export, next_available = can_export_data()
    days_until_next_export = None
    if next_available:
        time_diff = next_available - datetime.now()
        days_until_next_export = time_diff.days + (1 if time_diff.seconds > 0 else 0)

    cam_orders = []
    if profile and profile.get("email"):
        cam_orders = fetch_orders_by_email(profile["email"])

    data_counts = {
        "recommendations": count_recommendations(serial_prefixes),
        "favorites": count_bookmarks(serial_prefixes),
        "time_played": count_time_played(serial_prefixes),
        "polls": count_user_polls(wii_numbers),
        "suggestions": count_user_suggestions(wii_numbers),
        "contest_submissions": count_contest_submissions(wii_numbers),
        "cam_orders": len(cam_orders),
    }

    return render_template(
        "takeout.html",
        user_info=user_info,
        data_counts=data_counts,
        can_export=can_export,
        days_until_next_export=days_until_next_export,
    )


@auth_routes_bp.route(
    "/private/takeout/export", endpoint="takeout_export", methods=["POST"]
)
def takeout_export():
    if not (oidc and oidc.user_loggedin):
        return redirect(url_for("index"))

    # Check rate limiting
    can_export, _ = can_export_data()
    if not can_export:
        flash(
            "You can only export data once per month. Please try again later.", "error"
        )
        return redirect(url_for("auth_routes.takeout"))

    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(user_info)
    wii_numbers = user_info.get("linked_wii_no", [])

    profile = get_user_profile()
    email = profile.get("email") if profile else None

    # Get requested exports from form
    requested_exports = request.form.getlist("data_type")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        exported_items = []

        # Recommendations
        if "recommendations" in requested_exports:
            recommendations = fetch_recommendations(serial_prefixes)
            if recommendations:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(
                    csv_buffer, fieldnames=recommendations[0].keys()
                )
                writer.writeheader()
                writer.writerows(recommendations)
                zip_file.writestr("recommendations.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Recommendations: {len(recommendations)} entries"
                )

        # Favorites
        if "favorites" in requested_exports:
            favorites = fetch_favorites(serial_prefixes, limit=10000)
            if favorites:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=favorites[0].keys())
                writer.writeheader()
                writer.writerows(favorites)
                zip_file.writestr("favorites.csv", csv_buffer.getvalue())
                exported_items.append(f"Favorites: {len(favorites)} entries")

        # Time Played
        if "time_played" in requested_exports:
            time_played = fetch_time_played(serial_prefixes, limit=10000)
            if time_played:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=time_played[0].keys())
                writer.writeheader()
                writer.writerows(time_played)
                zip_file.writestr("time_played.csv", csv_buffer.getvalue())
                exported_items.append(f"Time Played: {len(time_played)} entries")

        # Polls
        if "polls" in requested_exports:
            polls = fetch_user_polls(wii_numbers, limit=10000)
            if polls:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=polls[0].keys())
                writer.writeheader()
                writer.writerows(polls)
                zip_file.writestr("polls.csv", csv_buffer.getvalue())
                exported_items.append(f"Polls: {len(polls)} entries")

        # Suggestions
        if "suggestions" in requested_exports:
            suggestions = fetch_user_suggestions(wii_numbers, limit=10000)
            if suggestions:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=suggestions[0].keys())
                writer.writeheader()
                writer.writerows(suggestions)
                zip_file.writestr("suggestions.csv", csv_buffer.getvalue())
                exported_items.append(f"Suggestions: {len(suggestions)} entries")

        # Contest Submissions
        if "contest_submissions" in requested_exports:
            submissions = fetch_contest_submissions(wii_numbers, limit=10000)
            if submissions:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=submissions[0].keys())
                writer.writeheader()
                writer.writerows(submissions)
                zip_file.writestr("contest_submissions.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Contest Submissions: {len(submissions)} entries"
                )

        # Digicam
        if "cam_orders" in requested_exports:
            cam_orders = fetch_orders_by_email(email) if email else []
            if cam_orders:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=cam_orders[0].keys())
                writer.writeheader()
                writer.writerows(cam_orders)
                zip_file.writestr("cam_orders.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Digicam Prints Orders: {len(cam_orders)} entries"
                )

        # Create summary file
        summary = f"""WiiLink Takeout Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Username: {user_info.get('username', 'Unknown')}

Exported Data:
"""
        for item in exported_items:
            summary += f"  - {item}\n"

        zip_file.writestr("EXPORT_SUMMARY.txt", summary)

    zip_buffer.seek(0)
    response = make_response(
        send_file(
            zip_buffer,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"checkout_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
    )

    # Set cookie for rate limiting (I know this is easily bypassed by clearing cookies, but it's just a dissuasive measure)
    response.set_cookie(
        "last_takeout_export",
        datetime.now().isoformat(),
        max_age=2592000,
        httponly=True,
        secure=True,
        samesite="Lax",
    )

    return response


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
