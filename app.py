import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (
    Flask,
    render_template,
    jsonify,
    request,
    redirect,
    url_for,
    abort,
    send_file,
)
import config
from flask_oidc import OpenIDConnect

from filters import format_serial, format_playtime
from auth import build_user_info, get_user_profile
from helpers import (
    parse_int,
    create_serial_page_context,
    create_unclaimed_serial_context,
    is_public_profile,
)
from tag_generator import generate_user_tag
from nc import (
    fetch_recommendations,
    fetch_time_played,
    fetch_recommendation_averages,
    fetch_time_played_stats,
    fetch_top_most_played,
    fetch_top_best_games,
    fetch_top_favorites,
    fetch_user_latest_games,
    fetch_user_latest_reviews,
    fetch_user_stats,
    fetch_favorites,
    serial_has_time_played,
    count_bookmarks,
    count_recommendations,
    count_time_played,
)
from utils import (
    get_serial_prefixes,
    fetch_authentik_users,
    find_user_by_wii_number,
    normalize_serial,
    extract_serial_prefix,
    build_viewed_user_info,
    build_unclaimed_user_info,
    search_authentik_users_by_name,
)
from discover import find_game_recommendation
from evc import (
    fetch_user_polls,
    fetch_user_suggestions,
    count_user_polls,
    count_user_suggestions,
)
from cmoc import (
    get_artisan_ids_from_wii_number,
    fetch_contest_submissions,
    count_contest_submissions,
    render_mii_to_url,
)

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = config.db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = config.secret_key
app.config["OIDC_CLIENT_SECRETS"] = config.oidc_client_secrets_json
app.config["OIDC_SCOPES"] = "openid profile email"
app.config["OIDC_OVERWRITE_REDIRECT_URI"] = config.oidc_redirect_uri

oidc = OpenIDConnect(app)

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Register template filters
app.jinja_env.filters["format_serial"] = format_serial
app.jinja_env.filters["format_playtime"] = format_playtime


@app.context_processor
def inject_artisan_id():
    """Inject artisan IDs for logged-in user into template context."""
    artisan_ids = []
    if oidc.user_loggedin:
        user_info = get_logged_in_user_info()
        if user_info and user_info.get("linked_wii_no"):
            wii_number = user_info["linked_wii_no"][0]
            artisan_ids = get_artisan_ids_from_wii_number(wii_number)
    return dict(artisan_ids=artisan_ids)


def get_logged_in_user_info():
    """Get logged-in user info or None if not logged in"""
    if oidc.user_loggedin:
        profile = get_user_profile()
        return build_user_info(profile)
    return None


def generate_top_page_cache():
    """Generate and cache only the games div for top pages."""
    try:
        pages = {
            "top_most_played.html": (fetch_top_most_played(30), "most_played"),
            "top_best_games.html": (fetch_top_best_games(30), "best_games"),
            "top_favorites.html": (fetch_top_favorites(30), "favorites"),
        }

        for cache_file, (games, score_type) in pages.items():
            games_html = render_template(
                "partials/games_grid.html", games=games, score_type=score_type
            )
            with open(os.path.join(CACHE_DIR, cache_file), "w") as f:
                f.write(games_html)
    except Exception as e:
        import traceback

        traceback.print_exc()


@app.route("/recommendations")
def recommendations():
    if not oidc.user_loggedin:
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


@app.route("/recommendations/averages")
def recommendation_averages():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    gender = parse_int(request.args.get("gender", ""))
    age_min = parse_int(request.args.get("age_min", ""))
    age_max = parse_int(request.args.get("age_max", ""))

    averages = fetch_recommendation_averages(
        game_id, gender=gender, age_min=age_min, age_max=age_max
    )
    return jsonify(averages or {"total": 0}), 200


@app.route("/time_played/stats")
def time_played_stats():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    stats = fetch_time_played_stats(game_id)
    return (
        jsonify(
            stats
            or {"total_players": 0, "total_minutes": 0, "avg_minutes_per_player": 0}
        ),
        200,
    )


@app.route("/time_played")
def time_played():
    if not oidc.user_loggedin:
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


@app.route("/search")
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


@app.route("/top/most-played")
def top_most_played():
    cache_file = os.path.join(CACHE_DIR, "top_most_played.html")
    games_html = ""

    # Load cached games content if exists
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            games_html = f.read()
    else:
        games = fetch_top_most_played(30)
        games_html = render_template(
            "partials/games_grid.html", games=games, score_type="most_played"
        )
        try:
            with open(cache_file, "w") as f:
                f.write(games_html)
        except Exception as e:
            print(f"[CACHE] Failed to save cache: {e}")

    user_info = get_logged_in_user_info()
    return render_template(
        "top_most_played.html", cached_games_html=games_html, user_info=user_info
    )


@app.route("/top/best-games")
def top_best_games():
    cache_file = os.path.join(CACHE_DIR, "top_best_games.html")
    games_html = ""

    # Load cached games content if exists
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            games_html = f.read()
    else:
        games = fetch_top_best_games(30)
        games_html = render_template(
            "partials/games_grid.html", games=games, score_type="best_games"
        )
        try:
            with open(cache_file, "w") as f:
                f.write(games_html)
        except Exception as e:
            print(f"[CACHE] Failed to save cache: {e}")

    user_info = get_logged_in_user_info()
    return render_template(
        "top_best_games.html", cached_games_html=games_html, user_info=user_info
    )


@app.route("/top/favorites")
def top_favorites():
    cache_file = os.path.join(CACHE_DIR, "top_favorites.html")
    games_html = ""

    # Load cached games content if exists
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            games_html = f.read()
    else:
        games = fetch_top_favorites(30)
        games_html = render_template(
            "partials/games_grid.html", games=games, score_type="favorites"
        )
        try:
            with open(cache_file, "w") as f:
                f.write(games_html)
        except Exception as e:
            print(f"[CACHE] Failed to save cache: {e}")

    user_info = get_logged_in_user_info()
    return render_template(
        "top_favorites.html", cached_games_html=games_html, user_info=user_info
    )


@app.route("/discover")
def discover():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)

    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    game = find_game_recommendation(serial_prefixes)
    return render_template("discover.html", user_info=user_info, game=game)


@app.route("/favorites")
def favorites():
    if not oidc.user_loggedin:
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


# User search routes


@app.route("/<wii_no>/favorites")
def favorites_by_serial(wii_no):
    """Display favorites - works for linked friend codes and unlinked serials"""
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(wii_no)
        
    user_serial = None
    if authentik_user:
        # Check if the user wants their data to be public
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

    # We need the serial to check for this info
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
        # It's a linked friend code but no serial, cannot show favorites without serial
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    # Serial is unlinked, check if it exists in the bookmarks database
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


@app.route("/<friend_code>.png")
def friend_code_tag(friend_code):
    """Generate a dynamic PNG tag for a user showing their stats and recent games"""
    png_io = generate_user_tag(friend_code)

    if png_io is None:
        abort(404)

    return send_file(png_io, mimetype="image/png", as_attachment=False)


@app.route("/<wii_no>/recommendations")
def recommendations_by_serial(wii_no):
    """Display recommendations - works for linked friend codes and unlinked serials"""
    wii_no = normalize_serial(wii_no)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code and serial
    authentik_user = find_user_by_wii_number(wii_no)
    user_serial = None
    if authentik_user:
        # Check if the user wants their data to be public
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
        context = create_serial_page_context(wii_no, "recommendations.html")
        context["user_info"] = user_info  # Ensure logged-in user info is included
        return render_template("recommendations.html", **context)

    if authentik_user:
        # It's a linked friend code but no serial, cannot show recommendations without serial
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    # Serial is unlinked, check if it exists in the recommendations database
    serial_prefixes = extract_serial_prefix(wii_no)
    if not serial_has_time_played(serial_prefixes):
        abort(404)

    context = create_unclaimed_serial_context(wii_no, "recommendations.html")
    context["user_info"] = user_info  # Ensure logged-in user info is included
    return render_template("recommendations.html", **context)


@app.route("/<wii_no>/time_played")
def time_played_by_serial(wii_no):
    """Display time played - works for linked friend codes and unlinked serials"""
    wii_no = normalize_serial(wii_no)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code and serial
    authentik_user = find_user_by_wii_number(wii_no)
    user_serial = None
    if authentik_user:
        # Check if the user wants their data to be public
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
        context = create_serial_page_context(wii_no, "time_played.html")
        context["user_info"] = user_info  # Ensure logged-in user info is included
        return render_template("time_played.html", **context)

    if authentik_user:
        # It's a linked friend code but no serial, cannot show time played without serial
        return (
            render_template("errors/not_linked_external.html", user_info=user_info),
            400,
        )

    # Serial is unlinked, check if it exists in the time_played database
    serial_prefixes = extract_serial_prefix(wii_no)
    if not serial_has_time_played(serial_prefixes):
        abort(404)

    context = create_unclaimed_serial_context(wii_no, "time_played.html")
    context["user_info"] = user_info  # Ensure logged-in user info is included
    return render_template("time_played.html", **context)


@app.route("/<wii_no>/polls")
def polls_by_serial(wii_no):
    """Display polls - works for linked friend codes and unlinked serials"""
    wii_no = normalize_serial(wii_no)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        # Check if the user wants their data to be public
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )
        # It's a linked friend code - show linked account data
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

    # Serial is unclaimed - cannot show polls/suggestions without wii_number
    # Return 404 for unclaimed serials
    abort(404)


@app.route("/<wii_no>/suggestions")
def suggestions_by_serial(wii_no):
    """Display suggestions - works for linked friend codes and unlinked serials"""
    wii_no = normalize_serial(wii_no)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        # Check if the user wants their data to be public
        if not is_public_profile(authentik_user, user_info):
            return (
                render_template("errors/private_profile.html", user_info=user_info),
                400,
            )
            
        # It's a linked friend code - show linked account data
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

    # Serial is unclaimed - cannot show polls/suggestions without wii_number
    # Return 404 for unclaimed serials
    abort(404)


@app.route("/<wii_no>/contest_submissions")
def contest_submissions_by_serial(wii_no):
    wii_no = normalize_serial(wii_no)

    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(wii_no)
    if authentik_user:
        # Check if the user wants their data to be public
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

    # Return 404 for unclaimed serials
    abort(404)


@app.route("/<friend_code>/")
def friend_code_home(friend_code):
    """Display a user's home page by friend code"""
    user_info = get_logged_in_user_info()

    # Look up user by Wii number (friend code)
    friend_code_normalized = normalize_serial(friend_code)
    authentik_user = find_user_by_wii_number(friend_code_normalized)

    # Return 404 if user not found
    if not authentik_user:
        abort(404)
        
    # Check if the user wants their data to be public
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

    # Fetch data from database
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

    # Build viewed user info
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


@app.route("/")
def index():
    if oidc.user_loggedin:
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


@app.route("/polls")
def polls():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()

    # Get Wii numbers from user info
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


@app.route("/suggestions")
def suggestions():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
    user_info = get_logged_in_user_info()

    # Get Wii numbers from user info
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


@app.route("/contest_submissions")
def contest_submissions():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
    user_info = get_logged_in_user_info()

    # Get Wii numbers from user info
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

    return render_template(
        "contest_submissions.html",
        submissions=submissions_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        artisan_id=(
            get_artisan_ids_from_wii_number(wii_numbers[0])[0] if wii_numbers else None
        ),
    )


# Template routes


@app.route("/logout")
def logout():
    """Logout user and redirect to login page"""
    oidc.logout()
    return redirect(url_for("index"))


@app.errorhandler(404)
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


@app.errorhandler(500)
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


@app.errorhandler(403)
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


@app.errorhandler(400)
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


scheduler = BackgroundScheduler()
scheduler.add_job(generate_top_page_cache, "cron", hour=0, minute=0)
scheduler.start()


if __name__ == "__main__":
    app.run(host="::", port=8080)
