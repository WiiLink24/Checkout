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
)
from utils import (
    get_serial_prefixes,
    fetch_authentik_users,
    find_user_by_wii_number,
    find_user_by_serial,
    normalize_serial,
    extract_serial_prefix,
    build_viewed_user_info,
    build_unclaimed_user_info,
)
from discover import find_game_recommendation
from evc import fetch_user_polls, fetch_user_suggestions
from cmoc import get_artisan_id_from_wii_number

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = config.db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = config.secret_key
app.config["OIDC_CLIENT_SECRETS"] = config.oidc_client_secrets_json
app.config["OIDC_SCOPES"] = "openid profile email"
app.config["OIDC_OVERWRITE_REDIRECT_URI"] = config.oidc_redirect_uri

oidc = OpenIDConnect(app)

# Register template filters
app.jinja_env.filters["format_serial"] = format_serial
app.jinja_env.filters["format_playtime"] = format_playtime


@app.context_processor
def inject_artisan_id():
    """Inject artisan ID for logged-in user into template context."""
    artisan_id = None
    if oidc.user_loggedin:
        user_info = get_logged_in_user_info()
        if user_info and user_info.get("linked_wii_no"):
            wii_number = user_info["linked_wii_no"][0]
            artisan_id = get_artisan_id_from_wii_number(wii_number)
    return dict(artisan_id=artisan_id)


def get_logged_in_user_info():
    """Get logged-in user info or None if not logged in"""
    if oidc.user_loggedin:
        profile = get_user_profile()
        return build_user_info(profile)
    return None


@app.route("/recommendations")
def recommendations():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400
    sort_by = request.args.get("sort", "recommendation_percent")
    if sort_by not in ("recommendation_percent", "last_recommended"):
        sort_by = "recommendation_percent"
    results = fetch_recommendations(serial_prefixes, sort_by=sort_by)
    return render_template(
        "recommendations.html",
        recommendations=results,
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
    )


@app.route("/recommendations/averages")
def recommendation_averages():
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
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
    if not oidc.user_loggedin:
        return redirect(url_for("index"))
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

    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"

    results = fetch_time_played(serial_prefixes, sort_by=sort_by)
    return render_template(
        "time_played.html",
        time_played=results,
        serial_prefix=", ".join(serial_prefixes),
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
        base_url=None,
    )


@app.route("/search")
def search():
    user_info = get_logged_in_user_info()
    users = fetch_authentik_users()
    search_query = request.args.get("search", "").strip().lower()

    # Filter users based on search query
    if search_query:
        users = [
            user
            for user in users
            if search_query in user.get("username", "").lower()
            or any(
                search_query in wii.lower()
                for wii in user.get("attributes", {}).get("wiis", [])
            )
        ]

    return render_template(
        "search.html", users=users, search_query=search_query, user_info=user_info
    )


@app.route("/top/most-played")
def top_most_played():
    user_info = get_logged_in_user_info()
    games = fetch_top_most_played(30)
    return render_template("top_most_played.html", games=games, user_info=user_info)


@app.route("/top/best-games")
def top_best_games():
    user_info = get_logged_in_user_info()
    games = fetch_top_best_games(30)
    return render_template("top_best_games.html", games=games, user_info=user_info)


@app.route("/top/favorites")
def top_favorites():
    user_info = get_logged_in_user_info()
    games = fetch_top_favorites(30)
    return render_template("top_favorites.html", games=games, user_info=user_info)


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

    games = fetch_favorites(serial_prefixes, 30)
    return render_template(
        "favorites.html",
        games=games,
        user_info=user_info,
        viewed_user=user_info,
        is_unclaimed=False,
        base_url=None,
    )


@app.route("/<serial_or_code>/favorites")
def favorites_by_serial(serial_or_code):
    """Display favorites - works for linked friend codes and unlinked serials"""
    serial_or_code = normalize_serial(serial_or_code)

    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(serial_or_code)
    if authentik_user:
        user_serial = authentik_user.get("attributes", {}).get("serial")
        if isinstance(user_serial, list):
            user_serial = user_serial[0] if user_serial else serial_or_code
        serial_prefixes = extract_serial_prefix(user_serial)

        games = fetch_favorites(serial_prefixes, 30)
        viewed_user = build_viewed_user_info(authentik_user)
        return render_template(
            "favorites.html",
            games=games,
            user_info=user_info,
            viewed_user=viewed_user,
            is_unclaimed=False,
            base_url=f"/{serial_or_code}",
        )

    # Check if it's a linked serial
    authentik_user_by_serial = find_user_by_serial(serial_or_code)
    if authentik_user_by_serial:
        abort(404)

    # Serial is unlinked - show unclaimed context
    serial_prefixes = extract_serial_prefix(serial_or_code)
    games = fetch_favorites(serial_prefixes, 30)
    logged_in_user_picture = user_info.get("profile_picture") if user_info else None
    viewed_user = build_unclaimed_user_info(serial_or_code, logged_in_user_picture)
    return render_template(
        "favorites.html",
        games=games,
        user_info=user_info,
        viewed_user=viewed_user,
        unclaimed_serial=serial_or_code,
        is_unclaimed=True,
        base_url=f"/{serial_or_code}",
    )


@app.route("/<friend_code>.png")
def friend_code_tag(friend_code):
    """Generate a dynamic PNG tag for a user showing their stats and recent games"""
    png_io = generate_user_tag(friend_code)

    if png_io is None:
        abort(404)

    return send_file(png_io, mimetype="image/png", as_attachment=False)


@app.route("/<serial_or_code>/recommendations")
def recommendations_by_serial(serial_or_code):
    """Display recommendations - works for linked friend codes and unlinked serials"""
    serial_or_code = normalize_serial(serial_or_code)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(serial_or_code)
    if authentik_user:
        # It's a linked friend code - show linked account data
        context = create_serial_page_context(serial_or_code, "recommendations.html")
        context["user_info"] = user_info  # Ensure logged-in user info is included
        return render_template("recommendations.html", **context)

    # Check if it's a linked serial
    authentik_user_by_serial = find_user_by_serial(serial_or_code)
    if authentik_user_by_serial:
        # It's a linked serial - return 404
        abort(404)

    # Serial is unlinked - show unclaimed context
    context = create_unclaimed_serial_context(serial_or_code, "recommendations.html")
    context["user_info"] = user_info  # Ensure logged-in user info is included
    return render_template("recommendations.html", **context)


@app.route("/<serial_or_code>/time_played")
def time_played_by_serial(serial_or_code):
    """Display time played - works for linked friend codes and unlinked serials"""
    serial_or_code = normalize_serial(serial_or_code)

    # Get logged-in user info if available
    user_info = get_logged_in_user_info()

    # Check if it's a linked friend code
    authentik_user = find_user_by_wii_number(serial_or_code)
    if authentik_user:
        # It's a linked friend code - show linked account data
        context = create_serial_page_context(serial_or_code, "time_played.html")
        context["user_info"] = user_info  # Ensure logged-in user info is included
        return render_template("time_played.html", **context)

    # Check if it's a linked serial
    authentik_user_by_serial = find_user_by_serial(serial_or_code)
    if authentik_user_by_serial:
        # It's a linked serial - return 404
        abort(404)

    # Serial is unlinked - show unclaimed context
    context = create_unclaimed_serial_context(serial_or_code, "time_played.html")
    context["user_info"] = user_info  # Ensure logged-in user info is included
    return render_template("time_played.html", **context)


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

    user_serial = authentik_user.get("attributes", {}).get("serial")
    if isinstance(user_serial, list):
        user_serial = user_serial[0] if user_serial else friend_code_normalized
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
    
    polls_data = fetch_user_polls(wii_numbers, 30)
    return render_template(
        "polls.html",
        polls=polls_data,
        user_info=user_info,
        viewed_user=user_info,
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
    
    suggestions_data = fetch_user_suggestions(wii_numbers, 30)
    return render_template(
        "suggestions.html",
        suggestions=suggestions_data,
        user_info=user_info,
        viewed_user=user_info,
    )


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


if __name__ == "__main__":
    app.run(host="::", port=8080)
