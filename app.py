from flask import Flask, render_template, jsonify, request, redirect, url_for, abort
import config
from flask_oidc import OpenIDConnect

from filters import format_serial, format_playtime
from auth import build_user_info, get_user_profile
from helpers import parse_int, create_serial_page_context
from utils import (
    get_serial_prefixes, fetch_recommendations, fetch_time_played,
    fetch_recommendation_averages, fetch_time_played_stats,
    fetch_top_most_played, fetch_top_best_games,
    fetch_authentik_users, find_user_by_wii_number,
    normalize_serial, extract_serial_prefix,
    build_viewed_user_info, build_unclaimed_user_info, find_game_recommendation,
    fetch_user_latest_games, fetch_user_latest_reviews, fetch_user_stats
)

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


@app.route("/recommendations")
def recommendations():
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    profile = get_user_profile()
    user_info = build_user_info(profile)
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return "User has not linked their Wii", 400
    results = fetch_recommendations(serial_prefixes)
    return render_template("recommendations.html", recommendations=results, user_info=user_info, viewed_user=user_info)


@app.route("/recommendations/averages")
def recommendation_averages():
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    
    gender = parse_int(request.args.get("gender", ""))
    age_min = parse_int(request.args.get("age_min", ""))
    age_max = parse_int(request.args.get("age_max", ""))

    averages = fetch_recommendation_averages(game_id, gender=gender, age_min=age_min, age_max=age_max)
    return jsonify(averages or {"total": 0}), 200


@app.route("/time_played/stats")
def time_played_stats():
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    
    stats = fetch_time_played_stats(game_id)
    return jsonify(stats or {"total_players": 0, "total_minutes": 0, "avg_minutes_per_player": 0}), 200


@app.route("/time_played")
def time_played():
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    profile = get_user_profile()
    user_info = build_user_info(profile)
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return "User has not linked their Wii", 400
    
    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"
    
    results = fetch_time_played(serial_prefixes, sort_by=sort_by)
    return render_template("time_played.html", time_played=results, serial_prefix=", ".join(serial_prefixes),
                         user_info=user_info, viewed_user=user_info, sort_by=sort_by, base_url=None)

@app.route("/search")
def search():
    user_info = None
    if oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
    users = fetch_authentik_users()
    search_query = request.args.get("search", "").strip().lower()
    
    # Filter users based on search query
    if search_query:
        users = [
            user for user in users
            if search_query in user.get("username", "").lower()
            or any(search_query in wii.lower() for wii in user.get("attributes", {}).get("wiis", []))
        ]
    
    return render_template("search.html", users=users, search_query=search_query, user_info=user_info)

@app.route("/top/most-played")
def top_most_played():
    user_info = None
    if oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
    games = fetch_top_most_played(30)
    return render_template("top_most_played.html", games=games, user_info=user_info)


@app.route("/top/best-games")
def top_best_games():
    user_info = None
    if oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
    games = fetch_top_best_games(30)
    return render_template("top_best_games.html", games=games, user_info=user_info)

@app.route("/discover")
def discover():
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    profile = get_user_profile()
    user_info = build_user_info(profile)
    serial_prefixes = get_serial_prefixes(profile)
    
    if not serial_prefixes:
        return "User has not linked their Wii", 400
    
    game = find_game_recommendation(serial_prefixes)
    return render_template("discover.html", user_info=user_info, game=game)

@app.route("/<friend_code>/recommendations")
def friend_code_recommendations(friend_code):
    context = create_serial_page_context(friend_code, "recommendations.html")
    return render_template("recommendations.html", **context)


@app.route("/<friend_code>/time_played")
def friend_code_time_played(friend_code):
    context = create_serial_page_context(friend_code, "time_played.html")
    return render_template("time_played.html", **context)


@app.route("/<friend_code>/")
def friend_code_home(friend_code):
    """Display a user's home page by friend code"""
    user_info = None
    if oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
    
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
    latest_games = fetch_user_latest_games(serial_prefixes, 5) if serial_prefixes else []
    latest_reviews = fetch_user_latest_reviews(serial_prefixes, 5) if serial_prefixes else []
    user_stats = fetch_user_stats(serial_prefixes) if serial_prefixes else {'total_minutes': 0, 'total_reviews': 0}
    
    # Build viewed user info
    viewed_user = build_viewed_user_info(authentik_user)
    
    return render_template("home.html",
                         user_info=user_info,
                         viewed_user=viewed_user,
                         latest_games=latest_games,
                         latest_reviews=latest_reviews,
                         user_stats=user_stats,
                         is_unclaimed=False,
                         base_url=f"/{friend_code}")

@app.route("/")
def index():
    if oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
        serial_prefixes = get_serial_prefixes(profile)
        
        latest_games = []
        latest_reviews = []
        user_stats = {'total_minutes': 0, 'total_reviews': 0}
        
        if serial_prefixes:
            latest_games = fetch_user_latest_games(serial_prefixes, 5)
            latest_reviews = fetch_user_latest_reviews(serial_prefixes, 5)
            user_stats = fetch_user_stats(serial_prefixes)
        
        return render_template("home.html", 
                             user_info=user_info,
                             viewed_user=user_info,
                             latest_games=latest_games,
                             latest_reviews=latest_reviews,
                             user_stats=user_stats)
    else:
        return render_template("login.html")


@app.route("/logout")
def logout():
    """Logout user and redirect to login page"""
    oidc.logout()
    return redirect(url_for("index"))

@app.errorhandler(404)
def handle_404(error):
    """Handle 404 Not Found errors"""
    return render_template("errors/error.html",
                         error_code=404,
                         error_title="Page Not Found",
                         error_message="The page you're looking for doesn't exist.",
                         error_details="Make sure the URL is correct and try again."), 404


@app.errorhandler(500)
def handle_500(error):
    """Handle 500 Internal Server errors"""
    return render_template("errors/error.html",
                         error_code=500,
                         error_title="Internal Server Error",
                         error_message="Something went wrong on our end.",
                         error_details="The server encountered an unexpected error. Please try again later."), 500


@app.errorhandler(403)
def handle_403(error):
    """Handle 403 Forbidden errors"""
    return render_template("errors/error.html",
                         error_code=403,
                         error_title="Access Forbidden",
                         error_message="You don't have permission to access this resource.",
                         error_details="If you believe this is a mistake, please contact an administrator."), 403


@app.errorhandler(400)
def handle_400(error):
    """Handle 400 Bad Request errors"""
    return render_template("errors/error.html",
                         error_code=400,
                         error_title="Bad Request",
                         error_message="The request was invalid or malformed.",
                         error_details="Please check your input and try again."), 400


if __name__ == "__main__":
    app.run(host="::", port=8080)

