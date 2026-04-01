from flask import Flask, render_template, jsonify, request, session
import re
import hashlib
import config
from flask_oidc import OpenIDConnect

from utils import get_serial_prefixes, fetch_recommendations, fetch_time_played, fetch_recommendation_averages, fetch_time_played_stats

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = config.db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = config.secret_key
app.config["OIDC_CLIENT_SECRETS"] = config.oidc_client_secrets_json
app.config["OIDC_SCOPES"] = "openid profile email"
app.config["OIDC_OVERWRITE_REDIRECT_URI"] = config.oidc_redirect_uri

oidc = OpenIDConnect(app)

@app.template_filter("format_serial")
def format_serial(s):
    """Format serial number with dashes every 4 characters"""
    s = str(s)
    return "-".join([s[i:i+4] for i in range(0, len(s), 4)])

@app.template_filter("format_playtime")
def format_playtime(minutes):
    """Format minutes as days, hours, minutes"""
    if not minutes:
        return "0m"
    minutes = int(minutes)
    days = minutes // (24 * 60)
    remaining = minutes % (24 * 60)
    hours = remaining // 60
    mins = remaining % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if mins > 0 or not parts:
        parts.append(f"{mins}m")
    return " ".join(parts)

def build_user_info(profile):
    username = profile.get("preferred_username")
    email = profile.get("email") or ""
    picture_value = hashlib.sha256(email.encode()).hexdigest()
    picture_value = f"https://www.gravatar.com/avatar/{picture_value}?d=identicon&s=128"
    return {
        "username": username,
        "full_name": profile.get("name") or "",
        "profile_picture": picture_value,
        "linked_wii_no": profile.get("wiis") or []
    }

@app.route("/recommendations")
@oidc.require_login
def recommendations():
    profile = session.get("oidc_auth_profile") or {}
    user_info = build_user_info(profile)
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return f"User has not linked their Wii", 400
    results = fetch_recommendations(serial_prefixes)
    print(profile)
    return render_template("recommendations.html", recommendations=results, user_info=user_info)

@app.route("/recommendations/averages")
@oidc.require_login
def recommendation_averages():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    gender_raw = request.args.get("gender", "")
    age_min_raw = request.args.get("age_min", "")
    age_max_raw = request.args.get("age_max", "")

    gender = int(gender_raw) if gender_raw.isdigit() else None
    age_min = int(age_min_raw) if age_min_raw.isdigit() else None
    age_max = int(age_max_raw) if age_max_raw.isdigit() else None

    averages = fetch_recommendation_averages(game_id, gender=gender, age_min=age_min, age_max=age_max)
    if not averages:
        return jsonify({"total": 0}), 200
    return jsonify(averages), 200

@app.route("/time_played/stats")
@oidc.require_login
def time_played_stats():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    
    stats = fetch_time_played_stats(game_id)
    if not stats:
        return jsonify({"total_players": 0, "total_minutes": 0, "avg_minutes_per_player": 0}), 200
    return jsonify(stats), 200

@app.route("/time_played")
@oidc.require_login
def time_played():
    profile = session.get("oidc_auth_profile") or {}
    user_info = build_user_info(profile)
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return f"User has not linked their Wii", 400
    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"
    results = fetch_time_played(serial_prefixes, sort_by=sort_by)
    return render_template("time_played.html", time_played=results, serial_prefix=", ".join(serial_prefixes), user_info=user_info, sort_by=sort_by)

@app.route("/")
def index():
    return render_template("login.html")

if __name__ == "__main__":
    app.run(host="::", port=8080)
