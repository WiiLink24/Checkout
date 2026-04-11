import os
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, has_request_context, render_template
import config
from flask_oidc import OpenIDConnect
from flask_session import Session

from utils.utils import format_serial, format_playtime
from channels.cmoc import get_artisan_ids_from_wii_number

# Import blueprint modules
from routes.auth import (
    auth_routes_bp,
    set_oidc as set_oidc_auth,
    get_logged_in_user_info,
)
from routes.public import public_routes_bp
from routes.trending import trending_bp
from routes.digicard import digicard_bp, set_oidc as set_oidc_digicard
from routes.misc import misc_routes_bp
from utils.cache import init_cache, generate_top_page_cache

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = config.db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = config.secret_key
app.config["OIDC_CLIENT_SECRETS"] = config.oidc_client_secrets_json
app.config["OIDC_SCOPES"] = "openid profile email"
app.config["OIDC_OVERWRITE_REDIRECT_URI"] = config.oidc_redirect_uri
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = os.getenv(
    "SESSION_FILE_DIR", os.path.join(os.path.dirname(__file__), "session")
)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True

os.makedirs(app.config["SESSION_FILE_DIR"], exist_ok=True)

oidc = OpenIDConnect(app)
Session(app)

init_cache(app)

# Register template filters
app.jinja_env.filters["format_serial"] = format_serial
app.jinja_env.filters["format_playtime"] = format_playtime


@app.context_processor
def inject_artisan_id():
    """Inject artisan IDs for logged-in user into template context."""
    artisan_ids = []
    # Only check for user login if we're in a request context
    if has_request_context() and oidc.user_loggedin:
        user_info = get_logged_in_user_info()
        if user_info and user_info.get("linked_wii_no"):
            wii_number = user_info["linked_wii_no"][0]
            artisan_ids = get_artisan_ids_from_wii_number(wii_number)
    return dict(artisan_ids=artisan_ids)


# Set OIDC instance for blueprints that need it
set_oidc_auth(oidc)
set_oidc_digicard(oidc)

# Register blueprints
app.register_blueprint(auth_routes_bp)
app.register_blueprint(public_routes_bp)
app.register_blueprint(trending_bp)
app.register_blueprint(digicard_bp)
app.register_blueprint(misc_routes_bp)

# Background scheduler for cache generation
scheduler = BackgroundScheduler()
scheduler.add_job(generate_top_page_cache, "cron", hour=0, minute=0)
scheduler.start()

# If any cache file is missing on startup, generate the cache immediately
cache_dir = os.path.join(os.path.dirname(__file__), "cache")
required_cache_files = [
    "top_most_played.html",
    "top_best_games.html",
    "top_favorites.html",
]
cache_files = set(os.listdir(cache_dir)) if os.path.exists(cache_dir) else set()
if not all(f in cache_files for f in required_cache_files):
    print("Cache directory incomplete. Generating cache...")
    generate_top_page_cache()


# Global error handlers
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
    app.run(host="::", port=8080, debug=True)
