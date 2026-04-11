from flask import Blueprint, render_template
from channels.nc import (
    fetch_top_most_played,
    fetch_top_best_games,
    fetch_top_favorites,
)
from utils.cache import load_cached_games

trending_bp = Blueprint("trending", __name__)


def get_logged_in_user_info():
    from app import get_logged_in_user_info as get_user

    return get_user()


@trending_bp.route("/top/most-played", endpoint="top_most_played")
def top_most_played():
    games_html = load_cached_games(
        "top_most_played.html", fetch_top_most_played, "most_played"
    )
    user_info = get_logged_in_user_info()
    return render_template(
        "top_most_played.html", cached_games_html=games_html, user_info=user_info
    )


@trending_bp.route("/top/best-games", endpoint="top_best_games")
def top_best_games():
    games_html = load_cached_games(
        "top_best_games.html", fetch_top_best_games, "best_games"
    )
    user_info = get_logged_in_user_info()
    return render_template(
        "top_best_games.html", cached_games_html=games_html, user_info=user_info
    )


@trending_bp.route("/top/favorites", endpoint="top_favorites")
def top_favorites():
    games_html = load_cached_games(
        "top_favorites.html", fetch_top_favorites, "favorites"
    )
    user_info = get_logged_in_user_info()
    return render_template(
        "top_favorites.html", cached_games_html=games_html, user_info=user_info
    )
