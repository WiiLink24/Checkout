"""Top pages take a while to generate per request, so the request ends up failing, instead we cache the html and regen every day."""

import os
from flask import render_template
from channels.nc import (
    fetch_top_most_played,
    fetch_top_best_games,
    fetch_top_favorites,
)

CACHE_DIR = "cache"
_app = None


def init_cache(app=None):
    global _app
    _app = app
    os.makedirs(CACHE_DIR, exist_ok=True)


def generate_top_page_cache():
    """Generate and cache only the games div for top pages."""
    if _app is None:
        return

    with _app.app_context():
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
                print(f"[CACHE] Generated cache for {cache_file}")
        except Exception as e:
            import traceback

            traceback.print_exc()


def load_cached_games(cache_filename, fetch_func, score_type):
    cache_file = os.path.join(CACHE_DIR, cache_filename)
    games_html = ""

    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            games_html = f.read()
    else:
        games = fetch_func(30)
        games_html = render_template(
            "partials/games_grid.html", games=games, score_type=score_type
        )
        try:
            with open(cache_file, "w") as f:
                f.write(games_html)
        except Exception as e:
            print(f"[CACHE] Failed to save cache: {e}")

    return games_html
