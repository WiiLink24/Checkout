import asyncio
from datetime import datetime
from io import BytesIO
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from filters import format_serial
from utils import (
    extract_serial_prefix,
    fetch_user_latest_games,
    fetch_user_stats,
    find_user_by_wii_number,
    generate_gravatar_url,
    normalize_serial,
)

try:
    from playwright.async_api import async_playwright

    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False


_TAG_VIEWPORT = {"width": 1000, "height": 400}
_TEMPLATE_ENV = Environment(
    loader=FileSystemLoader(str(Path(__file__).resolve().parent / "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)
_TAG_TEMPLATE = _TEMPLATE_ENV.get_template("tag/user_tag.html")


def generate_user_tag(friend_code):
    if not HAS_PLAYWRIGHT:
        raise ImportError(
            "Playwright is required. Install with: pip install playwright && playwright install"
        )

    friend_code_normalized = normalize_serial(friend_code)
    authentik_user = find_user_by_wii_number(friend_code_normalized)

    if not authentik_user:
        return None

    user_serial = _extract_user_serial(authentik_user)
    serial_prefixes = extract_serial_prefix(user_serial)

    user_stats = (
        fetch_user_stats(serial_prefixes)
        if serial_prefixes
        else {"total_minutes": 0, "total_reviews": 0}
    )
    latest_games = (
        fetch_user_latest_games(serial_prefixes, 7) if serial_prefixes else []
    )
    games = _build_game_data(latest_games)

    html_content = _TAG_TEMPLATE.render(
        username=authentik_user.get("username", "Unknown"),
        pfp=generate_gravatar_url(authentik_user.get("email", "")),
        formatted_code=format_serial(friend_code_normalized),
        playtime_text=_format_playtime(user_stats.get("total_minutes", 0)),
        games=games,
        tag_background_url=_get_tag_background_url(games),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    # Render HTML to PNG using Playwright
    try:
        png_bytes = asyncio.run(_render_html_to_png(html_content))
        return BytesIO(png_bytes)
    except Exception as e:
        print(f"Error rendering PNG: {e}")
        return None


async def _render_html_to_png(html_content):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.set_viewport_size(_TAG_VIEWPORT)
        await page.set_content(html_content)

        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except:
            pass

        png_bytes = await page.screenshot(full_page=False)

        await browser.close()
        return png_bytes


def _extract_user_serial(authentik_user):
    wiis = authentik_user.get("attributes", {}).get("wiis") or authentik_user.get(
        "wiis", []
    )
    if isinstance(wiis, list):
        for wii in wiis:
            if isinstance(wii, dict):
                serial = wii.get("serial_number")
                if serial:
                    return serial
    return None


def _format_playtime(total_minutes):
    total_minutes = int(total_minutes or 0)
    total_hours = total_minutes // 60
    remaining_mins = total_minutes % 60
    return f"{total_hours}h {remaining_mins}m"


def _build_game_data(latest_games):
    games = []
    for game in latest_games[:7]:
        cover_data = get_game_cover_url(game)
        games.append(
            {
                "title": game.get("title", "Game"),
                "cover_url": cover_data.get("url") if cover_data else "",
                "fallback_url": cover_data.get("fallback", "") if cover_data else "",
            }
        )
    return games


def _get_tag_background_url(games):
    if not games or not games[0].get("cover_url"):
        return ""

    return games[0]["cover_url"]


def get_game_cover_url(game):
    game_id = game.get("game_id")
    game_type = game.get("game_type", "Wii")

    if not game_id:
        return None

    if game_type == "DS":
        return {
            "url": f"https://art.gametdb.com/ds/coverHQ/US/{game_id}.jpg",
            "fallback": "",
        }
    elif game_type == "3DS":
        return {
            "url": f"https://art.gametdb.com/3ds/coverHQ/EN/{game_id}.jpg",
            "fallback": "",
        }
    else:  # Wii
        region = game.get("region") or "US"
        return {
            "url": f"https://art.gametdb.com/wii/cover/{region}/{game_id}.png",
            "fallback": f"https://art.gametdb.com/wii/cover/{region}/{game_id}01.png",
        }
