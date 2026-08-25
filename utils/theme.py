from io import BytesIO
import json
from pathlib import Path

import requests

from utils.utils import cache

_THEME_CACHE_TTL = 24 * 60 * 60
_THEME_CATALOG_PATH = Path(__file__).resolve().parent.parent / "static" / "themes.json"


def _catalog_asset_url(directory, filename):
    if not filename:
        return ""
    return f"/static/themes/{directory}/{filename}"


def get_theme_catalog():
    try:
        with _THEME_CATALOG_PATH.open(encoding="utf-8") as theme_file:
            themes = json.load(theme_file)
        normalized = {}
        normalized["default"] = {
            "id": "default",
            "name": "Default",
            "description": "Use the original profile theme.",
            "price": 0,
            "base": "#111827",
            "dark": "#030712",
            "light": "#646873",
            "soft": "#9ca3af",
            "rgb": "17, 24, 39",
            "dark_rgb": "3, 7, 18",
            "transparent": "rgba(17, 24, 39, 0.14)",
            "font": "",
            "background": "",
            "bgm": "",
        }
        for theme in themes:
            if not theme.get("id"):
                continue
            theme = dict(theme)
            theme["background"] = _catalog_asset_url("backgrounds", theme.get("background", ""))
            theme["bgm"] = _catalog_asset_url("audio", theme.get("bgm", ""))
            font = theme.get("font", "")
            theme["font"] = (
                "'Minecraft', sans-serif"
                if font == "minecraft"
                else font
            )
            normalized[theme["id"]] = theme
        return normalized
    except (OSError, ValueError, TypeError):
        return {}


def _download_image(url):
    try:
        response = requests.get(url, timeout=10)
        if not response.ok:
            return None
        return BytesIO(response.content)
    except Exception:
        return None


def _rgb_to_hex(rgb):
    return "#{:02x}{:02x}{:02x}".format(*[max(0, min(255, int(c))) for c in rgb])


def _mix_with(rgb, target, factor):
    """Blend rgb toward target (white for lighter, black for darker)."""
    factor = max(0.0, min(1.0, factor))
    return tuple(int(c + (t - c) * factor) for c, t in zip(rgb, target))


def _build_theme(pfp_url):
    image_file = _download_image(pfp_url)
    if image_file is None:
        return None

    try:
        import colorgram
    except ImportError:
        return None

    try:
        colors = colorgram.extract(image_file, 5)
        base_rgb = colors[0].rgb
    except Exception:
        return None

    # Darken the average color so page and sidebar backgrounds stay dark-mode safe
    base = _mix_with(base_rgb, (0, 0, 0), 0.65)
    dark = _mix_with(base, (0, 0, 0), 0.45)

    return {
        "base": _rgb_to_hex(base),
        "dark": _rgb_to_hex(dark),
        "light": _rgb_to_hex(_mix_with(base, (255, 255, 255), 0.35)),
        "soft": _rgb_to_hex(_mix_with(base, (255, 255, 255), 0.55)),
        "transparent": "rgba({}, {}, {}, 0.14)".format(*base),
        "rgb": "{}, {}, {}".format(*base),
        "dark_rgb": "{}, {}, {}".format(*dark),
    }


def get_user_theme(pfp_url, theme_id=None):
    """Return theme dict {base, dark, light, transparent, rgb} for a profile picture URL (cached)."""
    if theme_id:
        catalog_theme = get_theme_catalog().get(theme_id)
        if catalog_theme:
            return catalog_theme
    if not pfp_url:
        return None

    cache_key = f"user_theme:v2:{pfp_url}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    theme = _build_theme(pfp_url)
    if theme is None:
        return None

    cache.set(cache_key, theme, timeout=_THEME_CACHE_TTL)
    return theme
