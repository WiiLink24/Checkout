from io import BytesIO

import requests

from utils.utils import cache

_THEME_CACHE_TTL = 24 * 60 * 60


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

    # Darken the average color
    dark_base = _mix_with(base_rgb, (0, 0, 0), 0.65)

    print(
        f"Generated theme for {pfp_url}: base={dark_base}, light={_mix_with(dark_base, (255, 255, 255), 0.35)}, dark={_mix_with(dark_base, (0, 0, 0), 0.3)}, soft={_mix_with(dark_base, (255, 255, 255), 0.55)}"
    )
    return {
        "base": _rgb_to_hex(dark_base),
        "light": _rgb_to_hex(_mix_with(dark_base, (255, 255, 255), 0.35)),
        "dark": _rgb_to_hex(_mix_with(dark_base, (0, 0, 0), 0.3)),
        "soft": _rgb_to_hex(_mix_with(dark_base, (255, 255, 255), 0.55)),
        "rgb": "{}, {}, {}".format(*dark_base),
        "transparent": "rgba({}, {}, {}, 0.6)".format(*dark_base),
    }


def get_user_theme(pfp_url):
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
