from datetime import datetime
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

from utils.utils import (
    extract_serial_prefix,
    find_user_by_wii_number,
    generate_gravatar_url,
    normalize_serial,
    format_serial,
)
from channels.nc import fetch_user_latest_games, fetch_user_stats

_TAG_SIZE = (1000, 400)
_TAG_BG_START = (30, 30, 34)
_TAG_BG_END = (55, 55, 60)

_COLOR_USERNAME = (255, 255, 255)
_COLOR_CODE = (209, 209, 216)
_COLOR_STAT_LABEL = (171, 171, 186)
_COLOR_STAT_VALUE = (240, 240, 245)
_COLOR_FOOTER_TEXT = (169, 169, 180)
_COLOR_FOOTER_BORDER = (190, 190, 200, 56)
_COLOR_COVER_BORDER = (220, 220, 230, 51)
_COLOR_PLACEHOLDER_BG = (90, 90, 100, 115)
_COLOR_PLACEHOLDER_TEXT = (208, 208, 219)

_COVER_SIZE = (120, 170)
_COVER_GAP = 15

_FONT_CACHE = {}
_BUNDLED_RUBIK = str(
    Path(__file__).resolve().parent.parent / "static" / "fonts" / "Rubik[wght].ttf"
)
_FONT_CANDIDATES = {
    "regular": [
        _BUNDLED_RUBIK,
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
    ],
    "bold": [
        _BUNDLED_RUBIK,
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
    ],
}


def generate_user_tag(friend_code):
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

    try:
        png_bytes = _render_tag_png(
            username=authentik_user.get("username", "Unknown"),
            pfp_url=generate_gravatar_url(authentik_user.get("email", "")),
            formatted_code=format_serial(friend_code_normalized),
            playtime_text=_format_playtime(user_stats.get("total_minutes", 0)),
            games=games,
            tag_background_url=_get_tag_background_url(games),
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return BytesIO(png_bytes)
    except Exception as e:
        print(f"Error rendering PNG: {e}")
        return None


def _render_tag_png(
    username,
    pfp_url,
    formatted_code,
    playtime_text,
    games,
    tag_background_url,
    generated_at,
):
    tag = _draw_gradient(_TAG_SIZE, _TAG_BG_START, _TAG_BG_END).convert("RGBA")

    if tag_background_url:
        background = _load_image(tag_background_url)
        if background:
            tag = _blend_background(tag, background)

    draw = ImageDraw.Draw(tag, "RGBA")
    _draw_header(tag, draw, username, pfp_url, formatted_code, playtime_text)
    _draw_game_covers(tag, draw, games)
    _draw_footer(draw, generated_at)

    buffer = BytesIO()
    tag.convert("RGB").save(buffer, format="PNG")
    return buffer.getvalue()


def _draw_gradient(size, start, end):
    width, height = size

    horizontal = Image.new("RGB", size)
    vertical = Image.new("RGB", size)
    h_draw = ImageDraw.Draw(horizontal)
    v_draw = ImageDraw.Draw(vertical)

    for x in range(width):
        t = x / (width - 1)
        color = tuple(int(start[c] + (end[c] - start[c]) * t) for c in range(3))
        h_draw.line([(x, 0), (x, height - 1)], fill=color)

    for y in range(height):
        t = y / (height - 1)
        color = tuple(int(start[c] + (end[c] - start[c]) * t) for c in range(3))
        v_draw.line([(0, y), (width - 1, y)], fill=color)

    return ImageChops.add(horizontal, vertical, scale=2.0)


def _blend_background(tag, background):
    width, height = _TAG_SIZE
    scaled = background.resize(
        (int(width * 1.08), int(height * 1.08)), Image.LANCZOS
    ).filter(ImageFilter.GaussianBlur(24))

    left = (scaled.width - width) // 2
    top = (scaled.height - height) // 2
    cover = scaled.crop((left, top, left + width, top + height))
    cover = cover.convert("RGBA")
    cover.putalpha(Image.new("L", cover.size, 77))  # 30% opacity

    return Image.alpha_composite(tag, cover)


def _draw_header(tag, draw, username, pfp_url, formatted_code, playtime_text):
    pfp = _load_image(pfp_url)
    if pfp:
        pfp = _resize_square(pfp, 60)

    username_font = _get_font(48, bold=True)
    code_font = _get_font(16)
    label_font = _get_font(12)
    value_font = _get_font(36)
    label = "TOTAL PLAYTIME"

    gap = 8
    text_height = sum(username_font.getmetrics()) + gap + sum(code_font.getmetrics())
    playtime_height = sum(label_font.getmetrics()) + gap + sum(value_font.getmetrics())

    center_y = 60
    if pfp:
        _paste_rounded(tag, pfp, (40, int(center_y - 30)), radius=30)

    text_y = int(center_y - text_height / 2)
    draw.text((120, text_y), username, font=username_font, fill=_COLOR_USERNAME)
    code_y = text_y + sum(username_font.getmetrics()) + gap
    draw.text((120, code_y), formatted_code, font=code_font, fill=_COLOR_CODE)

    playtime_y = int(center_y - playtime_height / 2)
    right = 960
    draw.text(
        (right - draw.textlength(label, font=label_font), playtime_y),
        label,
        font=label_font,
        fill=_COLOR_STAT_LABEL,
    )
    value_y = playtime_y + sum(label_font.getmetrics()) + gap
    draw.text(
        (right - draw.textlength(playtime_text, font=value_font), value_y),
        playtime_text,
        font=value_font,
        fill=_COLOR_STAT_VALUE,
    )


def _draw_game_covers(tag, draw, games):
    covers = games[:7]
    if not covers:
        return

    cover_w, cover_h = _COVER_SIZE
    total_width = len(covers) * cover_w + (len(covers) - 1) * _COVER_GAP
    start_x = (1000 - total_width) / 2
    y = 151

    for i, game in enumerate(covers):
        x = int(start_x + i * (cover_w + _COVER_GAP))

        cover = _load_image(game.get("cover_url"))
        if cover is None and game.get("fallback_url"):
            cover = _load_image(game.get("fallback_url"))

        if cover is None:
            _draw_placeholder(draw, x, y)
            continue

        cover = _resize_cover(cover)
        tag.paste(cover, (x, y))
        draw.rectangle(
            [x, y, x + cover_w, y + cover_h],
            outline=_COLOR_COVER_BORDER,
            width=2,
        )


def _draw_placeholder(draw, x, y):
    cover_w, cover_h = _COVER_SIZE
    draw.rectangle(
        [x, y, x + cover_w - 1, y + cover_h - 1],
        fill=_COLOR_PLACEHOLDER_BG,
    )
    text = "No Cover"
    font = _get_font(12)
    text_w = draw.textlength(text, font=font)
    draw.text(
        (x + (cover_w - text_w) / 2, y + (cover_h - 12) / 2),
        text,
        font=font,
        fill=_COLOR_PLACEHOLDER_TEXT,
    )


def _draw_footer(draw, generated_at):
    line_y = 350
    draw.line([(40, line_y), (960, line_y)], fill=_COLOR_FOOTER_BORDER, width=1)

    text = f"Generated {generated_at} • WiiLink Checkout"
    font = _get_font(11)
    text_w = draw.textlength(text, font=font)
    draw.text(((1000 - text_w) / 2, 358), text, font=font, fill=_COLOR_FOOTER_TEXT)


def _load_image(url):
    if not url:
        return None
    try:
        response = requests.get(url, timeout=10)
        if not response.ok:
            return None
        return Image.open(BytesIO(response.content)).convert("RGB")
    except Exception as e:
        print(f"Error loading image {url}: {e}")
        return None


def _resize_square(image, size):
    side = min(image.size)
    left = (image.width - side) // 2
    top = (image.height - side) // 2
    image = image.crop((left, top, left + side, top + side))
    return image.resize((size, size), Image.LANCZOS)


def _resize_cover(image):
    cover_w, cover_h = _COVER_SIZE
    if image.width / image.height > cover_w / cover_h:
        new_w = int(cover_h * image.width / image.height)
        image = image.resize((new_w, cover_h), Image.LANCZOS)
        left = (new_w - cover_w) // 2
        image = image.crop((left, 0, left + cover_w, cover_h))
    else:
        new_h = int(cover_w * image.height / image.width)
        image = image.resize((cover_w, new_h), Image.LANCZOS)
        top = (new_h - cover_h) // 2
        image = image.crop((0, top, cover_w, top + cover_h))
    return image


def _paste_rounded(tag, image, position, radius):
    mask = Image.new("L", image.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [0, 0, image.size[0] - 1, image.size[1] - 1], radius=radius, fill=255
    )
    tag.paste(image, position, mask=mask)


def _get_font(size, bold=False):
    key = (size, bold)
    if key not in _FONT_CACHE:
        font_path = next(
            (
                path
                for path in _FONT_CANDIDATES["bold" if bold else "regular"]
                if Path(path).exists()
            ),
            None,
        )
        if font_path:
            font = ImageFont.truetype(font_path, size)
            try:
                font.set_variation_by_axes([700] if bold else [400])
            except (AttributeError, ValueError):
                pass
            _FONT_CACHE[key] = font
        else:
            try:
                _FONT_CACHE[key] = ImageFont.load_default(size)
            except TypeError:
                _FONT_CACHE[key] = ImageFont.load_default()
    return _FONT_CACHE[key]


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
