from flask import (
    Blueprint,
    redirect,
    url_for,
    render_template,
    request,
    jsonify,
    send_file,
    make_response,
    flash,
    session as flask_session,
)
import csv
import io
import zipfile
from datetime import datetime, timedelta
from utils.auth import get_user_profile, build_user_info
from utils.helpers import parse_int
from utils.utils import (
    get_serial_prefixes,
    find_user_by_wii_number,
    generate_gravatar_url,
    format_serial,
    cache,
)
from utils.achievements import refresh_achievements_for_user
from utils.theme import get_theme_catalog
from channels.nc import (
    fetch_recommendations,
    fetch_time_played,
    fetch_recommendation_averages,
    fetch_time_played_stats,
    fetch_user_latest_games,
    fetch_user_latest_reviews,
    fetch_user_stats,
    fetch_favorites,
    serial_has_time_played,
    count_bookmarks,
    count_recommendations,
    count_time_played,
)
from channels.evc import (
    fetch_user_polls,
    fetch_user_suggestions,
    count_user_polls,
    count_user_suggestions,
)
from channels.cmoc import (
    get_artisan_ids_from_wii_number,
    fetch_contest_submissions,
    count_contest_submissions,
    render_mii_to_url,
)
import config
from channels.discover import find_game_recommendation
from channels.digi import fetch_orders_by_email, render_card_to_image, get_card_name

auth_routes_bp = Blueprint("auth_routes", __name__)
oidc = None


def _get_owned_user():
    user_info = get_logged_in_user_info()
    linked_wii = (user_info or {}).get("linked_wii_no", [])
    return find_user_by_wii_number(linked_wii[0]) if linked_wii else None


def _enrich_redeemables(redeemables, catalog):
    """Attach theme catalog data to theme redeemables for preview rendering."""
    enriched = []
    for item in redeemables:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "theme":
            enriched.append({**item, "theme": catalog.get(item.get("value"))})
        else:
            enriched.append(item)
    return enriched


def _apply_coupon(coupon, payload, user):
    """Apply a coupon's redeemables to the user's achievements payload and save it."""
    from utils.utils import update_user_attributes

    applied = []
    for item in coupon.get("redeemables") or []:
        kind = item.get("type")
        value = item.get("value")
        if kind == "points":
            points = payload.setdefault(
                "points", {"earned": 0, "spent": 0, "balance": 0}
            )
            points["earned"] = int(points.get("earned", 0)) + int(value)
            points["balance"] = max(0, points["earned"] - int(points.get("spent", 0)))
            applied.append(f"{value} points")
        elif kind == "theme":
            themes = payload.setdefault("themes", {"unlocked": [], "active": None})
            themes.setdefault("unlocked", [])
            if value not in themes["unlocked"]:
                themes["unlocked"].append(value)
                applied.append(f'theme "{value}"')

    if not applied:
        return applied

    attributes = (user.get("attributes") or {}).copy()
    attributes["achievements"] = payload
    update_user_attributes(user, attributes)
    return applied


@auth_routes_bp.route("/themes", methods=["GET", "POST"], endpoint="themes")
def themes():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))

    user = _get_owned_user()
    if not user:
        return render_template("errors/not_linked.html", user_info=None), 400

    user_info = get_logged_in_user_info()

    payload, _ = refresh_achievements_for_user(user, force=True)
    payload = payload or {
        "points": {"balance": 0, "spent": 0, "earned": 0},
        "themes": {"unlocked": [], "active": None},
    }
    catalog = get_theme_catalog()
    theme_state = payload.setdefault("themes", {"unlocked": [], "active": None})
    theme_state.setdefault("unlocked", [])
    points = payload.setdefault("points", {"balance": 0, "spent": 0, "earned": 0})
    points_error = None

    unlocked = set(theme_state.get("unlocked", []))
    categories = []
    seen = []
    for tid, theme in catalog.items():
        if tid == "default":
            continue
        if theme.get("hidden") and tid not in unlocked:
            continue
        cat = theme.get("category") or {}
        title = cat.get("title") or "Other"
        if title not in seen:
            seen.append(title)
            categories.append(
                {
                    "title": title,
                    "description": cat.get("description", ""),
                    "themes": [],
                }
            )
        for bucket in categories:
            if bucket["title"] == title:
                bucket["themes"].append(theme)
                break
    for bucket in categories:
        bucket["themes"].sort(key=lambda t: t["id"] not in unlocked)

    default_theme = catalog.get("default")
    if default_theme:
        categories = [
            {
                "title": "Default",
                "description": "The classic WiiLink Checkout look. Always available.",
                "themes": [default_theme],
            }
        ] + categories

    if request.method == "POST":
        theme_id = request.form.get("theme_id", "")
        action = request.form.get("action", "")
        theme = catalog.get(theme_id)
        if not theme:
            flash("That theme does not exist.", "error")
        elif theme.get("hidden") and theme_id not in theme_state["unlocked"]:
            flash("This theme cannot be purchased.", "error")
        elif action == "unlock" and theme_id not in theme_state["unlocked"]:
            price = max(0, int(theme.get("price", 0)))
            if points.get("balance", 0) < price:
                points_error = {
                    "theme": theme.get("name", theme_id),
                    "price": price,
                    "missing": price - points.get("balance", 0),
                }
            else:
                theme_state["unlocked"].append(theme_id)
                points["spent"] = points.get("spent", 0) + price
                points["balance"] = max(0, points["earned"] - points["spent"])
                flash("Theme unlocked.", "success")
        elif action == "activate" and theme_id in theme_state["unlocked"]:
            theme_state["active"] = theme_id
        elif action == "deactivate":
            theme_state["active"] = None

        attributes = (user.get("attributes") or {}).copy()
        attributes["achievements"] = payload
        from utils.utils import update_user_attributes

        update_user_attributes(user, attributes)

    if user_info is not None:
        user_info["achievements"] = payload
    return render_template(
        "themes.html",
        user_info=user_info,
        viewed_user=user_info,
        categories=categories,
        theme_data=payload,
        points_error=points_error,
    )


@auth_routes_bp.route(
    "/coupons/redeem", methods=["GET", "POST"], endpoint="coupons_redeem"
)
def coupons_redeem():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))

    user = _get_owned_user()
    if not user:
        return render_template("errors/not_linked.html", user_info=None), 400

    user_info = get_logged_in_user_info()
    payload, _ = refresh_achievements_for_user(user, force=True)
    payload = payload or {
        "points": {"balance": 0, "spent": 0, "earned": 0},
        "themes": {"unlocked": [], "active": None},
    }
    coupon_preview = None
    catalog = get_theme_catalog()

    if request.method == "POST":
        from channels.coupons import (
            coupon_available,
            fetch_coupon_by_code,
            fetch_coupon_by_uuid,
            consume_coupon,
            refund_coupon,
        )

        action = request.form.get("action", "")
        username = (user_info or {}).get("username", "Unknown")

        if action == "coupon_preview":
            coupon_code = request.form.get("coupon_code", "").strip()
            coupon = fetch_coupon_by_code(coupon_code) if coupon_code else None
            if not coupon:
                flash("That coupon code is invalid.", "error")
            else:
                available, reason = coupon_available(coupon, username)
                if not available:
                    flash(reason, "error")
                else:
                    coupon_preview = {
                        "uuid": coupon["uuid"],
                        "code": coupon["coupon_code"],
                        "issuer": coupon["issuer"],
                        "max_uses": coupon["max_uses"],
                        "uses_left": (
                            -1
                            if coupon["max_uses"] == -1
                            else max(
                                0, coupon["max_uses"] - (coupon["uses_count"] or 0)
                            )
                        ),
                        "redeemables": _enrich_redeemables(
                            coupon.get("redeemables") or [], catalog
                        ),
                    }
        elif action == "coupon_redeem":
            coupon_uuid = request.form.get("coupon_uuid", "").strip()
            coupon = fetch_coupon_by_uuid(coupon_uuid) if coupon_uuid else None
            if not coupon:
                flash("That coupon is no longer valid.", "error")
            else:
                available, reason = coupon_available(coupon, username)
                if not available:
                    flash(reason, "error")
                elif consume_coupon(coupon["uuid"], username):
                    try:
                        applied = _apply_coupon(coupon, payload, user)
                        if applied:
                            flash(
                                "Coupon redeemed: " + ", ".join(applied) + ".",
                                "success",
                            )
                        else:
                            refund_coupon(coupon["uuid"], username)
                            flash("This coupon contains nothing to redeem.", "error")
                    except Exception as e:
                        refund_coupon(coupon["uuid"], username)
                        print(f"[COUPONS] Redemption failed: {e}")
                        flash("Could not redeem the coupon.", "error")
                else:
                    flash("This coupon has already been fully used.", "error")

    if user_info is not None:
        user_info["achievements"] = payload

    from channels.coupons import user_redeem_history

    username = (user_info or {}).get("username", "")
    return render_template(
        "redeem.html",
        user_info=user_info,
        viewed_user=user_info,
        theme_data=payload,
        coupon_preview=coupon_preview,
        redeem_history=user_redeem_history(username) if username else [],
    )


@auth_routes_bp.route("/friends/toggle", methods=["POST"], endpoint="toggle_friend")
def toggle_friend():
    if not oidc or not oidc.user_loggedin:
        return jsonify({"ok": False, "error": "Not logged in."}), 401

    user_info = get_logged_in_user_info()
    if not user_info or not user_info.get("linked_wii_no"):
        return jsonify({"ok": False, "error": "No Wii linked to your account."}), 400

    data = request.get_json(silent=True) or {}
    friend_code = (
        data.get("friend_code") or request.form.get("friend_code") or ""
    ).strip()
    if not friend_code:
        return jsonify({"ok": False, "error": "Missing friend code."}), 400

    own_user = find_user_by_wii_number(user_info["linked_wii_no"][0])
    if not own_user:
        return jsonify({"ok": False, "error": "Account not found."}), 400

    payload, _ = refresh_achievements_for_user(own_user)
    if not payload:
        return jsonify({"ok": False, "error": "Could not load achievements."}), 500

    friends = payload.setdefault("friends", [])
    if not isinstance(friends, list):
        friends = []
        payload["friends"] = friends

    if friend_code in friends:
        friends.remove(friend_code)
        is_friend = False
    else:
        friends.append(friend_code)
        is_friend = True

    attributes = (own_user.get("attributes") or {}).copy()
    attributes["achievements"] = payload
    from utils.utils import update_user_attributes

    try:
        update_user_attributes(own_user, attributes)
        cache.delete(f"friends:{user_info['linked_wii_no'][0]}")
    except Exception as e:
        print(f"[FRIENDS] Failed to save friends: {e}")
        return jsonify({"ok": False, "error": "Failed to save friends."}), 500

    return jsonify({"ok": True, "is_friend": is_friend, "friend_count": len(friends)})


def _resolve_friends(payload, wii_number):
    """Resolve friend codes into user details, cached briefly for the sidebar."""
    friend_codes = (payload or {}).get("friends") or []
    if not isinstance(friend_codes, list) or not friend_codes:
        return []

    cache_key = f"friends:{wii_number}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    friends = []
    for code in friend_codes:
        friend_user = find_user_by_wii_number(code)
        if not friend_user:
            continue
        email = friend_user.get("email", "")
        friends.append(
            {
                "username": friend_user.get("username", "Unknown"),
                "avatar": generate_gravatar_url(email),
                "wii_number": code,
                "code": format_serial(code),
                "profile_url": f"/{code}/",
            }
        )

    cache.set(cache_key, friends, timeout=120)
    return friends


def set_oidc(oidc_instance):
    global oidc
    oidc = oidc_instance


def get_logged_in_user_info():
    if oidc and oidc.user_loggedin:
        profile = get_user_profile()
        user_info = build_user_info(profile)
        if user_info.get("linked_wii_no"):
            payload = None
            user_id = profile.get("sub") or profile.get("uuid")
            try:
                if user_id:
                    payload, _ = refresh_achievements_for_user({"uuid": user_id})
                else:
                    own_user = find_user_by_wii_number(user_info["linked_wii_no"][0])
                    if own_user:
                        payload, _ = refresh_achievements_for_user(own_user)
            except Exception as e:
                print(f"[ACHIEVEMENTS] session refresh failed: {e}")
                payload = None
            if payload:
                user_info["achievements"] = payload
            user_info["friends"] = _resolve_friends(
                payload, user_info["linked_wii_no"][0]
            )
        return user_info
    return None


def can_export_data():
    """Check if user can export data (once per month limit)"""
    last_export = request.cookies.get("last_takeout_export")
    if not last_export:
        return True, None

    try:
        last_export_time = datetime.fromisoformat(last_export)
        next_export_time = last_export_time + timedelta(days=30)
        if datetime.now() >= next_export_time:
            return True, None
        return False, next_export_time
    except (ValueError, TypeError):
        return True, None


def get_next_export_time():
    """Get the next time user can export data"""
    last_export = request.cookies.get("last_takeout_export")
    if not last_export:
        return None

    try:
        last_export_time = datetime.fromisoformat(last_export)
        next_export_time = last_export_time + timedelta(days=30)
        return next_export_time
    except (ValueError, TypeError):
        return None


@auth_routes_bp.route("/recommendations", endpoint="recommendations")
def recommendations():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400
    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    sort_by = request.args.get("sort", "recommendation_percent")
    if sort_by not in ("recommendation_percent", "last_recommended"):
        sort_by = "recommendation_percent"

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_recommendations(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    results = fetch_recommendations(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    return render_template(
        "recommendations.html",
        recommendations=results,
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/recommendations/averages", endpoint="recommendation_averages")
def recommendation_averages():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    gender = parse_int(request.args.get("gender", ""))
    age_min = parse_int(request.args.get("age_min", ""))
    age_max = parse_int(request.args.get("age_max", ""))

    averages = fetch_recommendation_averages(
        game_id[:3], gender=gender, age_min=age_min, age_max=age_max
    )
    return jsonify(averages or {"total": 0}), 200


@auth_routes_bp.route("/time_played/stats", endpoint="time_played_stats")
def time_played_stats():
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"error": "game_id is required"}), 400

    stats = fetch_time_played_stats(game_id[:3])
    return (
        jsonify(
            stats
            or {"total_players": 0, "total_minutes": 0, "avg_minutes_per_player": 0}
        ),
        200,
    )


@auth_routes_bp.route("/time_played", endpoint="time_played")
def time_played():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)
    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400
    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    sort_by = request.args.get("sort", "time_played")
    if sort_by not in ("time_played", "times_played", "last_played"):
        sort_by = "time_played"

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_time_played(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    results = fetch_time_played(
        serial_prefixes, sort_by=sort_by, limit=per_page, offset=offset
    )
    return render_template(
        "time_played.html",
        time_played=results,
        serial_prefix=", ".join(serial_prefixes),
        user_info=user_info,
        viewed_user=user_info,
        sort_by=sort_by,
        base_url=None,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/favorites", endpoint="favorites")
def favorites():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)

    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    if not serial_has_time_played(serial_prefixes):
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_bookmarks(serial_prefixes)
    total_pages = (total_count + per_page - 1) // per_page

    games = fetch_favorites(serial_prefixes, limit=per_page, offset=offset)
    return render_template(
        "favorites.html",
        games=games,
        user_info=user_info,
        viewed_user=user_info,
        is_unclaimed=False,
        base_url=None,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/discover", endpoint="discover")
def discover():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()
    serial_prefixes = get_serial_prefixes(profile)

    if not serial_prefixes:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    game = find_game_recommendation(serial_prefixes)
    return render_template("discover.html", user_info=user_info, game=game)


@auth_routes_bp.route("/polls", endpoint="polls")
def polls():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    profile = get_user_profile()
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_user_polls(wii_numbers, db_url=config.evc_db_url)
    total_pages = (total_count + per_page - 1) // per_page

    polls_data = fetch_user_polls(
        wii_numbers, limit=per_page, offset=offset, db_url=config.evc_db_url
    )
    return render_template(
        "polls.html",
        polls=polls_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/suggestions", endpoint="suggestions")
def suggestions():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_user_suggestions(wii_numbers, db_url=config.evc_db_url)
    total_pages = (total_count + per_page - 1) // per_page

    suggestions_data = fetch_user_suggestions(
        wii_numbers, limit=per_page, offset=offset, db_url=config.evc_db_url
    )
    return render_template(
        "suggestions.html",
        suggestions=suggestions_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


@auth_routes_bp.route("/contest_submissions", endpoint="contest_submissions")
def contest_submissions():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("auth_routes.index"))
    user_info = get_logged_in_user_info()

    wii_numbers = user_info.get("linked_wii_no", [])
    if isinstance(wii_numbers, str):
        wii_numbers = [wii_numbers]

    if not wii_numbers:
        return render_template("errors/not_linked.html", user_info=user_info), 400

    page = parse_int(request.args.get("page", "1"))
    if page < 1:
        page = 1
    per_page = 30
    offset = (page - 1) * per_page

    total_count = count_contest_submissions(wii_numbers)
    total_pages = (total_count + per_page - 1) // per_page

    submissions_data = fetch_contest_submissions(
        wii_numbers, limit=per_page, offset=offset
    )

    for submission in submissions_data:
        if submission.get("mii_data"):
            submission["mii_image_url"] = render_mii_to_url(submission["mii_data"])
        else:
            submission["mii_image_url"] = None

    artisan_ids = get_artisan_ids_from_wii_number(wii_numbers[0]) if wii_numbers else []

    return render_template(
        "contest_submissions.html",
        submissions=submissions_data,
        user_info=user_info,
        viewed_user=user_info,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        artisan_id=artisan_ids[0] if artisan_ids else None,
    )


@auth_routes_bp.route("/private/takeout", endpoint="takeout")
def takeout():
    if not (oidc and oidc.user_loggedin):
        return redirect(url_for("auth_routes.index"))

    user_info = get_logged_in_user_info()
    profile = get_user_profile()
    serial_prefixes = get_serial_prefixes(profile)
    wii_numbers = user_info.get("linked_wii_no", [])
    email = profile.get("email") if profile else None

    can_export, next_available = can_export_data()
    days_until_next_export = None
    if next_available:
        time_diff = next_available - datetime.now()
        days_until_next_export = time_diff.days + (1 if time_diff.seconds > 0 else 0)

    cam_orders = []
    if profile and profile.get("email"):
        cam_orders = fetch_orders_by_email(profile["email"])

    data_counts = {
        "recommendations": count_recommendations(serial_prefixes),
        "favorites": count_bookmarks(serial_prefixes),
        "time_played": count_time_played(serial_prefixes),
        "polls": count_user_polls(wii_numbers),
        "suggestions": count_user_suggestions(wii_numbers),
        "contest_submissions": count_contest_submissions(wii_numbers),
        "cam_orders": len(cam_orders),
    }

    return render_template(
        "takeout.html",
        user_info=user_info,
        data_counts=data_counts,
        can_export=can_export,
        days_until_next_export=days_until_next_export,
    )


@auth_routes_bp.route(
    "/private/takeout/export", endpoint="takeout_export", methods=["POST"]
)
def takeout_export():
    if not (oidc and oidc.user_loggedin):
        return redirect(url_for("auth_routes.index"))

    # Check rate limiting
    can_export, _ = can_export_data()
    if not can_export:
        flash(
            "You can only export data once per month. Please try again later.", "error"
        )
        return redirect(url_for("auth_routes.takeout"))

    user_info = get_logged_in_user_info()
    profile = get_user_profile()
    serial_prefixes = get_serial_prefixes(profile)
    wii_numbers = user_info.get("linked_wii_no", [])
    email = profile.get("email") if profile else None

    # Get requested exports from form
    requested_exports = request.form.getlist("data_type")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        exported_items = []

        # Recommendations
        if "recommendations" in requested_exports:
            recommendations = fetch_recommendations(serial_prefixes)
            if recommendations:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(
                    csv_buffer, fieldnames=recommendations[0].keys()
                )
                writer.writeheader()
                writer.writerows(recommendations)
                zip_file.writestr("recommendations.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Recommendations: {len(recommendations)} entries"
                )

        # Favorites
        if "favorites" in requested_exports:
            favorites = fetch_favorites(serial_prefixes, limit=10000)
            if favorites:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=favorites[0].keys())
                writer.writeheader()
                writer.writerows(favorites)
                zip_file.writestr("favorites.csv", csv_buffer.getvalue())
                exported_items.append(f"Favorites: {len(favorites)} entries")

        # Time Played
        if "time_played" in requested_exports:
            time_played = fetch_time_played(serial_prefixes, limit=10000)
            if time_played:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=time_played[0].keys())
                writer.writeheader()
                writer.writerows(time_played)
                zip_file.writestr("time_played.csv", csv_buffer.getvalue())
                exported_items.append(f"Time Played: {len(time_played)} entries")

        # Polls
        if "polls" in requested_exports:
            polls = fetch_user_polls(wii_numbers, limit=10000)
            if polls:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=polls[0].keys())
                writer.writeheader()
                writer.writerows(polls)
                zip_file.writestr("polls.csv", csv_buffer.getvalue())
                exported_items.append(f"Polls: {len(polls)} entries")

        # Suggestions
        if "suggestions" in requested_exports:
            suggestions = fetch_user_suggestions(wii_numbers, limit=10000)
            if suggestions:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=suggestions[0].keys())
                writer.writeheader()
                writer.writerows(suggestions)
                zip_file.writestr("suggestions.csv", csv_buffer.getvalue())
                exported_items.append(f"Suggestions: {len(suggestions)} entries")

        # Contest Submissions
        if "contest_submissions" in requested_exports:
            submissions = fetch_contest_submissions(wii_numbers, limit=10000)
            if submissions:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=submissions[0].keys())
                writer.writeheader()
                writer.writerows(submissions)
                zip_file.writestr("contest_submissions.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Contest Submissions: {len(submissions)} entries"
                )

        # Digicam
        if "cam_orders" in requested_exports:
            cam_orders = fetch_orders_by_email(email) if email else []
            if cam_orders:
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=cam_orders[0].keys())
                writer.writeheader()
                writer.writerows(cam_orders)
                zip_file.writestr("cam_orders.csv", csv_buffer.getvalue())
                exported_items.append(
                    f"Digicam Prints Orders: {len(cam_orders)} entries"
                )

        # Create summary file
        summary = f"""WiiLink Takeout Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Username: {user_info.get('username', 'Unknown')}

Exported Data:
"""
        for item in exported_items:
            summary += f"  - {item}\n"

        zip_file.writestr("EXPORT_SUMMARY.txt", summary)

    zip_buffer.seek(0)
    response = make_response(
        send_file(
            zip_buffer,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"checkout_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
    )

    # Set cookie for rate limiting (I know this is easily bypassed by clearing cookies, but it's just a dissuasive measure)
    response.set_cookie(
        "last_takeout_export",
        datetime.now().isoformat(),
        max_age=2592000,
        httponly=True,
        secure=True,
        samesite="Lax",
    )

    return response


@auth_routes_bp.route("/", endpoint="index")
def index():
    if oidc and oidc.user_loggedin:
        profile = get_user_profile()
        user_info = get_logged_in_user_info()
        serial_prefixes = get_serial_prefixes(profile)

        if not serial_prefixes:
            return render_template("errors/not_linked.html", user_info=user_info), 400

        # Refresh the logged-in user's points and achievements from live metrics.
        if user_info and user_info.get("linked_wii_no"):
            own_user = find_user_by_wii_number(user_info["linked_wii_no"][0])
            if own_user:
                fresh_payload, _ = refresh_achievements_for_user(own_user, force=True)
                if fresh_payload:
                    user_info["achievements"] = fresh_payload

        latest_games = fetch_user_latest_games(serial_prefixes, 6)
        latest_favorites = fetch_favorites(serial_prefixes, 5)
        latest_reviews = fetch_user_latest_reviews(serial_prefixes, 6)
        user_stats = fetch_user_stats(serial_prefixes)

        user_counts = {
            "favorites": count_bookmarks(serial_prefixes),
            "games_played": count_time_played(serial_prefixes),
        }

        # Get user's wii numbers for contests and polls
        wii_numbers = user_info.get("linked_wii_no", [])
        if isinstance(wii_numbers, str):
            wii_numbers = [wii_numbers]

        if wii_numbers:
            user_counts["polls"] = count_user_polls(wii_numbers)
            user_counts["suggestions"] = count_user_suggestions(wii_numbers)
            user_counts["contest_submissions"] = count_contest_submissions(wii_numbers)
        else:
            user_counts["polls"] = 0
            user_counts["suggestions"] = 0
            user_counts["contest_submissions"] = 0

        recent_contests = (
            fetch_contest_submissions(wii_numbers, limit=3) if wii_numbers else []
        )
        recent_polls = (
            fetch_user_polls(wii_numbers, limit=3, db_url=config.evc_db_url)
            if wii_numbers
            else []
        )

        if wii_numbers:
            user_counts["polls"] = count_user_polls(wii_numbers)
            user_counts["suggestions"] = count_user_suggestions(wii_numbers)
            user_counts["contest_submissions"] = count_contest_submissions(wii_numbers)
        else:
            user_counts["polls"] = 0
            user_counts["suggestions"] = 0
            user_counts["contest_submissions"] = 0

        # Render Mii images for recent contests
        for submission in recent_contests:
            if submission.get("mii_data"):
                submission["mii_image_url"] = render_mii_to_url(submission["mii_data"])
            else:
                submission["mii_image_url"] = None

        # Fetch latest digicard
        latest_digicard = None
        email = profile.get("email") if profile else None
        if email:
            orders = fetch_orders_by_email(email)
            if orders:
                latest_order = orders[0]
                image_base64 = render_card_to_image(latest_order)
                if image_base64:
                    card_info = get_card_name(latest_order.get("order_schema", ""))
                    latest_digicard = {
                        "order_id": latest_order["order_id"],
                        "date_created": latest_order["date_created"],
                        "image_base64": image_base64,
                        "card_info": card_info,
                    }

        return render_template(
            "home.html",
            user_info=user_info,
            viewed_user=user_info,
            latest_games=latest_games,
            latest_favorites=latest_favorites,
            latest_reviews=latest_reviews,
            user_stats=user_stats,
            user_counts=user_counts,
            recent_contests=recent_contests,
            recent_polls=recent_polls,
            latest_digicard=latest_digicard,
        )
    else:
        return render_template("login.html", user_info=None)
