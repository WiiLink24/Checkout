from functools import wraps

import config
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    abort,
)

from routes.auth import get_logged_in_user_info
from utils.push import CATEGORIES, send_push_to_all, send_push_to_user

NOTIFICATIONS_ADMIN_GROUP_UUID = getattr(config, "coupon_admin_group_uuid", "")

notifications_admin_bp = Blueprint(
    "notifications_admin", __name__, url_prefix="/notifications/admin"
)


def notifications_admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user_info = get_logged_in_user_info()
        if not user_info:
            abort(401)
        groups = user_info.get("groups") or []
        if NOTIFICATIONS_ADMIN_GROUP_UUID not in groups:
            abort(403)
        return f(*args, **kwargs)

    return decorated


def _valid_url(value):
    return (value.startswith("/") and not value.startswith("//")) or value.startswith(
        ("http://", "https://")
    )


@notifications_admin_bp.route("/", methods=["GET", "POST"])
@notifications_admin_required
def panel():
    user_info = get_logged_in_user_info()

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = request.form.get("body") or ""
        username = (request.form.get("username") or "").strip()
        category = (request.form.get("category") or "").strip()
        url = (request.form.get("url") or "").strip() or "/"
        icon = (request.form.get("icon") or "").strip() or None
        tag = (request.form.get("tag") or "").strip() or None

        if not title:
            flash("A notification title is required.")
            return redirect(url_for("notifications_admin.panel"))
        if not _valid_url(url):
            flash("The click URL must be a relative path or an http(s) URL.")
            return redirect(url_for("notifications_admin.panel"))
        if icon and not _valid_url(icon):
            flash("The icon must be a relative path or an http(s) URL.")
            return redirect(url_for("notifications_admin.panel"))

        if category and category not in CATEGORIES:
            flash("Unknown notification category.")
            return redirect(url_for("notifications_admin.panel"))
        category = category or None

        if username:
            sent = send_push_to_user(
                username,
                title=title,
                body=body,
                url=url,
                tag=tag,
                category=category,
                icon=icon,
            )
        else:
            sent = send_push_to_all(
                title=title,
                body=body,
                url=url,
                tag=tag,
                category=category,
                icon=icon,
            )
        flash(f"Notification sent to {sent} device(s).")
        return redirect(url_for("notifications_admin.panel"))

    categories = sorted(CATEGORIES)
    return render_template(
        "notifications_admin.html",
        user_info=user_info,
        categories=categories,
    )
