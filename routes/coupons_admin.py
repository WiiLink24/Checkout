from functools import wraps
from datetime import datetime, timezone

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

from channels.coupons import (
    list_coupons,
    create_coupon,
    delete_coupon,
    _is_expired,
)
from routes.auth import get_logged_in_user_info
from utils.theme import get_theme_catalog

COUPON_ADMIN_GROUP_UUID = getattr(config, "coupon_admin_group_uuid", "")

coupons_admin_bp = Blueprint("coupons_admin", __name__, url_prefix="/coupons/admin")


def coupon_admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user_info = get_logged_in_user_info()
        if not user_info:
            abort(401)
        groups = user_info.get("groups") or []
        if COUPON_ADMIN_GROUP_UUID not in groups:
            abort(403)
        return f(*args, **kwargs)

    return decorated


@coupons_admin_bp.route("/", methods=["GET", "POST"])
@coupon_admin_required
def panel():
    user_info = get_logged_in_user_info()
    catalog = get_theme_catalog()

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "create":
            coupon_code = (request.form.get("coupon_code") or "").strip().upper()
            issuer = (request.form.get("issuer") or "WiiLink").strip()
            try:
                max_uses = int(request.form.get("max_uses", "1"))
            except ValueError:
                max_uses = 1
            points_raw = (request.form.get("points") or "").strip()
            theme_id = (request.form.get("theme") or "").strip()

            redeemables = []
            if points_raw:
                try:
                    points = int(points_raw)
                except ValueError:
                    points = 0
                if points > 0:
                    redeemables.append({"type": "points", "value": points})
            if theme_id:
                redeemables.append({"type": "theme", "value": theme_id})

            if not coupon_code or not redeemables:
                flash("A coupon code and at least one reward are required.")
            else:
                expires_at = None
                expires_raw = (request.form.get("expires_at") or "").strip()
                if expires_raw:
                    try:
                        parsed = datetime.fromisoformat(expires_raw)
                        # datetime-local has no timezone: assume server-local time
                        if parsed.tzinfo is None:
                            parsed = parsed.astimezone()
                        expires_at = parsed.astimezone(timezone.utc)
                    except ValueError:
                        flash("Invalid expiration date; coupon not created.")
                        return redirect(url_for("coupons_admin.panel"))
                if create_coupon(
                    coupon_code, issuer, redeemables, max_uses, expires_at
                ):
                    flash(f"Coupon {coupon_code} created.")
                else:
                    flash("Failed to create coupon.")

        elif action == "delete":
            coupon_uuid = (request.form.get("coupon_uuid") or "").strip()
            if delete_coupon(coupon_uuid):
                flash("Coupon deleted.")
            else:
                flash("Failed to delete coupon.")

        return redirect(url_for("coupons_admin.panel"))

    themes = [theme for tid, theme in catalog.items() if tid != "default"]
    coupons = list_coupons()
    for coupon in coupons:
        coupon["is_expired"] = _is_expired(coupon)
    return render_template(
        "coupons_admin.html",
        user_info=user_info,
        coupons=coupons,
        themes=themes,
    )
