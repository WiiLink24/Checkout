"""Routes for digicard functionality."""

from flask import Blueprint, render_template, redirect, url_for
from utils.auth import get_user_profile
from channels.digi import fetch_orders_by_email, render_card_to_image, get_card_name

digicard_bp = Blueprint("digicard", __name__)
oidc = None


def set_oidc(oidc_instance):
    global oidc
    oidc = oidc_instance


def get_logged_in_user_info():
    from app import get_logged_in_user_info as get_user

    return get_user()


@digicard_bp.route("/private/digicard", endpoint="private_digicard", methods=["GET"])
def private_digicard():
    if not oidc or not oidc.user_loggedin:
        return redirect(url_for("index"))

    user_info = get_logged_in_user_info()

    if not user_info:
        return redirect(url_for("index"))

    # Get email from OIDC profile
    profile = get_user_profile()
    email = profile.get("email")

    # Fetch orders from cam_server
    orders = fetch_orders_by_email(email)

    if not orders:
        return render_template(
            "digicard.html", cards=[], user_info=user_info, email=email, has_cards=False
        )

    # Render each card and prepare card data
    cards_data = []
    for order in orders:
        image_base64 = render_card_to_image(order)

        if image_base64:
            card_info = get_card_name(order.get("order_schema", ""))
            cards_data.append(
                {
                    "order_id": order["order_id"],
                    "date_created": order["date_created"],
                    "image_base64": image_base64,
                    "card_info": card_info,
                }
            )

    return render_template(
        "digicard.html",
        cards=cards_data,
        user_info=user_info,
        email=email,
        has_cards=len(cards_data) > 0,
    )
