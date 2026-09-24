import json
import logging

import config
from pywebpush import WebPushException, webpush

from utils.db import NotificationPreference, PushSubscription, db

logger = logging.getLogger(__name__)

# Categories a notification can target
CATEGORIES = frozenset(
    {
        "nintendo_channel",
        "evc",
        "cmoc",
        "wii_room",
        "general_announcements",
        "critical_announcements",
        "newsletter_announcements",
        "food_channel",
        "digicam_prints",
        "kirby_tv_channel",
    }
)


def _payload(title, body, url, tag, icon=None):
    return {
        "title": title or "WiiLink Checkout",
        "body": body or "",
        "url": url or "/",
        "tag": tag,
        # The service worker falls back to the Checkout PWA icon when empty.
        "icon": icon or "",
    }


def _category_disabled_usernames(category):
    rows = db.session.execute(
        db.select(NotificationPreference.username).where(
            NotificationPreference.category == category,
            NotificationPreference.enabled.is_(False),
        )
    ).scalars()
    return set(rows)


def _deliver(subscription, payload):
    """Send one notification; returns (delivered, stale)."""
    try:
        webpush(
            subscription_info={
                "endpoint": subscription.endpoint,
                "keys": {"p256dh": subscription.p256dh, "auth": subscription.auth},
            },
            data=json.dumps(payload),
            vapid_private_key=config.vapid_private_key,
            vapid_claims={"sub": config.vapid_subject},
        )
        return True, False
    except WebPushException as exc:
        status = getattr(exc.response, "status_code", None)
        # 404/410 means the subscription is gone (browser unsubscribed/expired).
        return False, status in (404, 410)
    except Exception:
        logger.exception("Unexpected error pushing to %s", subscription.endpoint)
        return False, False


def _prune(endpoint):
    db.session.execute(
        db.delete(PushSubscription).where(PushSubscription.endpoint == endpoint)
    )
    db.session.commit()


def send_push_to_user(
    username, title, body="", url="/", tag=None, category=None, icon=None
):
    if category:
        pref = db.session.execute(
            db.select(NotificationPreference).where(
                NotificationPreference.username == username,
                NotificationPreference.category == category,
            )
        ).scalar_one_or_none()
        if pref is not None and not pref.enabled:
            return 0

    payload = _payload(title, body, url, tag, icon)
    subscriptions = (
        db.session.execute(
            db.select(PushSubscription).where(PushSubscription.username == username)
        )
        .scalars()
        .all()
    )

    sent = 0
    for subscription in subscriptions:
        delivered, stale = _deliver(subscription, payload)
        if stale:
            _prune(subscription.endpoint)
        if delivered:
            sent += 1
    return sent


def send_push_to_all(title, body="", url="/", tag=None, category=None, icon=None):
    payload = _payload(title, body, url, tag, icon)
    subscriptions = db.session.execute(db.select(PushSubscription)).scalars().all()

    skipped = _category_disabled_usernames(category) if category else set()

    sent = 0
    for subscription in subscriptions:
        if subscription.username in skipped:
            continue
        delivered, stale = _deliver(subscription, payload)
        if stale:
            _prune(subscription.endpoint)
        if delivered:
            sent += 1
    return sent
