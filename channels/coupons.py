import json

import config
from psycopg2.extras import Json
from utils.utils import _run_query, _execute


def _db_url(db_url=None):
    return db_url or getattr(config, "checkout_db_url", None)


def fetch_coupon_by_code(coupon_code, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not coupon_code:
        return None
    query = "SELECT * FROM coupons WHERE coupon_code = %s LIMIT 1"
    rows = _run_query(query, [coupon_code], db_url, use_cache=False)
    return rows[0] if rows else None


def fetch_coupon_by_uuid(coupon_uuid, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not coupon_uuid:
        return None
    query = "SELECT * FROM coupons WHERE uuid = %s LIMIT 1"
    rows = _run_query(query, [coupon_uuid], db_url, use_cache=False)
    return rows[0] if rows else None


def user_redeemed(coupon_uuid, username, db_url=None):
    """True if this user has already redeemed the coupon."""
    db_url = _db_url(db_url)
    if not db_url or not coupon_uuid or not username:
        return False
    query = """
        SELECT 1 FROM coupon_redemptions
        WHERE coupon_uuid = %s AND redeemed_by = %s
        LIMIT 1
    """
    rows = _run_query(query, [coupon_uuid, username], db_url, use_cache=False)
    return bool(rows)


def coupon_available(coupon, username=None, db_url=None):
    """Return (ok, reason). A user can only redeem a coupon once; max_uses of -1
    means unlimited total uses (still once per user)."""
    if username:
        if user_redeemed(coupon["uuid"], username, db_url):
            return False, "You have already redeemed this coupon."
    if coupon.get("max_uses") == -1:
        return True, None
    used = coupon.get("uses_count", 0) or 0
    if used >= coupon.get("max_uses", 1):
        return False, "This coupon has already been fully used."
    return True, None


def consume_coupon(coupon_uuid, redeemed_by, db_url=None):
    """Atomically mark a coupon as redeemed by a user (once per user) and record it."""
    db_url = _db_url(db_url)
    if not db_url:
        return False
    query = """
        WITH updated AS (
            UPDATE coupons
            SET uses_count = uses_count + 1
            WHERE uuid = %s
              AND (max_uses = -1 OR uses_count < max_uses)
              AND NOT EXISTS (
                  SELECT 1 FROM coupon_redemptions
                  WHERE coupon_uuid = coupons.uuid AND redeemed_by = %s
              )
            RETURNING uuid
        )
        INSERT INTO coupon_redemptions (coupon_uuid, redeemed_by)
        SELECT uuid, %s FROM updated
        RETURNING coupon_uuid
    """
    rows = _run_query(
        query,
        [coupon_uuid, redeemed_by, redeemed_by],
        db_url,
        use_cache=False,
    )
    return bool(rows)


def refund_coupon(coupon_uuid, redeemed_by, db_url=None):
    """Undo a redemption: decrement uses and remove the history record."""
    db_url = _db_url(db_url)
    if not db_url or not redeemed_by:
        return
    _execute(
        "UPDATE coupons SET uses_count = GREATEST(uses_count - 1, 0) WHERE uuid = %s",
        [coupon_uuid],
        db_url,
    )
    _execute(
        "DELETE FROM coupon_redemptions WHERE coupon_uuid = %s AND redeemed_by = %s",
        [coupon_uuid, redeemed_by],
        db_url,
    )


def user_redeem_history(username, db_url=None):
    db_url = _db_url(db_url)
    if not db_url or not username:
        return []
    query = """
        SELECT c.coupon_code, c.issuer, c.redeemables, r.redeemed_at
        FROM coupon_redemptions r
        JOIN coupons c ON c.uuid = r.coupon_uuid
        WHERE r.redeemed_by = %s
        ORDER BY r.redeemed_at DESC
    """
    return _run_query(query, [username], db_url, use_cache=False)


def list_coupons(db_url=None):
    db_url = _db_url(db_url)
    if not db_url:
        return []
    query = """
        SELECT c.uuid, c.coupon_code, c.issuer, c.issued_at, c.max_uses, c.uses_count,
               c.redeemables,
               (SELECT COUNT(*) FROM coupon_redemptions r WHERE r.coupon_uuid = c.uuid)
                   AS redeemer_count
        FROM coupons c
        ORDER BY c.issued_at DESC
    """
    return _run_query(query, [], db_url, use_cache=False)


def create_coupon(coupon_code, issuer, redeemables, max_uses, db_url=None):
    db_url = _db_url(db_url)
    if not db_url:
        return False
    query = """
        INSERT INTO coupons (coupon_code, issuer, redeemables, max_uses)
        VALUES (%s, %s, %s, %s)
    """
    return (
        _execute(
            query,
            [coupon_code, issuer, Json(redeemables), int(max_uses)],
            db_url,
        )
        == 1
    )


def delete_coupon(coupon_uuid, db_url=None):
    db_url = _db_url(db_url)
    if not db_url:
        return False
    return _execute("DELETE FROM coupons WHERE uuid = %s", [coupon_uuid], db_url) > 0
