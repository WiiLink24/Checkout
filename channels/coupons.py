from datetime import datetime, timezone

from sqlalchemy import exists, func, or_, select
from utils.db import Coupon, CouponRedemption, SessionLocal


def _is_expired(coupon):
    """True if the coupon has an expires_at in the past (None = never)."""
    expires_at = coupon.get("expires_at") if coupon else None
    if not expires_at:
        return False
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.fromisoformat(expires_at)
        except ValueError:
            return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= datetime.now(timezone.utc)


def _coupon_dict(coupon, redeemer_count=None):
    data = {
        "uuid": coupon.uuid,
        "coupon_code": coupon.coupon_code,
        "issuer": coupon.issuer,
        "issued_at": coupon.issued_at,
        "redeemables": coupon.redeemables,
        "max_uses": coupon.max_uses,
        "uses_count": coupon.uses_count,
        "expires_at": coupon.expires_at,
    }
    if redeemer_count is not None:
        data["redeemer_count"] = redeemer_count
    return data


def fetch_coupon_by_code(coupon_code):
    if not coupon_code:
        return None
    try:
        session = SessionLocal()
        coupon = session.query(Coupon).filter(Coupon.coupon_code == coupon_code).first()
        return _coupon_dict(coupon) if coupon else None
    finally:
        SessionLocal.remove()


def fetch_coupon_by_uuid(coupon_uuid):
    if not coupon_uuid:
        return None
    try:
        session = SessionLocal()
        coupon = session.query(Coupon).filter(Coupon.uuid == coupon_uuid).first()
        return _coupon_dict(coupon) if coupon else None
    finally:
        SessionLocal.remove()


def user_redeemed(coupon_uuid, username):
    """True if this user has already redeemed the coupon."""
    if not coupon_uuid or not username:
        return False
    try:
        session = SessionLocal()
        row = (
            session.query(CouponRedemption.id)
            .filter(
                CouponRedemption.coupon_uuid == coupon_uuid,
                CouponRedemption.redeemed_by == username,
            )
            .first()
        )
        return row is not None
    finally:
        SessionLocal.remove()


def coupon_available(coupon, username=None):
    """Return (ok, reason). Checks expiration, once-per-user, and max_uses
    (max_uses of -1 means unlimited total uses, still once per user)."""
    if _is_expired(coupon):
        return False, "This coupon has expired."
    if username:
        if user_redeemed(coupon["uuid"], username):
            return False, "You have already redeemed this coupon."
    if coupon.get("max_uses") == -1:
        return True, None
    used = coupon.get("uses_count", 0)
    if used >= coupon.get("max_uses", 1):
        return False, "This coupon has already been fully used."
    return True, None


def consume_coupon(coupon_uuid, redeemed_by):
    """Atomically mark a coupon as redeemed by a user (once per user) and record
    it. Refuses expired or exhausted coupons at the SQL level."""
    if not coupon_uuid or not redeemed_by:
        return False
    try:
        session = SessionLocal()
        already_redeemed = exists(
            select(CouponRedemption.id).where(
                CouponRedemption.coupon_uuid == Coupon.uuid,
                CouponRedemption.redeemed_by == redeemed_by,
            )
        )
        updated = (
            session.query(Coupon)
            .filter(
                Coupon.uuid == coupon_uuid,
                or_(
                    Coupon.max_uses == -1,
                    Coupon.uses_count < Coupon.max_uses,
                ),
                or_(
                    Coupon.expires_at.is_(None),
                    Coupon.expires_at > func.now(),
                ),
                ~already_redeemed,
            )
            .update(
                {"uses_count": Coupon.uses_count + 1},
                synchronize_session=False,
            )
        )
        if not updated:
            session.rollback()
            return False
        session.add(CouponRedemption(coupon_uuid=coupon_uuid, redeemed_by=redeemed_by))
        session.commit()
        return True
    finally:
        SessionLocal.remove()


def refund_coupon(coupon_uuid, redeemed_by):
    """Undo a redemption: decrement uses and remove the history record."""
    if not coupon_uuid or not redeemed_by:
        return
    try:
        session = SessionLocal()
        session.query(Coupon).filter(Coupon.uuid == coupon_uuid).update(
            {"uses_count": func.greatest(Coupon.uses_count - 1, 0)},
            synchronize_session=False,
        )
        session.query(CouponRedemption).filter(
            CouponRedemption.coupon_uuid == coupon_uuid,
            CouponRedemption.redeemed_by == redeemed_by,
        ).delete()
        session.commit()
    finally:
        SessionLocal.remove()


def user_redeem_history(username):
    if not username:
        return []
    try:
        session = SessionLocal()
        rows = (
            session.query(Coupon, CouponRedemption.redeemed_at)
            .join(
                CouponRedemption,
                CouponRedemption.coupon_uuid == Coupon.uuid,
            )
            .filter(CouponRedemption.redeemed_by == username)
            .order_by(CouponRedemption.redeemed_at.desc())
            .all()
        )
        return [
            {
                "coupon_code": coupon.coupon_code,
                "issuer": coupon.issuer,
                "redeemables": coupon.redeemables,
                "redeemed_at": redeemed_at,
            }
            for coupon, redeemed_at in rows
        ]
    finally:
        SessionLocal.remove()


def list_coupons():
    try:
        session = SessionLocal()
        rows = (
            session.query(Coupon, func.count(CouponRedemption.id))
            .outerjoin(
                CouponRedemption,
                CouponRedemption.coupon_uuid == Coupon.uuid,
            )
            .group_by(Coupon)
            .order_by(Coupon.issued_at.desc())
            .all()
        )
        return [_coupon_dict(coupon, redeemer_count=count) for coupon, count in rows]
    finally:
        SessionLocal.remove()


def create_coupon(coupon_code, issuer, redeemables, max_uses, expires_at=None):
    session = SessionLocal()
    try:
        session.add(
            Coupon(
                coupon_code=coupon_code,
                issuer=issuer,
                redeemables=redeemables,
                max_uses=int(max_uses),
                expires_at=expires_at,
            )
        )
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        SessionLocal.remove()


def delete_coupon(coupon_uuid):
    try:
        session = SessionLocal()
        deleted = session.query(Coupon).filter(Coupon.uuid == coupon_uuid).delete()
        session.commit()
        return deleted > 0
    finally:
        SessionLocal.remove()
