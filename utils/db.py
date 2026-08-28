import config
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    create_engine,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

engine = create_engine(
    getattr(config, "checkout_db_url", config.db_url),
    pool_pre_ping=True,
)
SessionLocal = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))


def init_db(app):
    """Bind the scoped session lifetime to the Flask request context."""
    app.teardown_appcontext(_remove_session)


def _remove_session(exception=None):
    SessionLocal.remove()


class Base(DeclarativeBase):
    pass


class Coupon(Base):
    __tablename__ = "coupons"

    uuid = Column(
        UUID(as_uuid=False),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    coupon_code = Column(Text, nullable=False)
    issuer = Column(Text, nullable=False)
    issued_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    redeemables = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    max_uses = Column(Integer, nullable=False, server_default=text("1"))
    uses_count = Column(Integer, nullable=False, server_default=text("0"))
    expires_at = Column(TIMESTAMP(timezone=True), nullable=True)


class CouponRedemption(Base):
    __tablename__ = "coupon_redemptions"

    id = Column(
        UUID(as_uuid=False),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    coupon_uuid = Column(UUID(as_uuid=False), nullable=False)
    redeemed_by = Column(Text, nullable=False)
    redeemed_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class Friend(Base):
    __tablename__ = "friends"

    follower_wii_number = Column(String(20), primary_key=True)
    followed_wii_number = Column(String(20), primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
