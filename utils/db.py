from datetime import datetime
from typing import Optional

from datetime import datetime
from pathlib import Path
from typing import Optional

import config
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from alembic import command
from alembic.config import Config


class Base(DeclarativeBase):
    pass


db = SQLAlchemy(model_class=Base)

CHECKOUT_BIND = "checkout"


def init_db(app):
    """Connect the checkout database as a Flask-SQLAlchemy bind."""
    app.config.setdefault("SQLALCHEMY_BINDS", {})
    app.config["SQLALCHEMY_BINDS"][CHECKOUT_BIND] = config.checkout_db_url

    db.init_app(app)
    _run_migrations()


def _run_migrations():
    root = Path(__file__).resolve().parent.parent
    alembic_cfg = Config(str(root / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(root / "migrations"))
    command.upgrade(alembic_cfg, "head")


class Coupon(db.Model):
    __bind_key__ = CHECKOUT_BIND
    __tablename__ = "coupons"

    uuid: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    coupon_code: Mapped[str] = mapped_column(Text)
    issuer: Mapped[str] = mapped_column(Text)
    issued_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    redeemables: Mapped[list] = mapped_column(JSONB, server_default=text("'[]'::jsonb"))
    max_uses: Mapped[int] = mapped_column(Integer, server_default=text("1"))
    uses_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )


class CouponRedemption(db.Model):
    __bind_key__ = CHECKOUT_BIND
    __tablename__ = "coupon_redemptions"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    coupon_uuid: Mapped[str] = mapped_column(UUID(as_uuid=False))
    redeemed_by: Mapped[str] = mapped_column(Text)
    redeemed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )


class Friend(db.Model):
    __bind_key__ = CHECKOUT_BIND
    __tablename__ = "friends"

    follower_wii_number: Mapped[str] = mapped_column(String(16), primary_key=True)
    followed_wii_number: Mapped[str] = mapped_column(String(16), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
