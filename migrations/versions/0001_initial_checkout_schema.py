"""Initial checkout schema: coupons, coupon_redemptions, friends, theme_purchases

Revision ID: 0001
Revises:
Create Date: 2026-08-30

"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS coupons (
                uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                coupon_code TEXT NOT NULL,
                issuer TEXT NOT NULL,
                issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                redeemables JSONB NOT NULL DEFAULT '[]'::jsonb,
                max_uses INTEGER NOT NULL DEFAULT 1,
                uses_count INTEGER NOT NULL DEFAULT 0,
                expires_at TIMESTAMPTZ
            );

            ALTER TABLE coupons ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

            CREATE TABLE IF NOT EXISTS coupon_redemptions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                coupon_uuid UUID NOT NULL,
                redeemed_by TEXT NOT NULL,
                redeemed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_coupon_redemptions_coupon
                ON coupon_redemptions (coupon_uuid);

            CREATE TABLE IF NOT EXISTS friends (
                follower_wii_number VARCHAR(20) NOT NULL,
                followed_wii_number VARCHAR(20) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (follower_wii_number, followed_wii_number)
            );

            CREATE INDEX IF NOT EXISTS idx_friends_followed
                ON friends (followed_wii_number);

            CREATE TABLE IF NOT EXISTS theme_purchases (
                username TEXT NOT NULL,
                theme_id TEXT NOT NULL,
                cost INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'shop',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (username, theme_id)
            );

            CREATE INDEX IF NOT EXISTS idx_theme_purchases_username
                ON theme_purchases (username);
            """
        )
    )


def downgrade():
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            DROP TABLE IF EXISTS theme_purchases;
            DROP TABLE IF EXISTS friends;
            DROP TABLE IF EXISTS coupon_redemptions;
            DROP TABLE IF EXISTS coupons;
            """
        )
    )
