from alembic import context
from sqlalchemy import create_engine, pool

import config as app_config
from utils.db import db

config = context.config

target_metadata = db.metadata

url = getattr(app_config, "checkout_db_url", None) or app_config.db_url


_autogenerating = bool(getattr(config.cmd_opts, "autogenerate", False))
if _autogenerating and not target_metadata.tables:
    raise RuntimeError(
        "utils/db.py loaded no models — refusing to autogenerate a migration "
        "that would drop existing tables."
    )


def run_migrations_offline():
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = create_engine(url, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
