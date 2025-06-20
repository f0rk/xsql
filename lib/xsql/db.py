from sqlalchemy import create_engine, text

from .config import config


def make_engine(url):

    create_engine_args = {}

    engine = create_engine(
        url,
        **create_engine_args,
    )

    return engine


def connect(args):
    engine = make_engine(args.url)
    conn = engine.connect()

    if conn.dialect.name == "snowflake":
        if config.isolation_level == "AUTOCOMMIT":
            conn.execute(text("ALTER SESSION SET AUTOCOMMIT = TRUE"))
    else:
        conn = conn.execution_options(isolation_level=config.isolation_level)

    return conn
