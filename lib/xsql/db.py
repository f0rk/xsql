from sqlalchemy import create_engine

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
    conn = engine.connect().execution_options(isolation_level=config.isolation_level)

    return conn
