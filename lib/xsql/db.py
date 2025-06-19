from sqlalchemy import create_engine


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

    return conn
