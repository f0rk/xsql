import sys

import sqlalchemy.exc
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from .alias import load_aliases
from .config import config


class Reconnect(Exception):

    def __init__(self, target):
        self.target = target


def make_engine(url):

    create_engine_args = {}

    engine = create_engine(
        url,
        **create_engine_args,
    )

    return engine


def connect(url):
    engine = make_engine(url)
    conn = engine.connect()

    if conn.dialect.name == "snowflake":
        if config.isolation_level == "AUTOCOMMIT":
            conn.execute(text("ALTER SESSION SET AUTOCOMMIT = TRUE"))
    else:
        conn = conn.execution_options(isolation_level=config.isolation_level)

    return conn


def get_ssl_info(conn):
    if hasattr(conn.connection, "dbapi_connection"):
        if hasattr(conn.connection.dbapi_connection, "info"):
            ssl_in_use = getattr(conn.connection.dbapi_connection.info, "ssl_in_use")

            if ssl_in_use:

                info_obj = conn.connection.dbapi_connection.info

                info = {
                    "protocol": info_obj.ssl_attribute("protocol"),
                    "cipher": info_obj.ssl_attribute("cipher"),
                    "bits": info_obj.ssl_attribute("key_bits"),
                    "compression": info_obj.ssl_attribute("compression"),
                }

                return info

    if conn.dialect.name == "snowflake":
        info = {
            "ocsp mode": conn.connection.dbapi_connection._ocsp_mode().name,
        }

        return info

    return None


def display_ssl_info(conn):

    ssl_info = get_ssl_info(conn)
    if ssl_info:

        ssl_output = ["{}: {}".format(k, v) for k, v in ssl_info.items()]
        ssl_output = " ".join(ssl_output)

        sys.stdout.write(
            "SSL connection ({})\n"
            .format(ssl_output)
        )


def get_server_name(conn):
    return conn.dialect.name


def get_server_version(conn):

    def as_str(version):
        items = []
        for v in version:
            items.append(str(v))
        return ".".join(items)

    if conn.dialect.name in ("postgresql", "redshift"):
        if not conn.dialect.server_version_info:
            version_info = conn.dialect._get_server_version_info()
        else:
            version_info = conn.dialect.server_version_info

        return as_str(version_info)

    elif conn.dialect.name == "snowflake":
        res = conn.execute(text("select current_version()")).fetchone()
        return res[0]
    else:
        if conn.dialect.server_version_info:
            return as_str(conn.dialect.server_version_info)


def resolve_url(target):
    is_url = False
    url = None
    try:
        make_url(target)
        is_url = True
        url = target
    except sqlalchemy.exc.ArgumentError:
        pass

    # not a url, check aliases
    if not is_url:

        aliases = load_aliases()

        if target in aliases:
            url = aliases[target]

    return is_url, url
