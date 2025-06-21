import importlib
import os
import sys

from .config import config


def translate(conn, query, from_=None, to=None):

    translate_dir = os.path.expanduser("~/.xsql/")
    if not os.path.exists(translate_dir):
        return query

    if from_ is None:
        from_ = config.translate_from
    else:
        from_ = conn.dialect.name

    if to is None:
        to = config.translate_to
    else:
        to = conn.dialect.name

    if from_ == to:
        return query

    if translate_dir not in sys.path:
        sys.path.append(translate_dir)

    try:
        try:
            sys.path.insert(0, translate_dir)
            mod = importlib.import_module("translate")
        finally:
            sys.path.remove(translate_dir)
    except ImportError:
        sys.stdout.write("xsql: error: unable to import translator, but translation requested\n")
        sys.stdout.flush()
        return None

    return mod.translate(
        from_,
        to,
        conn,
        query,
        config.variables.get("translate_options"),
    )
