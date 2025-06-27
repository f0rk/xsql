try:
    from psycopg2 import Error as PGError
except ImportError:
    class PGError(Exception):
        pass

try:
    from psycopg2.errors import QueryCanceled
except ImportError:
    class QueryCanceled(Exception):
        pass


class QuitException(Exception):
    pass


def is_cancel_exception(exc):
    if isinstance(exc, QueryCanceled):
        return True
    return False
