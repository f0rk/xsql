try:
    from psycopg2 import Error as PGError
except ImportError:
    class PGError(Exception):
        pass


class QuitException(Exception):
    pass


