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


try:
    from snowflake.connector.errors import ProgrammingError as SnowflakeProgrammingError
except ImportError:
    class SnowflakeProgrammingError(Exception):
        pass


try:
    from snowflake.connector.errors import Error as SnowflakeError
except ImportError:
    class SnowflakeError(Exception):
        pass


try:
    from snowflake.connector.network import ReauthenticationRequest as SnowflakeReauthenticationRequest
except ImportError:
    class SnowflakeReauthenticationRequest(Exception):
        pass


class QuitException(Exception):
    pass


def is_cancel_exception(exc):
    if isinstance(exc, QueryCanceled):
        return True
    return False
