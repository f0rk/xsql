from sqlalchemy import text

from .output import write


def run_command(conn, command, output=None, autocommit=False):
    results = conn.execute(text(command).execution_options(autocommit=autocommit))
    try:
        write(results)
    except BrokenPipeError:
        pass


def run_file(conn, file, output=None, autocommit=False):

    with open(file, "rt") as fp:
        query = fp.read()

    results = conn.execute(text(query).execution_options(autocommit=autocommit))
    try:
        write(results)
    except BrokenPipeError:
        pass
