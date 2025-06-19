import re
import sys

from sqlalchemy import text

from .config import config
from .output import write


def get_metacommand(command):

    if not command:
        return False

    if is_maybe_metacommand(command):
        match = re.search(r"^\s*\\([a-z?+]+)(?:\s+(.+))?$", command)
        return match

    return False


def is_maybe_metacommand(command):
    return command.strip().startswith("\\")


def run_command(conn, command, output=None, autocommit=None):

    if autocommit is None:
        autocommit = config.autocommit

    if is_maybe_metacommand(command):
        match = get_metacommand(command)
        if not match:
            handle_invalid_command(command, output)
            return

        metacommand, rest = match.groups()

        run_metacommand(
            conn,
            metacommand,
            rest,
            output=output,
            autocommit=autocommit,
        )
    else:
        results = conn.execute(text(command).execution_options(autocommit=autocommit))
        try:
            write(results)
        except BrokenPipeError:
            pass


def run_file(conn, file, output=None, autocommit=None):

    if autocommit is None:
        autocommit = config.autocommit

    with open(file, "rt") as fp:
        query = fp.read()

    results = conn.execute(text(query).execution_options(autocommit=autocommit))
    try:
        write(results)
    except BrokenPipeError:
        pass


def run_metacommand(conn, metacommand, rest, output=None, autocommit=None):

    if autocommit is None:
        autocommit = config.autocommit

    if metacommand == "i":
        run_file(conn, rest.strip(), output=output, autocommit=autocommit)
    elif metacommand == "?":
        metacommand_help(output)
    else:
        handle_invalid_command(metacommand, output)

def handle_invalid_command(command, output):
    if output is None:
        output = sys.stdout

    output.write("invalid command ")
    output.write(command)
    output.write("\n")
    output.write("Try \\? for help.\n")


def metacommand_help(output):
    if output is None:
        output = sys.stdout

    output.write("Input/Output\n")
    output.write("  \\i FILE                execute commands from file\n")
