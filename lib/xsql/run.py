import os
import re
import sys
import tempfile

from prompt_toolkit.buffer import Buffer
from sqlalchemy import text

from .config import config, set_extended_display, set_timing
from .exc import QuitException
from .history import history
from .output import write


def get_metacommand(command):

    if not command:
        return False

    if command == "help":
        command = "\\??"

    if is_maybe_metacommand(command):
        match = re.search(r"^\s*\\([a-z?+]+)(?:\s+(.+))?$", command)
        return match

    return False


def is_maybe_metacommand(command):
    if command.strip() == "help":
        return True

    return command.strip().startswith("\\")


def resolve_options(func):
    def wrapper(*args, **kwargs):
        if "output" in kwargs and kwargs["output"] is None:
            kwargs["output"] = sys.stdout
        if "autocommit" in kwargs and kwargs["autocommit"] is None:
            kwargs["autocommit"] = config.autocommit

        return func(*args, **kwargs)

    return wrapper


def strip(v):
    if v is not None:
        v = v.strip()

    return v


def glob_to_like(v):
    if not v:
        return v

    return v.replace("*", "%")


@resolve_options
def run_command(conn, command, output=None, autocommit=None, title=None):

    if isinstance(command, str) and is_maybe_metacommand(command):
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
        if isinstance(command, str):
            command = text(command)

        results = conn.execute(command.execution_options(autocommit=autocommit))
        try:
            write(results, title=title, show_rowcount=True)
        except BrokenPipeError:
            pass


@resolve_options
def run_file(conn, file, output=None, autocommit=None):

    with open(file, "rt") as fp:
        query = fp.read()

    results = conn.execute(text(query).execution_options(autocommit=autocommit))
    try:
        write(results)
    except BrokenPipeError:
        pass


@resolve_options
def run_metacommand(conn, metacommand, rest, output=None, autocommit=None):
    if metacommand == "i":
        run_file(conn, strip(rest), output=output, autocommit=autocommit)
    elif metacommand == "e":
        query = run_editor(rest)
        run_command(
            conn,
            query,
            output=output,
            autocommit=autocommit,
        )
    elif metacommand == "d":
        metacommand_describe(
            conn,
            strip(rest),
            output=output,
            autocommit=autocommit,
        )
    elif metacommand == "dt":
        metacommand_describe_tables(
            conn,
            strip(rest),
            output=output,
            autocommit=autocommit,
        )
    elif metacommand == "?":
        metacommand_help(output=output)
    elif metacommand == "??":
        metacommand_help_main(output=output)
    elif metacommand == "q":
        raise QuitException()
    elif metacommand == "timing":
        if not rest:
            value = not config.timing
        else:
            if rest in ("on", "true"):
                value = True
            elif rest in ("off", "false"):
                value = False
            else:
                handle_invalid_command_value(
                    metacommand,
                    rest,
                    expected="Boolean expected",
                    output=output,
                )
                set_timing(config.timing)
                return

        set_timing(value)
    elif metacommand == "x":
        if not rest:
            value = not config.extended_display
        else:
            if rest in ("on", "true"):
                value = True
            elif rest in ("off", "false"):
                value = False
            else:
                handle_invalid_command_value(
                    metacommand,
                    rest,
                    expected="Boolean expected",
                    output=output,
                )
                set_extended_display(config.extended_display)
                return

        set_extended_display(value)
    else:
        handle_invalid_command(metacommand, output)


@resolve_options
def handle_invalid_command(command, output=None):
    output.write("invalid command ")
    output.write(command)
    output.write("\n")
    output.write("Try \\? for help.\n")


@resolve_options
def handle_invalid_command_value(command, value, expected=None, output=None):
    output.write('unrecognized value "{}" for "\\{}"'.format(value, command))
    if expected:
        output.write(": ")
        output.write(expected)
    output.write("\n")


@resolve_options
def metacommand_help(output=None):
    output.write("Input/Output\n")
    output.write("  \\i FILE                execute commands from file\n")


@resolve_options
def metacommand_help_main(output=None):
    output.write("You are using xsql, the command-line interface to any database.\n")
    output.write("Type:  \\? for help with xsql commands\n")
    output.write("       \\q to quit\n")


@resolve_options
def metacommand_describe(conn, target, output=None, autocommit=None):
    if not target:
        metacommand_describe_tables(
            conn,
            target,
            output=output,
            autocommit=autocommit,
        )
    else:
        query = """
        select
            information_schema.columns.column_name as "Column",
            information_schema.columns.data_type as "Type",
            coalesce(information_schema.columns.collation_name, '') as "Collation",
            case
                when information_schema.columns.is_nullable = 'NO' then 'not null'
                else ''
            end as "Nullable",
            coalesce(information_schema.columns.column_default, '') as "Default"
        from
            information_schema.columns
        where
            information_schema.columns.table_name ilike :table_name
        order by
            information_schema.columns.ordinal_position
        """

        query = text(query).bindparams(table_name=glob_to_like(target))

        run_command(
            conn,
            query,
            output=output,
            autocommit=autocommit,
        )


@resolve_options
def metacommand_describe_tables(conn, target, output=None, autocommit=None):
    query = """
    select
        information_schema.tables.table_schema as "Schema",
        information_schema.tables.table_name as "Name",
        case
            when information_schema.tables.table_type = 'BASE TABLE' then 'table'
            when information_schema.tables.table_type = 'VIEW' then 'view'
            else 'other'
        end as "Type",
        null::text as "Owner"
    from
        information_schema.tables
    where
        :target is null
        or (information_schema.tables.table_schema || '.' || information_schema.tables.table_name) ilike :target
        or information_schema.tables.table_name ilike :target
    order by
        information_schema.tables.table_schema,
        information_schema.tables.table_name
    """

    if not target:
        target = None

    target = glob_to_like(target)

    query = text(query).bindparams(target=target)

    run_command(
        conn,
        query,
        output=output,
        autocommit=autocommit,
        title="List of relations",
    )


def run_editor(text):
    filename = None
    add_to_history = False

    if not text:
        add_to_history = True
        for entry in history.load_history_strings():
            if entry.strip().startswith("\e"):
                continue

            text = entry
            break
    else:
        filename = text

    if filename is None:
        descriptor, filename = tempfile.mkstemp(".sql")

        os.write(descriptor, text.encode("utf-8"))
        os.close(descriptor)

        def cleanup():
            os.unlink(filename)
    else:
        cleanup = None

    try:
        success = Buffer._open_file_in_editor(None, filename)

        if success:
            with open(filename, "rb") as f:
                text = f.read().decode("utf-8")

                if text.endswith("\n"):
                    text = text[:-1]

                if add_to_history:
                    history.append_string(text)

                return text
    finally:
        if cleanup is not None:
            cleanup()

    return ""
