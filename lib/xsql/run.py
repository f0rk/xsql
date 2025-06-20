import os
import re
import sys
import time
import tempfile

from prompt_toolkit.buffer import Buffer
from sqlalchemy import text

from .config import (
    config,
    process_command_with_variable,
    set_color,
    set_extended_display,
    set_format,
    set_null_display,
    set_output,
    set_syntax,
    set_timing,
    set_tuples_only,
)
from .exc import QuitException
from .history import history
from .output import write
from .postgres import get_command_status
from .time import write_time


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


def strip(v):
    if v is not None:
        v = v.strip()

    return v


def glob_to_like(v):
    if not v:
        return v

    return v.replace("*", "%")


def run_command(conn, command, title=None):

    if isinstance(command, str) and is_maybe_metacommand(command):
        match = get_metacommand(command)
        if not match:
            handle_invalid_command(command, sys.stderr)
            return

        metacommand, rest = match.groups()

        run_metacommand(
            conn,
            metacommand,
            rest,
        )
    else:

        # XXX: really need to get cursor.statusmessage or pgresult_ptr to work
        status = None

        if isinstance(command, str):
            if match := re.search("^\s*((create|drop)\s+(\w+))", command, flags=re.I):
                status = match.groups()[0]
            elif match := re.search("^\s*(insert)", command, flags=re.I):
                status = match.groups()[0]
            elif match := re.search("^\s*(update)", command, flags=re.I):
                status = match.groups()[0]

            command = text(command)

        if config.autocommit:
            conn.execute(text("begin;"))

        start_time = time.monotonic()

        results = conn.execute(command)

        total_time = time.monotonic() - start_time

        if results.returns_rows:
            try:
                write(results, title=title, show_rowcount=True)
            except BrokenPipeError:
                pass

        else:

            if results.cursor is not None:
                status = results.cursor.statusmessage

                if not status:
                    if conn.dialect.driver == "psycopg2":
                        status = get_command_status(results.cursor)

            if status:
                config.output.write(status.upper())
                if results.rowcount > -1:
                    config.output.write(" ")
                    config.output.write(str(results.rowcount))
                config.output.write("\n")

            write_time(total_time)

        if config.autocommit:
            conn.execute(text("commit;"))


def run_file(conn, file):

    with open(file, "rt") as fp:
        query = fp.read()

    results = conn.execute(text(query).execution_options(autocommit=config.autocommit))
    try:
        write(results)
    except BrokenPipeError:
        pass

    if config.autocommit:
        conn.execute(text("commit;"))


def run_metacommand(conn, metacommand, rest):
    if metacommand == "i":
        run_file(conn, strip(rest))
    elif metacommand == "o":
        set_output(strip(rest))
    elif metacommand == "e":
        query = run_editor(rest)
        run_command(conn, query)
    elif metacommand == "d":
        metacommand_describe(conn, strip(rest))
    elif metacommand == "dt":
        metacommand_describe_tables(conn, strip(rest))
    elif metacommand == "?":
        metacommand_help()
    elif metacommand == "??":
        metacommand_help_main()
    elif metacommand == "q":
        raise QuitException()
    elif metacommand in ("timing", "x", "t", "a"):

        config_attr = {
            "timing": "timing",
            "x": "extended_display",
            "t": "tuples_only",
            "a": "format_",
            "syntax": "syntax",
            "color": "color",
        }[metacommand]

        set_function = {
            "timing": set_timing,
            "x": set_extended_display,
            "t": set_tuples_only,
            "a": set_format,
            "syntax": set_syntax,
            "color": set_color,
        }[metacommand]

        if not rest:
            if metacommand == "a":
                value = getattr(config, config_attr)
                if value != "aligned":
                    value = "aligned"
                else:
                    value = "unaligned"
            else:
                value = not getattr(config, config_attr)
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
                    output=sys.stderr,
                )
                set_function(getattr(config, config_attr))
                return

        set_function(value)
    elif metacommand == "pset":
        metacommand_pset(rest)
    else:
        handle_invalid_command(metacommand, sys.stderr)


def handle_invalid_command(command):
    sys.stderr.write("invalid command ")
    sys.stderr.write(command)
    sys.stderr.write("\n")
    sys.stderr.write("Try \\? for help.\n")
    sys.stderr.flush()


def handle_invalid_command_value(command, value, expected=None):
    sys.stderr.write('unrecognized value "{}" for "\\{}"'.format(value, command))
    if expected:
        sys.stderr.write(": ")
        sys.stderr.write(expected)
    sys.stderr.write("\n")
    sys.stderr.flush()


def metacommand_help():
    sys.stdout.write("Input/Output\n")
    sys.stdout.write("  \\i FILE                execute commands from file\n")
    sys.stdout.flush()


def metacommand_help_main():
    sys.stdout.write("You are using xsql, the command-line interface to any database.\n")
    sys.stdout.write("Type:  \\? for help with xsql commands\n")
    sys.stdout.write("       \\q to quit\n")
    sys.stdout.flush()


def metacommand_describe(conn, target):
    if not target:
        metacommand_describe_tables(conn, target)
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

        run_command(conn, query)


def metacommand_describe_tables(conn, target):
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

    run_command(conn, query, title="List of relations")


def metacommand_pset(target):
    variable, value = process_command_with_variable(None, target)

    if variable == "null":
        set_null_display(value)
    elif variable == "format":
        set_format(value)
    else:
        sys.stderr.write(
            "xsql error: \\pset: unknown option: {}\n"
            .format(variable)
        )
        sys.stderr.flush()


def run_editor(text):
    filename = None
    add_to_history = False

    if not text:
        add_to_history = True
        for entry in history.load_history_strings():
            if entry.strip().startswith("\\e"):
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
