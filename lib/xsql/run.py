import copy
import functools
import io
import os
import re
import subprocess
import sys
import time
import tempfile

import lark
from prompt_toolkit.buffer import Buffer
from sqlalchemy import text

from .completion import clear_completions, refresh_completions
from .config import (
    config,
    process_command_with_variable,
    set_autocomplete_refresh,
    set_color,
    set_extended_display,
    set_field_separator,
    set_format,
    set_null_display,
    set_output,
    set_record_separator,
    set_set,
    set_syntax,
    set_timing,
    set_translate,
    set_tuples_only,
)
from .db import Reconnect, display_ssl_info
from .exc import QuitException
from .formatters import CopyWriter
from .history import history
from .output import get_pager, should_use_pager, write, write_csv
from .parsers import parse_copy
from .postgres import get_command_status
from .time import write_time
from .translate import translate


def get_metacommand(command):

    if not command:
        return False

    command = command.strip()

    if command == "help":
        command = "\\??"

    if is_maybe_metacommand(command):
        match = re.search(r"^\s*\\([a-z?+!]+)(?:\s+(.+))?$", command)
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


def run_command(conn, command, title=None, show_rowcount=True, extra_content=None):

    if isinstance(command, str) and is_maybe_metacommand(command):
        match = get_metacommand(command)
        if not match:
            handle_invalid_command(command)
            return

        metacommand, rest = match.groups()

        run_metacommand(
            conn,
            metacommand,
            rest,
        )
    else:

        command = translate(conn, command)
        if command is None:
            return

        # XXX: really need to get cursor.statusmessage or pgresult_ptr to work
        status = None

        if isinstance(command, str):
            if match := re.search(r"^\s*((create|drop)\s+(materialized\s+)?(\w+))", command, flags=re.I):
                status = match.groups()[0]
            elif match := re.search(r"^\s*(insert|update|delete|truncate|analyze|vacuum|copy|begin|commit)\b", command, flags=re.I):
                status = match.groups()[0]

            command = text(command)

        start_time = time.monotonic_ns()

        results = conn.execute(command)

        total_time = time.monotonic_ns() - start_time

        if results.returns_rows:
            try:
                write(
                    results,
                    title=title,
                    show_rowcount=show_rowcount,
                    extra_content=extra_content,
                    total_time=total_time,
                )
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

            if config.autocomplete:
                if status and re.search("^(create|drop|alter)", status.lower()):
                    refresh_completions(conn)
            else:
                clear_completions()


def run_file(conn, file):

    with open(file, "rt") as fp:
        query = fp.read()

    query = translate(conn, query)
    if query is None:
        return

    results = conn.execute(text(query))
    try:
        write(results)
    except BrokenPipeError:
        pass


def build_native_copy(query, options):
    statement = "copy (" + query + ") " + options.direction

    if options.direction == "to":
        statement += " stdout"
    else:
        statement += " stdin"

    statement += " with (format " + options.format_

    if options.freeze is not None:
        statement += ", freeze " + str(options.freeze).lower()

    if options.null:
        statement += ", null '" + options.null + "'"

    if options.default:
        statement += ", default '" + options.default + "'"

    if options.header:
        statement += ", header"

    if options.quote:
        statement += ", quote '" + options.quote + "'"

    if options.escape:
        statement += ", escape '" + options.escape + "'"

    if options.force_quote:
        if "*" in options.force_quote:
            force_quote_cols = "*"
        else:
            force_quote_cols = "(" + ", ".join(options.force_quote) + ")"
        statement += ", force_quote " + force_quote_cols

    if options.force_not_null:
        statement += ", force_not_null (" + ", ".join(options.force_not_null) + ")"

    if options.force_null:
        statement += ", force_null (" + ", ".join(options.force_null) + ")"

    if options.on_error:
        statement += ", on_error " + options.on_error

    if options.reject_limit:
        statement += ", reject_limit " + options.reject_limit

    if options.encoding:
        statement += ", encoding '" + options.encoding + "'"

    if options.log_verbosity:
        statement += ", log_verbosity " + options.log_verbosity

    statement += ")"

    return statement


def run_copy(conn, command):

    command = "copy " + command

    command = translate(conn, command)
    if command is None:
        return

    try:
        query, options = parse_copy(command)
    except lark.exceptions.UnexpectedCharacters as uex:
        sys.stderr.write(
            "ERROR:  unable to parse command at line {} col {} near {}\n"
            .format(
                uex.line,
                uex.column,
                uex.char,
            )
        )

        uex_parts = str(uex).split("\n")

        context = uex_parts[2]
        arrow = uex_parts[3]

        sys.stderr.write(context)
        sys.stderr.write("\n")
        sys.stderr.write(arrow)
        sys.stderr.write("\n")
        sys.stderr.flush()

        return

    total_time = None
    total_rows = None

    if conn.dialect.name == "postgresql":

        statement = build_native_copy(query, options)

        closable = None

        try:

            if options.target_type == "file":
                closable = open(options.target, "wt")
                fp = closable
            elif options.target_type == "pipe":
                if options.target == "stdout":
                    fp = sys.stdout
                else:
                    fp = sys.stdin
            elif options.target_type == "program":
                sys.stderr.write("copy program is not implemented\n")
                sys.stderr.flush()
                return

            start_time = time.monotonic_ns()
            with conn._dbapi_connection.cursor() as curs:
                curs.copy_expert(statement, fp)
                total_rows = curs.rowcount
            total_time = time.monotonic_ns() - start_time
        finally:
            if closable is not None:
                closable.close()

    else:

        if options.direction == "from":
            sys.stderr.write(
                "copy from is not implemented for {}\n"
                .format(conn.dialect.name)
            )
            sys.stderr.flush()
            return

        try:

            if options.target_type == "file":
                closable = open(options.target, "wt")
                fp = closable
            elif options.target_type == "pipe":
                if options.target == "pstdout":
                    fp = sys.stdout
                elif options.target == "pstdin":
                    fp = sys.stdin
                elif options.target == "stdout":
                    fp = config.output
                elif options.target == "stdin":
                    fp = sys.stdin
                else:
                    sys.stderr.write(
                        "copy {} {} is not implemented\n"
                        .format(
                            options.direction,
                            options.target,
                        )
                    )
                    sys.stderr.flush()
                    return

            elif options.target_type == "program":
                sys.stderr.write("copy program is not implemented\n")
                sys.stderr.flush()
                return

            if options.format_ == "text":
                writer = CopyWriter(
                    fp,
                    null=(options.null or "\\N"),
                    delimiter=(options.delimiter or "\t"),
                )

                start_time = time.monotonic_ns()
                results = conn.execute(text(query))

                total_rows = 0
                for result in results:
                    total_rows += 1
                    writer.writerow(result)

                total_time = time.monotonic_ns() - start_time
            elif options.format_ == "csv":
                start_time = time.monotonic_ns()
                results = conn.execute(text(query))

                total_rows = write_csv(
                    fp,
                    results,
                    results,
                    write_header=options.header,
                    delimiter=(options.delimiter or ","),
                )

                total_time = time.monotonic_ns() - start_time
            else:
                sys.stderr.write(
                    "copy format {} is not implemented\n"
                    .format(options.format_)
                )
                sys.stderr.flush()
                return

        finally:
            if closable is not None:
                closable.close()

    if total_time is not None:
        if options.target_type != "pipe":
            if total_rows is not None:
                sys.stdout.write("COPY {}\n".format(total_rows))
            else:
                sys.stdout.write("COPY\n")
            sys.stdout.flush()

        do_write_timing = config.timing

        if do_write_timing:
            write_time(total_time)


def metacommand_connect(target):
    raise Reconnect(target)


def metacommand_conninfo(conn, now=""):

    sys.stdout.write("You are {}connected".format(now))

    url = conn.engine.url

    if url.database:
        sys.stdout.write(' to database "{}"'.format(url.database))
    if url.username:
        sys.stdout.write(' as user "{}"'.format(url.username))

    if url.host:
        sys.stdout.write(' on host "{}"'.format(url.host))
    else:
        if conn.dialect.name == "postgresql":
            res = conn.execute(text("show unix_socket_directories")).fetchone()
            sys.stdout.write(' via socket in "{}"'.format(res[0]))

    if url.port:
        sys.stdout.write(' at port "{}"'.format(url.port))
    else:
        if conn.dialect.name in ("postgresql", "redshift"):

            port = conn.connection.dbapi_connection.get_dsn_parameters().get("port")
            if not port:
                port = 5432

            sys.stdout.write(' at port "{}"'.format(port))

        elif conn.dialect.name == "snowflake":
            sys.stdout.write(' at port "443"')

    sys.stdout.write("\n")

    display_ssl_info(conn)

    sys.stdout.flush()


def run_metacommand(conn, metacommand, rest):
    if metacommand == "i":
        run_file(conn, strip(rest))
    elif metacommand == "copy":
        run_copy(conn, strip(rest))
    elif metacommand == "o":
        set_output(strip(rest))
    elif metacommand == "f":
        if not strip(rest):
            sys.stdout.write('Field separator is "{}".\n'.format(config.field_separator))
            sys.stdout.flush()
        else:
            set_field_separator(strip(rest))
    elif metacommand == "e":
        query = run_editor(rest)
        run_command(conn, query)
    elif metacommand == "d":
        metacommand_describe(conn, strip(rest))
    elif metacommand == "dt":
        metacommand_describe_tables(conn, strip(rest))
    elif metacommand in ("c", "connect"):
        metacommand_connect(strip(rest))
    elif metacommand == "conninfo":
        metacommand_conninfo(conn)
    elif metacommand == "cd":
        if strip(rest):
            os.chdir(strip(rest))
    elif metacommand == "setenv":
        variable, value = process_command_with_variable(None, strip(rest))
        if not value:
            if variable in os.environ:
                del os.environ[variable]
        else:
            os.environ[variable] = value
    elif metacommand == "!":
        run_shell(str(rest))
    elif metacommand == "?":
        metacommand_help()
    elif metacommand == "??":
        metacommand_help_main()
    elif metacommand == "q":
        raise QuitException()
    elif metacommand in ("timing", "x", "t", "a", "syntax", "color", "autocomplete"):

        config_attr = {
            "timing": "timing",
            "x": "extended_display",
            "t": "tuples_only",
            "a": "format_",
            "syntax": "syntax",
            "color": "color",
            "autocomplete": "autocomplete",
        }[metacommand]

        set_function = {
            "timing": set_timing,
            "x": set_extended_display,
            "t": set_tuples_only,
            "a": set_format,
            "syntax": functools.partial(set_syntax, conn),
            "color": set_color,
            "autocomplete": functools.partial(set_autocomplete_refresh, conn),
        }[metacommand]

        if not rest:
            if metacommand == "a":
                value = getattr(config, config_attr)
                if value != "aligned":
                    value = "aligned"
                else:
                    value = "unaligned"
            elif metacommand == "autocomplete":
                value = None
            else:
                value = not getattr(config, config_attr)
        else:
            if metacommand == "autocomplete":
                if rest in ("readline", "column", "multi_column", "on", "auto"):
                    value = rest
                    if value == "on":
                        value = "auto"
                elif rest == "off":
                    value = None
                elif rest == "refresh":
                    refresh_completions(conn)
                else:
                    handle_invalid_command_value(
                        metacommand,
                        rest,
                        expected="One of readline, column, multi_column, auto, on, or off expected",
                        output=sys.stderr,
                    )
                    set_function(getattr(config, config_attr))
                    return
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
    elif metacommand == "set":
        metacommand_set(rest)
    elif metacommand == "unset":
        metacommand_unset(rest)
    elif metacommand == "pset":
        metacommand_pset(rest)
    elif metacommand == "translate":
        metacommand_translate(rest)
    else:
        handle_invalid_command(metacommand)


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

    syntax_display = "off"
    if config.syntax:
        syntax_display = "on"

    color_display = "off"
    if config.color:
        color_display = "on"

    autocomplete_display = "off"
    if config.autocomplete:
        autocomplete_display = config.autocomplete

    current_timing = "off"
    if config.timing:
        current_timing = "on"

    current_tuples_only = "off"
    if config.tuples_only:
        current_tuples_only = "on"

    extended_display = "off"
    if config.extended_display:
        extended_display = "on"

    pager = None
    if should_use_pager():
        pager, output = get_pager()
    else:
        output = sys.stdout

    output.write("Query Buffer\n")
    output.write("  \\e [FILE]              edit the query buffer (or file) with external editor\n")
    output.write("  \\translate [FROM] [TO] invoke translation function with query\n")
    output.write("  \\syntax [on|off]       turn syntax highlighting on or off (currently {})\n".format(syntax_display))
    output.write("  \\color [on|off]        turn color on or off (currently {})\n".format(color_display))
    output.write("  \\autocomplete [on|off|auto|readline|column|multi_column|refresh]\n")
    output.write("                         turn autocomplete on or off, or refresh the cache (currently {})\n".format(autocomplete_display))
    output.write("\n")

    output.write("Input/Output\n")
    output.write("  \\copy ...              perform SQL COPY with data stream to the client host\n")
    output.write("  \\i FILE                execute commands from file\n")
    output.write("  \\o [FILE]              send all query results to file or |pipe\n")
    output.write("\n")

    output.write("Informational\n")
    output.write("  \\d                     list tables and views\n")
    output.write("  \\d      NAME           describe table or view\n")
    output.write("\n")

    output.write("Formatting\n")
    output.write("  \\a                     toggle between unaligned and aligned output mode\n")
    output.write("  \\f [STRING]            show or set field separator for unaligned query output\n")
    output.write("  \\pset [NAME [VALUE]]   set table output option\n")
    output.write("                         fieldsep_zero|format|null|recordsep_zero|tuples_only\n")
    output.write("  \\t [on|off]            show only rows (currently {})\n".format(current_tuples_only))
    output.write("  \\x [on|off]            toggle expanded output (currently {})\n".format(extended_display))
    output.write("\n")

    output.write("Connection\n")
    output.write("  \\c[onnect] {url | alias}\n")
    output.write("                         connect to new database\n")
    output.write("  \\conninfo              display information about current connection\n")
    output.write("\n")

    output.write("Operating System\n")
    output.write("  \\cd [DIR]              change the current working directory\n")
    output.write("  \\setenv NAME [VALUE]   set or unset environment variable\n")
    output.write("  \\timing [on|off]       toggle timing of commands (currently {})\n".format(current_timing))
    output.write("  \\! [COMMAND]           execute command in shell or start interactive shell\n")
    output.write("\n")

    output.write("Variables\n")
    output.write("  \\set [NAME [VALUE]]    set internal variable, or list all if no parameters\n")
    output.write("  \\unset NAME            unset (delete) internal variable\n")
    output.flush()

    if pager is not None:
        pager.communicate()


def metacommand_help_main():
    sys.stdout.write("You are using xsql, the command-line interface to any database.\n")
    sys.stdout.write("Type:  \\? for help with xsql commands\n")
    sys.stdout.write("       \\q to quit\n")
    sys.stdout.flush()


def metacommand_describe(conn, target):
    if not target:
        metacommand_describe_tables(conn, target)
    else:

        filter_target = glob_to_like(target)

        objects = []

        if conn.dialect.name == "sqlite":

            object_query = """
            select
                't' as object_type,
                null as object_schema,
                sqlite_master.tbl_name as object_name
            from
                sqlite_master
            where
                sqlite_master.tbl_name like :filter_target
            order by
                sqlite_master.tbl_name
            """

            object_results = conn.execute(text(object_query).bindparams(filter_target=filter_target))

            for object_result in object_results:
                objects.append(object_result)

            table_query = """
            select
                info.name as "Column",
                info."type" as "Type",
                '' as "Collation",
                case
                    when info."notnull" is distinct from 0 then 'not null'
                    else ''
                end as "Nullable",
                info.dflt_value as "Default"
            from
                sqlite_master,
                pragma_table_info(sqlite_master.tbl_name) as info
            where
                null is not distinct from :object_schema
                and sqlite_master.tbl_name = :object_name
            order by
                info.cid
            """

            index_query = None
        else:

            object_query = """
            select
                't' as object_type,
                information_schema.tables.table_schema as object_schema,
                information_schema.tables.table_name as object_name
            from
                information_schema.tables
            where
                :filter_target is null
                or (information_schema.tables.table_schema || '.' || information_schema.tables.table_name) ilike :filter_target
                or information_schema.tables.table_name ilike :filter_target
            order by
                information_schema.tables.table_schema,
                information_schema.tables.table_name
            """

            object_results = conn.execute(text(object_query).bindparams(filter_target=filter_target))

            for object_result in object_results:
                objects.append(object_result)

            table_query = """
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
                information_schema.columns.table_schema = :object_schema
                and information_schema.columns.table_name = :object_name
            order by
                information_schema.columns.ordinal_position
            """

            if conn.dialect.name == "postgresql":

                conperiod = "false AS conperiod"
                if conn.dialect.server_version_info[0] >= 18:
                    conperiod = "pg_catalog.pg_constraint.conperiod"

                index_query = """
                select
                    pg_catalog.pg_namespace.nspname,
                    pg_catalog.pg_class.relname,
                    index_class.relname as index_name,
                    pg_catalog.pg_index.indisprimary,
                    pg_catalog.pg_index.indisunique,
                    pg_catalog.pg_index.indisclustered,
                    pg_catalog.pg_index.indisvalid,
                    pg_catalog.pg_get_indexdef(pg_catalog.pg_index.indexrelid, 0, true) as indexdef,
                    pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) as constraintdef,
                    pg_catalog.pg_index.indisreplident,
                    pg_catalog.pg_constraint.contype,
                    pg_catalog.pg_constraint.condeferrable,
                    pg_catalog.pg_constraint.condeferred,
                    {conperiod}
                from
                    pg_catalog.pg_class
                join
                    pg_catalog.pg_namespace
                on
                    pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid
                join
                    pg_catalog.pg_index
                on
                    pg_catalog.pg_class.oid = pg_catalog.pg_index.indrelid
                join
                    pg_catalog.pg_class as index_class
                on
                    pg_catalog.pg_index.indexrelid = index_class.oid
                left join
                    pg_catalog.pg_constraint
                on
                    pg_catalog.pg_constraint.conrelid = pg_catalog.pg_index.indrelid
                    and pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid
                    and pg_catalog.pg_constraint.contype in ('p', 'u', 'x')
                where
                    pg_catalog.pg_namespace.nspname = :object_schema
                    and pg_catalog.pg_class.relname = :object_name
                order by
                    pg_catalog.pg_index.indisprimary desc,
                    index_class.relname
                """.format(conperiod=conperiod)
            else:
                index_query = None

            if conn.dialect.name == "postgresql":
                check_query = """
                select
                    pg_catalog.pg_constraint.conname as constraint_name,
                    pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) as constraintdef
                from
                    pg_catalog.pg_constraint
                join
                    pg_catalog.pg_class
                on
                    pg_catalog.pg_class.oid = pg_catalog.pg_constraint.conrelid
                join
                    pg_catalog.pg_namespace
                on
                    pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid
                where
                    pg_catalog.pg_namespace.nspname = :object_schema
                    and pg_catalog.pg_class.relname = :object_name
                    and pg_catalog.pg_constraint.contype = 'c'
                order by
                    1
                """
            else:
                check_query = None

            if conn.dialect.name == "postgresql":
                if conn.dialect.server_version_info[0] >= 12:
                    references_query = """
                    select
                        pg_catalog.pg_constraint.conrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass as sametable,
                        pg_catalog.pg_constraint.conname as constraint_name,
                        pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) as constraintdef,
                        pg_catalog.pg_constraint.conrelid::pg_catalog.regclass as ontable
                    from
                        pg_catalog.pg_constraint
                    left join (
                        select
                            *
                        from
                            pg_catalog.pg_partition_ancestors((:object_schema || '.' || :object_name)::pg_catalog.regclass)
                    ) as ancestors
                    on
                        pg_catalog.pg_constraint.conrelid = ancestors.relid
                    where
                        pg_catalog.pg_constraint.contype = 'f'
                        and pg_catalog.pg_constraint.conparentid = 0
                        and (
                            pg_catalog.pg_constraint.conrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass
                            or ancestors.relid = pg_catalog.pg_constraint.conrelid
                        )
                    order by
                        sametable desc,
                        pg_catalog.pg_constraint.conname
                    """
                else:
                    references_query = """
                    select
                        pg_catalog.pg_constraint.conrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass as sametable,
                        pg_catalog.pg_constraint.conname as constraint_name,
                        pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) as constraintdef,
                        pg_catalog.pg_constraint.conrelid::pg_catalog.regclass as ontable
                    from
                        pg_catalog.pg_constraint
                    where
                        pg_catalog.pg_constraint.contype = 'f'
                        and pg_catalog.pg_constraint.conrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass
                    order by
                        sametable desc,
                        pg_catalog.pg_constraint.conname
                    """
            else:
                references_query = None

            if conn.dialect.name == "postgresql":
                if conn.dialect.server_version_info[0] >= 12:
                    foreign_key_query = """
                    select
                        pg_catalog.pg_constraint.conname as constraint_name,
                        pg_catalog.pg_constraint.conrelid::pg_catalog.regclass as ontable,
                        pg_catalog.pg_get_constraintdef(oid, true) as constraintdef
                    from
                        pg_catalog.pg_constraint
                    where
                        pg_catalog.pg_constraint.confrelid in (
                            select
                                pg_catalog.pg_partition_ancestors((:object_schema || '.' || :object_name)::pg_catalog.regclass)
                            union all
                            values
                                ((:object_schema || '.' || :object_name)::pg_catalog.regclass)
                        )
                        and pg_catalog.pg_constraint.contype = 'f'
                        and pg_catalog.pg_constraint.conparentid = 0
                    order by
                        pg_catalog.pg_constraint.conname
                    """
                else:
                    foreign_key_query = """
                    select
                        conname as constraint_name,
                        conrelid::pg_catalog.regclass as ontable,
                        pg_catalog.pg_get_constraintdef(oid, true) as constraintdef
                    from
                        pg_catalog.pg_constraint
                    where
                        pg_catalog.pg_constraint.confrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass
                        and pg_catalog.pg_constraint.contype = 'f'
                    order by
                        pg_catalog.pg_constraint.conname
                    """
            else:
                foreign_key_query = None

            if conn.dialect.name == "postgresql":

                trigger_parent = "null as parent"
                if conn.dialect.server_version_info[0] >= 13:
                    trigger_parent = """
                    case when pg_catalog.pg_trigger.tgparentid != 0 then (
                        select
                            pg_catalog.pg_trigger.tgrelid::pg_catalog.regclass
                        from
                            pg_catalog.pg_trigger,
                            pg_catalog.pg_partition_ancestors(pg_catalog.pg_trigger.tgrelid) with ordinality as ancestors (relid, depth)
                        where
                            pg_catalog.pg_trigger.tgname = pg_catalog.pg_trigger.tgname
                            and pg_catalog.pg_trigger.tgrelid = ancestors.relid
                            and pg_catalog.pg_trigger.tgparentid = 0
                        order by
                            ancestors.depth
                        limit
                            1
                    ) end as parent
                    """

                trigger_query = """
                select
                    pg_catalog.pg_trigger.tgname as trigger_name,
                    pg_catalog.pg_get_triggerdef(pg_catalog.pg_trigger.oid, true) as triggerdef,
                    pg_catalog.pg_trigger.tgenabled,
                    pg_catalog.pg_trigger.tgisinternal,
                    {trigger_parent}
                from
                    pg_catalog.pg_trigger
                where
                    pg_catalog.pg_trigger.tgrelid = (:object_schema || '.' || :object_name)::pg_catalog.regclass
                """.format(trigger_parent=trigger_parent)

                if conn.dialect.server_version_info[0] >= 11 and conn.dialect.server_version_info[0] < 15:
                    trigger_query += """
                    and (
                        not pg_catalog.pg_trigger.tgisinternal
                        or (
                            pg_catalog.pg_trigger.tgisinternal
                            and pg_catalog.pg_trigger.tgenabled = 'D'
                        )
                        or exists (
                            select
                                1
                            from
                                pg_catalog.pg_depend
                            where
                                pg_catalog.pg_depend.objid = pg_catalog.pg_trigger.oid
                                and pg_catalog.pg_depend.refclassid = 'pg_catalog.pg_trigger'::pg_catalog.regclass
                        )
                    )
                    """
                else:
                    trigger_query += """
                    and (
                        not pg_catalog.pg_trigger.tgisinternal
                        or (
                            pg_catalog.pg_trigger.tgisinternal
                            and pg_catalog.pg_trigger.tgenabled = 'D'
                        )
                    )
                    """

                trigger_query += """
                order by 1;
                """
            else:
                trigger_query = None

        for object_result in objects:

            title = None

            if object_result.object_type == "t":
                title = 'Table "{}"'.format(object_result.object_name)

                if object_result.object_schema:
                    title = 'Table "{}.{}"'.format(
                        object_result.object_schema,
                        object_result.object_name,
                    )

            params = {
                "object_schema": object_result.object_schema,
                "object_name": object_result.object_name,
            }
            query = text(table_query).bindparams(**params)

            extra_content = None

            if object_result.object_type == "t":
                if index_query is not None:

                    if extra_content is None:
                        extra_content = io.StringIO()

                    index_results = conn.execute(text(index_query).bindparams(**params))

                    write_index_header = True

                    for index_result in index_results:
                        if write_index_header:
                            extra_content.write("Indexes:\n")
                            write_index_header = False

                        extra_content.write('    "')
                        extra_content.write(index_result.index_name)
                        extra_content.write('" ')

                        if index_result.contype == "x" or index_result.conperiod == "u":
                            extra_content.write(index_result.constraintdef)
                        else:

                            index_type = ""
                            if index_result.indisprimary:
                                index_type = "PRIMARY KEY, "
                            elif index_result.indisunique:
                                if index_result.contype == "u":
                                    index_type = "UNIQUE CONSTRAINT, "
                                else:
                                    index_type = "UNIQUE, "

                            extra_content.write(index_type)

                            _, index_definition = index_result.indexdef.split(" USING ", maxsplit=1)
                            extra_content.write(index_definition)

                            if index_result.condeferrable:
                                extra_content.write(" DEFERRABLE")
                            if index_result.condeferred:
                                extra_content.write(" INITIALLY DEFERRED")

                        if index_result.indisclustered:
                            extra_content.write(" CLUSTER")
                        if not index_result.indisvalid:
                            extra_content.write(" INVALID")
                        if index_result.indisreplident:
                            extra_content.write(" REPLICA IDENTITY")

                        extra_content.write("\n")

                if check_query is not None:
                    if extra_content is None:
                        extra_content = io.StringIO()

                    check_results = conn.execute(text(check_query).bindparams(**params))

                    write_check_header = True

                    for check_result in check_results:
                        if write_check_header:
                            extra_content.write("Check constraints:\n")
                            write_check_header = False

                        extra_content.write('    "')
                        extra_content.write(check_result.constraint_name)
                        extra_content.write('" ')
                        extra_content.write(check_result.constraintdef)
                        extra_content.write("\n")

                if references_query is not None:
                    if extra_content is None:
                        extra_content = io.StringIO()

                    references_results = conn.execute(text(references_query).bindparams(**params))

                    write_references_header = True

                    for references_result in references_results:
                        if write_references_header:
                            extra_content.write("Foreign-key constraints:\n")
                            write_references_header = False

                        if not references_result.sametable:
                            extra_content.write('    TABLE "')
                            extra_content.write(references_result.ontable)
                            extra_content.write('" CONSTRAINT "')
                        else:
                            extra_content.write('    "')

                        extra_content.write(references_result.constraint_name)
                        extra_content.write('" ')
                        extra_content.write(references_result.constraintdef)
                        extra_content.write("\n")

                if foreign_key_query is not None:
                    if extra_content is None:
                        extra_content = io.StringIO()

                    foreign_key_results = conn.execute(text(foreign_key_query).bindparams(**params))

                    write_foreign_key_header = True

                    for foreign_key_result in foreign_key_results:
                        if write_foreign_key_header:
                            extra_content.write("Referenced by:\n")
                            write_foreign_key_header = False

                        extra_content.write('    TABLE "')
                        extra_content.write(foreign_key_result.ontable)
                        extra_content.write('" CONSTRAINT "')
                        extra_content.write(foreign_key_result.constraint_name)
                        extra_content.write('" ')
                        extra_content.write(foreign_key_result.constraintdef)
                        extra_content.write("\n")

                if trigger_query is not None:

                    if extra_content is None:
                        extra_content = io.StringIO()

                    trigger_results = conn.execute(text(trigger_query).bindparams(**params))

                    sections = {
                        "triggers": "Triggers",
                        "disabled_user_triggers": "Disabled user triggers",
                        "disabled_internal_triggers": "Disabled internal triggers",
                        "triggers_firing_always": "Triggers firing always",
                        "triggers_firing_replica_only": "Triggers firing on replica only",
                    }

                    write_header = {}

                    for section_key, section_title in sections.items():
                        for trigger_result in trigger_results:

                            trigger_section = None
                            if trigger_result.tgenabled == "O" or trigger_result.tgenabled is True:
                                trigger_section = "triggers"
                            elif not trigger_result.tgisinternal and (trigger_result.tgenabled == "D" or trigger_result.tgenabled is False):
                                trigger_section = "disabled_user_triggers"
                            elif trigger_result.tgisinternal and (trigger_result.tgenabled == "D" or trigger_result.tgenabled is False):
                                trigger_section = "disabled_internal_triggers"
                            elif trigger_result.tgenabled == "A":
                                trigger_section = "triggers_firing_always"
                            elif trigger_result.tgenabled == "R":
                                trigger_section = "triggers_firing_replica_only"
                            else:
                                trigger_section = "triggers"  # XXX

                            if trigger_section != section_key:
                                continue

                            if section_key not in write_header:
                                extra_content.write(section_title)
                                extra_content.write(":\n")
                                write_header[section_key] = False

                            extra_content.write('    ')

                            _, trigger_definition = trigger_result.triggerdef.split(" TRIGGER ", maxsplit=1)
                            extra_content.write(trigger_definition)

                            if trigger_result.parent:
                                extra_content.write(", ON TABLE ")
                                extra_content.write(trigger_result.parent)

                            extra_content.write("\n")

            if extra_content is not None:
                extra_content.seek(0)

            run_command(
                conn,
                query,
                title=title,
                show_rowcount=False,
                extra_content=extra_content,
            )


def metacommand_describe_tables(conn, target):

    if not target:
        target = None

    target = glob_to_like(target)

    if conn.dialect.name == "sqlite":
        query = """
        select
            'sqlite' as "Schema",
            tbl_name as "Name",
            "type" as "Type",
            null as "Owner"
        from
            sqlite_master
        where
            :target is null
            or ('sqlite.' || tbl_name) like :target
            or tbl_name like :target
        order by
            tbl_name
        """
    else:
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

    query = text(query).bindparams(target=target)

    run_command(conn, query, title="List of relations")


def metacommand_set(target):

    if not strip(target):
        values = copy.deepcopy(config.variables)
        values["prompt1"] = config.prompt1
        values["prompt2"] = config.prompt2
        values["histsize"] = config.history_size
        values["verbosity"] = config.verbosity
        names = sorted(list(values.keys()))

        for name in names:
            value = values[name]
            if value is None:
                continue

            if value is True:
                value = "on"
            elif value is False:
                value = "off"

            sys.stdout.write("{} = {!r}\n".format(name, value))
    else:
        variable, value = process_command_with_variable(None, target)
        set_set(variable, value)


def metacommand_unset(rest):
    if strip(rest) in config.variables:
        del config.variables[strip(rest)]


def metacommand_pset(target):
    variable, value = process_command_with_variable(None, target)

    if variable == "null":
        set_null_display(value)
    elif variable == "format":
        set_format(value)
    elif variable == "tuples_only":
        set_tuples_only(value)
    elif variable == "fieldsep":
        set_field_separator(value)
    elif variable == "fieldsep_zero":
        set_field_separator("\0")
    elif variable == "recordsep_zero":
        set_record_separator(value)
    else:
        sys.stderr.write(
            "xsql error: \\pset: unknown option: {}\n"
            .format(variable)
        )
        sys.stderr.flush()


def metacommand_translate(target):
    if not strip(target):
        if config.translate_from is None:
            sys.stdout.write("Translate is off.\n")
        else:
            sys.stdout.write(
                'Translate is from "{} to "{}".\n'
                .format(
                    config.translate_from,
                    config.translate_to,
                )
            )
        sys.stdout.flush()
    else:
        from_, to = process_command_with_variable(None, target)
        set_translate(from_, to)


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


def run_shell(command):
    process = subprocess.Popen(command, shell=True)
    process.communicate()
