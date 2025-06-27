import logging
import re
import signal
import sys
import time

try:
    import psycopg2.extensions
    import psycopg2.extras
    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
except ImportError:
    pass

import sqlalchemy
import sqlglot
from prompt_toolkit import PromptSession
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.output.color_depth import ColorDepth

from .completion import completer, get_complete_style, refresh_completions
from .config import config
from .db import (
    Reconnect,
    connect,
    display_ssl_info,
    get_server_name,
    get_server_version,
    resolve_url,
)
from .exc import PGError, QuitException, is_cancel_exception
from .history import history
from .lexer import lexer
from .prompt import render_prompt
from .run import (
    is_maybe_metacommand,
    metacommand_conninfo,
    run_command,
    run_file,
)
from .time import write_time
from .version import __version__


sqlglot_logger = logging.getLogger("sqlglot")
sqlglot_logger.setLevel(logging.ERROR)


bindings = KeyBindings()


@bindings.add(Keys.Enter)
def _(event):

    if is_maybe_metacommand(event.current_buffer.text):
        event.current_buffer.validate_and_handle()
        return

    if not re.search(r";\s*$", event.current_buffer.text):
        event.current_buffer.insert_text("\n")
        return

    try:
        sqlglot.transpile(event.current_buffer.text)
    except sqlglot.errors.ParseError:
        if re.search(r";\s*$", event.current_buffer.text):
            event.current_buffer.validate_and_handle()
        else:
            event.current_buffer.insert_text("\n")
        return

    event.current_buffer.validate_and_handle()


def try_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def run(args):

    if args.version:
        sys.stdout.write("xsql {}\n".format(__version__))
        sys.exit(0)

    is_url, url = resolve_url(args.url)

    if url is None:
        if is_url:
            # XXX: unreachable?
            sys.stdout.write(
                'xsql: error: connection to server failed: FATAL:  url "{}" is not valid\n'
                .format(args.url)
            )
        else:
            sys.stdout.write(
                'xsql: error: connection to server failed: FATAL:  alias "{}" does not exist\n'
                .format(args.url)
            )

        sys.exit(2)

    try:
        conn = connect(url)
    except (sqlalchemy.exc.SQLAlchemyError, PGError) as exc:
        is_postgres = False
        if hasattr(exc, "orig") and hasattr(exc.orig, "pgerror"):
            is_postgres = True
            pgexc = exc.orig
        if hasattr(exc, "pgerror"):
            is_postgres = True
            pgexc = exc

        sys.stdout.write("xsql: error: ")
        if not is_postgres:
            sys.stdout.write("connection to server failed: ")
            sys.stdout.write(exc.orig.args[0])
            if not exc.orig.args[0].endswith("\n"):
                sys.stdout.write("\n")
        else:
            sys.stdout.write(pgexc.args[0])

        sys.stdout.flush()
        sys.exit(2)

    def sigint_handler(*_):
        if hasattr(conn.connection.dbapi_connection, "cancel"):
            conn.connection.dbapi_connection.cancel()

    signal.signal(signal.SIGINT, sigint_handler)

    if args.quiet:
        config.quiet = args.quiet

    if not args.no_xsqlrc:
        config.load(conn)

    if args.tuples_only:
        config.tuples_only = args.tuples_only

    if args.csv:
        config.format_ = "csv"

    if args.no_align:
        config.format_ = "unaligned"

    if args.expanded:
        config.extended_display = args.expanded

    if args.field_separator:
        config.field_separator = args.field_separator

    if args.field_separator_zero:
        config.field_separator = "\0"

    if args.record_separator_zero:
        config.record_separator = "\0"

    if args.output:
        config.output = open(args.output, "wt")

    if args.translate:
        from_, to = args.translate.split(":")
        config.translate_from = from_
        config.translate_to = to

    if args.set:
        for entry in args.set:
            name, value = entry.split("=")
            config.variables[name] = value

    command = None

    if not sys.stdin.isatty():
        command = sys.stdin.read()

    if not command:
        command = args.command

    if command:

        if args.single_transaction:
            conn.execute(sqlalchemy.text("begin;"))

        run_command(conn, command)
        if args.single_transaction:
            conn.execute(sqlalchemy.text("commit;"))

        clean_exit(conn)
    elif args.file:

        if args.single_transaction:
            conn.execute(sqlalchemy.text("begin;"))

        run_file(conn, args.file)
        if args.single_transaction:
            conn.execute(sqlalchemy.text("commit;"))

        clean_exit(conn)

    if not config.quiet:
        sys.stdout.write("xsql ({}".format(__version__))

        server_name = get_server_name(conn)
        if server_name:
            sys.stdout.write(", ")
            sys.stdout.write(server_name)

            server_version = get_server_version(conn)
            if server_version:
                sys.stdout.write(" ")
                sys.stdout.write(server_version)

        sys.stdout.write(")\n")

        display_ssl_info(conn)

        sys.stdout.write('Type "help" for help.\n\n')

        sys.stdout.flush()

    prompt_args = {
        "vi_mode": True,
        "enable_open_in_editor": True,
        "tempfile_suffix": ".sql",
        "key_bindings": bindings,
        "lexer": lexer,
    }

    if config.history_size:
        prompt_args["history"] = history

    if config.autocomplete:
        prompt_args["completer"] = completer

        complete_style = get_complete_style()
        if complete_style:
            prompt_args["complete_style"] = complete_style

        refresh_completions(conn)

    session = PromptSession(**prompt_args)

    def color_depth():
        if config.color:
            return ColorDepth.from_env()
        else:
            return ColorDepth.DEPTH_1_BIT
    session.app._color_depth = color_depth

    while True:
        try:
            def prompt_continuation(width, line_number, is_soft_wrap):
                return render_prompt(conn, config.prompt2)

            text = session.prompt(
                render_prompt(conn, config.prompt1),
                multiline=True,
                prompt_continuation=prompt_continuation,
            )

            if text.strip():
                start_time = time.monotonic_ns()
                try:
                    run_command(conn, text)
                except (sqlalchemy.exc.SQLAlchemyError, PGError) as exc:

                    total_time = time.monotonic_ns() - start_time
                    is_postgres = False
                    pgexc = None
                    if hasattr(exc, "orig") and hasattr(exc.orig, "pgerror"):
                        is_postgres = True
                        pgexc = exc.orig
                    if hasattr(exc, "pgerror"):
                        is_postgres = True
                        pgexc = exc

                    was_cancelled = False
                    if is_cancel_exception(pgexc):
                        was_cancelled = True

                    if not was_cancelled:
                        if not is_postgres:
                            sys.stdout.write(exc.orig.args[0])
                            if not exc.orig.args[0].endswith("\n"):
                                sys.stdout.write("\n")
                        else:
                            sys.stdout.write(
                                "ERROR:  {}: {}"
                                .format(
                                    pgexc.pgcode,
                                    pgexc.args[0],
                                )
                            )

                            for notice in conn.connection.notices:
                                sys.stdout.write(notice)

                            conn.connection.notices.clear()

                        if config.timing:
                            write_time(total_time)

                        sys.stdout.flush()

        except EOFError:
            if not config.quiet:
                sys.stdout.write("\\q\n")
            clean_exit(conn)
        except QuitException:
            clean_exit(conn)
        except KeyboardInterrupt:
            pass
        except Reconnect as reconnect:

            is_url, url = resolve_url(reconnect.target)

            if url is None:
                if is_url:
                    # XXX: unreachable?
                    sys.stdout.write(
                        'connection to server failed: FATAL:  url "{}" is not valid\n'
                        .format(reconnect.target)
                    )
                else:
                    sys.stdout.write(
                        'connection to server failed: FATAL:  alias "{}" does not exist\n'
                        .format(reconnect.target)
                    )

                sys.stdout.write("Previous connection kept\n")
                sys.stdout.flush()
            else:
                try:
                    conn = connect(url)
                    metacommand_conninfo(conn)
                except (sqlalchemy.exc.SQLAlchemyError, PGError) as exc:
                    is_postgres = False
                    if hasattr(exc, "orig") and hasattr(exc.orig, "pgerror"):
                        is_postgres = True
                        pgexc = exc.orig
                    if hasattr(exc, "pgerror"):
                        is_postgres = True
                        pgexc = exc

                    if not is_postgres:
                        sys.stdout.write("connection to server failed: ")
                        sys.stdout.write(exc.orig.args[0])
                        if not exc.orig.args[0].endswith("\n"):
                            sys.stdout.write("\n")
                    else:
                        sys.stdout.write(pgexc.args[0])

                    sys.stdout.write("Previous connection kept\n")
                    sys.stdout.flush()


def clean_exit(conn=None):
    try_close(conn)
    sys.exit(0)
