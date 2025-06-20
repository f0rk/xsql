import logging
import re
import sys
import time

import sqlalchemy
import sqlglot
from prompt_toolkit import PromptSession
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers import sql

from .config import config
from .db import connect
from .exc import QuitException
from .history import history
from .prompt import render_prompt
from .run import is_maybe_metacommand, run_command, run_file
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

    conn = connect(args)

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

    command = None

    if not sys.stdin.isatty():
        command = sys.stdin.read()

    if not command:
        command = args.command

    if command:

        autocommit = True
        if args.single_transaction:
            conn.execute(sqlalchemy.text("begin;"))
            autocommit = False

        run_command(conn, command, output=config.output, autocommit=autocommit)
        if args.single_transaction:
            conn.execute(sqlalchemy.text("commit;"))

        clean_exit(conn)
    elif args.file:

        autocommit = True
        if args.single_transaction:
            conn.execute(sqlalchemy.text("begin;"))
            autocommit = False

        run_file(conn, args.file, output=config.output, autocommit=True)
        if args.single_transaction:
            conn.execute(sqlalchemy.text("commit;"))

        clean_exit(conn)

    if not config.quiet:
        sys.stdout.write("xsql ({})\n".format(__version__))
        sys.stdout.write('Type "help" for help.\n\n')
        sys.stdout.flush()

    prompt_args = {
        "vi_mode": True,
        "enable_open_in_editor": True,
        "tempfile_suffix": ".sql",
        "key_bindings": bindings,
    }

    if config.highlight:
        lexer_class = sql.SqlLexer
        if conn.dialect.name in ("postgresql", "redshift"):
            lexer_class = sql.PostgresLexer
        elif conn.dialect.name == "mysql":
            lexer_class = sql.MySqlLexer

        prompt_args["lexer"] = PygmentsLexer(lexer_class)

    if config.history_size:
        prompt_args["history"] = history

    session = PromptSession(**prompt_args)

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
                start_time = time.monotonic()
                try:
                    run_command(conn, text, autocommit=config.autocommit)
                except sqlalchemy.exc.SQLAlchemyError as exc:
                    total_time = time.monotonic() - start_time
                    is_postgres = False
                    if hasattr(exc, "orig") and hasattr(exc.orig, "pgerror"):
                        is_postgres = True

                    if not is_postgres:
                        sys.stdout.write(exc.orig.args[0])
                        if not exc.orig.args[0].endswith("\n"):
                            sys.stdout.write("\n")
                    else:
                        sys.stdout.write(
                            "ERROR:  {}: {}"
                            .format(
                                exc.orig.pgcode,
                                exc.orig.args[0],
                            )
                        )

                        for notice in conn.connection.notices:
                            sys.stdout.write(notice)

                        conn.connection.notices.clear()

                    if config.timing:
                        sys.stdout.write("Time: {:.3} ms\n".format(total_time * 1000))

                    sys.stdout.flush()

                    if config.autocommit:
                        conn.rollback()

                        config.run_sets(conn)

        except EOFError:
            clean_exit(conn)
        except QuitException:
            clean_exit(conn)
        except KeyboardInterrupt:
            pass


def clean_exit(conn=None):
    try_close(conn)
    sys.exit(0)
