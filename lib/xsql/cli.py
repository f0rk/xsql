import re
import sys

import sqlalchemy
import sqlglot
from prompt_toolkit import PromptSession
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers import sql

from .config import config
from .db import connect
from .run import run_command, run_file


bindings = KeyBindings()


@bindings.add(Keys.Enter)
def _(event):

    if not re.search(r";\s*$", event.current_buffer.text):
        event.current_buffer.insert_text("\n")
        return

    try:
        sqlglot.transpile(event.current_buffer.text)
    except sqlglot.errors.ParseError:
        event.current_buffer.insert_text("\n")
        return event

    event.current_buffer.validate_and_handle()


def prompt_continuation(width, line_number, is_soft_wrap):
    return "> "


def run(args):

    conn = connect(args)

    if args.command:
        run_command(conn, args.command, output=args.output, autocommit=True)
        clean_exit()
    elif args.file:
        run_file(conn, args.file, output=args.output, autocommit=True)
        clean_exit()

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

    session = PromptSession(**prompt_args)

    while True:
        try:
            text = session.prompt(
                "[db]> ",
                multiline=True,
                prompt_continuation=prompt_continuation,
            )

            if text.strip():
                try:
                    run_command(conn, text, autocommit=config.autocommit)
                except sqlalchemy.exc.SQLAlchemyError as exc:
                    is_postgres = False
                    if hasattr(exc, "orig") and hasattr(exc.orig, "pgerror"):
                        is_postgres = True

                    if not is_postgres:
                        sys.stdout.write(exc.orig.args[0])
                    else:
                        sys.stdout.write(
                            "ERROR:  {}: {}\n"
                            .format(
                                exc.orig.pgcode,
                                exc.orig.args[0],
                            )
                        )

                        for notice in conn.connection.notices:
                            sys.stdout.write(notice)

                        conn.connection.notices.clear()

                    if config.autocommit:
                        conn.rollback()

        except EOFError:
            clean_exit()
        except KeyboardInterrupt:
            pass


def clean_exit():
    sys.exit(0)
