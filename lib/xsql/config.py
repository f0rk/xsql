import os
import re
import sys
import time

from sqlalchemy import text


class Configuration:

    def __init__(
        self,
        autocommit=True,
        null="<NÜLLZØR>",
        pager=None,
        highlight=False,
        history_size=500,
        verbosity=None,
        timing=False,
        prompt1="%/=# ",
        prompt2="%/-# ",
        encoding=None,
        quiet=False,
        sets=None,
    ):

        self.autocommit = autocommit
        self.null = null

        if pager is None:
            pager = os.environ.get("PAGER")

        self.pager = pager
        self.highlight = highlight
        self.history_size = history_size
        self.verbosity = verbosity
        self.timing = timing
        self.prompt1 = prompt1
        self.prompt2 = prompt2
        self.encoding = encoding
        self.quiet = quiet

        if sets is None:
            sets = []

        self.sets = sets

    def load(self, conn, filename=None):

        if filename is None:
            filename = os.path.expanduser("~/.xsqlrc")

        if os.path.exists(filename):

            dialects = {}

            with open(filename, "rt") as fp:

                section = None

                for line_number, line in enumerate(fp):

                    line = line.strip()

                    if re.search(r"^\[\w+\]$", line):
                        section = line.strip("[").strip("]")
                    else:
                        dialects.setdefault(section, [])
                        dialects[section].append((filename, line_number, line))

            dialects_to_process = ["default", conn.dialect.name]
            for dialect_to_process in dialects_to_process:
                if dialect_to_process in dialects:
                    for filename, line_number, line in dialects[dialect_to_process]:
                        if line.strip():
                            process_config_line(conn, filename, line_number, line)


def trim_quotes(value):
    if isinstance(value, str):
        return value.strip("'")
    else:
        return value


def process_command_with_variable(command, line, default=None):
    remainder = line[len(command):].strip()

    res = re.split(r"\s+", remainder, maxsplit=1)
    variable = res[0]
    if len(res) > 1:
        value = res[1]
    else:
        value = default

    return variable, trim_quotes(value)


def process_command_with_value(command, line, default=None):
    remainder = line[len(command):].strip()

    if not remainder:
        return default

    return remainder


def process_command_with_boolean(command, line, default=None):
    value = process_command_with_value(command, line, default=default)

    if value in (True, "on"):
        value = True
    else:
        value = False

    return value


def set_null_display(value):
    config.null = value

    if not config.quiet:
        sys.stdout.write('Null display is "{}".\n'.format(value))


def set_timing(value):
    config.timing = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Timing is {}.\n".format(display_value))


def set_highlight(value):
    config.highlight = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Highlight is {}.\n".format(display_value))


def process_config_line(conn, filename, line_number, line):
    if line.startswith("\\pset"):
        variable, value = process_command_with_variable("\\pset", line)

        if variable == "null":
            set_null_display(value)
        else:
            sys.stderr.write(
                "xsql:{}:{} error: \\pset: unknown option: {}\n"
                .format(
                    filename,
                    line_number,
                    variable,
                )
            )
    elif line == "\\timing":
        value = process_command_with_boolean("\\timing", line, default=True)
        set_timing(value)
    elif line == "\\highlight":
        value = process_command_with_boolean("\\highlight", line, default=True)
        set_highlight(value)
    elif line.startswith("\\set"):
        variable, value = process_command_with_variable("\\set", line)

        if variable.lower() == "prompt1":
            config.prompt1 = value
        elif variable.lower() == "prompt2":
            config.prompt2 = value
        elif variable.lower() == "histsize":
            config.history_size = int(value)
        elif variable.lower() == "verbosity":
            config.verbosity = value
        else:
            pass
    else:
        config.sets.append(line)
        start_time = time.monotonic()
        conn.execute(text(line).execution_options(autocommit=True))

        if not config.quiet:
            if re.search("^set", line, flags=re.I):
                sys.stdout.write("SET\n")
            elif re.search("^select", line, flags=re.I):
                sys.stdout.write("SELECT\n")

        total_time = time.monotonic() - start_time
        if config.timing:
            sys.stdout.write("Time: {:.3f} ms\n".format(total_time * 1000))


config = Configuration()
