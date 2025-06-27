import os
import re
import sys
import time

from sqlalchemy import text

from .time import write_time


class Configuration:

    def __init__(
        self,
        output=None,
        isolation_level="AUTOCOMMIT",
        null="<NÜLLZØR>",
        pager=None,
        syntax=False,
        color=False,
        autocomplete=None,
        history_size=500,
        verbosity=None,
        timing=False,
        prompt1="%/=# ",
        prompt2="%/-# ",
        encoding=None,
        quiet=False,
        extended_display=False,
        tuples_only=False,
        format_="aligned",
        field_separator="|",
        record_separator="\n",
        sets=None,
        variables=None,
        translate_from=None,
        translate_to=None,
    ):

        if output is None:
            output = sys.stdout

        self.output = output

        self.isolation_level = isolation_level
        self.null = null

        if pager is None:
            pager = os.environ.get("PAGER")

        self.pager = pager
        self.syntax = syntax
        self.color = color
        self.autocomplete = autocomplete
        self.history_size = history_size
        self.verbosity = verbosity
        self.timing = timing
        self.prompt1 = prompt1
        self.prompt2 = prompt2
        self.encoding = encoding
        self.quiet = quiet
        self.extended_display = extended_display
        self.tuples_only = tuples_only
        self.format_ = format_
        self.field_separator = field_separator
        self.record_separator = record_separator

        if sets is None:
            sets = []

        self.sets = sets

        if variables is None:
            variables = {}

        self.variables = variables

        self.translate_from = translate_from
        self.translate_to = translate_to

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

    def run_sets(self, conn):
        for set_ in self.sets:
            conn.execute(text(set_))
            conn.execute(text("commit;"))


def trim_quotes(value):
    if isinstance(value, str):
        return value.strip("'")
    else:
        return value


def process_command_with_variable(command, line, default=None):
    if command is not None:
        remainder = line[len(command):].strip()
    else:
        remainder = line

    res = re.split(r"\s+", remainder, maxsplit=1)
    variable = res[0]
    if len(res) > 1:
        value = res[1]
    else:
        value = default

    return variable, trim_quotes(value)


def get_remainder(command, line):
    return line[len(command):].strip()


def process_command_with_value(command, line, default=None):
    remainder = get_remainder(command, line)

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


def set_set(variable, value):
    if variable.lower() == "prompt1":
        config.prompt1 = value
    elif variable.lower() == "prompt2":
        config.prompt2 = value
    elif variable.lower() == "histsize":
        config.history_size = int(value)
    elif variable.lower() == "verbosity":
        config.verbosity = value
    else:
        config.variables[variable] = value


def set_translate(from_, to):
    if from_ == "off":
        config.translate_from = None
        config.translate_to = None
    else:
        config.translate_from = from_
        config.translate_to = to

    if not config.quiet:
        if config.translate_from is None:
            sys.stdout.write("Translate is off.\n")
        else:
            sys.stdout.write('Translate is from "{} to "{}".\n'.format(from_, to))
        sys.stdout.flush()


def set_null_display(value):
    config.null = value

    if not config.quiet:
        sys.stdout.write('Null display is "{}".\n'.format(value))
        sys.stdout.flush()


def set_output(value):
    if not value:
        if config.output is not sys.stdout and config.output is not sys.stderr:
            config.output.close()

        config.output = sys.stdout
    else:
        if isinstance(value, str):
            value = os.path.expanduser(value)

        new_output = None
        if isinstance(value, str):
            try:
                new_output = open(value, "wt")
            except OSError:
                sys.stdout.write("{}: No such file or directory\n".format(value))
                sys.stdout.flush()
                return

        if config.output is not sys.stdout and config.output is not sys.stderr:
            config.output.close()

        if new_output is None:
            config.output = open(value, "wt")
        else:
            config.output = new_output


def set_format(value):
    if value not in ("aligned", "unaligned", "csv"):
        sys.stderr.write("\\pset: allowed formats are aligned, csv, unaligned\n")
        sys.stderr.flush()
        return

    config.format_ = value

    if not config.quiet:
        sys.stdout.write('Outupt format is "{}".\n'.format(value))
        sys.stdout.flush()


def set_timing(value):
    config.timing = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Timing is {}.\n".format(display_value))
        sys.stdout.flush()


def set_extended_display(value):
    config.extended_display = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Extended display is {}.\n".format(display_value))
        sys.stdout.flush()


def set_tuples_only(value):
    config.tuples_only = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Tuples only is {}.\n".format(display_value))
        sys.stdout.flush()


def set_field_separator(value):
    config.field_separator = value

    if not config.quiet:
        sys.stdout.write('Field seprator is "{}".\n'.format(value))
        sys.stdout.flush()


def set_record_separator(value):
    config.record_separator = value

    if not config.quiet:
        sys.stdout.write('Record seprator is "{}".\n'.format(value))
        sys.stdout.flush()


def set_syntax(conn, value):
    from .lexer import lexer

    config.syntax = value
    if value:
        display_value = "on"
        lexer.set_selected_by_name(conn.dialect.name)
    else:
        display_value = "off"
        lexer.set_selected_by_name(None)

    if not config.quiet:
        sys.stdout.write("Syntax is {}.\n".format(display_value))
        sys.stdout.flush()


def set_color(value):
    config.color = value
    if value:
        display_value = "on"
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Color is {}.\n".format(display_value))
        sys.stdout.flush()


def set_autocomplete(value):
    config.autocomplete = value
    if value:
        display_value = value
    else:
        display_value = "off"

    if not config.quiet:
        sys.stdout.write("Autocomplete is {}.\n".format(display_value))
        sys.stdout.flush()


def process_config_line(conn, filename, line_number, line):
    from .run import run_metacommand

    if line.startswith("--"):
        return
    elif line.startswith("\\pset"):
        run_metacommand(
            None,
            "pset",
            get_remainder("\\pset", line),
        )
    elif line.startswith("\\timing"):
        run_metacommand(
            None,
            "timing",
            get_remainder("\\timing", line),
        )
    elif line.startswith("\\translate"):
        run_metacommand(
            None,
            "translate",
            get_remainder("\\translate", line),
        )
    elif line.startswith("\\x"):
        run_metacommand(
            None,
            "x",
            get_remainder("\\x", line),
        )
    elif line.startswith("\\t"):
        run_metacommand(
            None,
            "t",
            get_remainder("\\t", line),
        )
    elif line.startswith("\\syntax"):
        value = process_command_with_boolean("\\syntax", line, default=not config.syntax)
        set_syntax(conn, value)
    elif line.startswith("\\color"):
        value = process_command_with_boolean("\\color", line, default=not config.color)
        set_color(value)
    elif line.startswith("\\autocomplete"):
        if config.autocomplete:
            default = None
        else:
            default = "auto"
        value = process_command_with_value("\\autocomplete", line, default=default)
        set_autocomplete(value)
    elif line.startswith("\\set"):
        variable, value = process_command_with_variable("\\set", line)
        set_set(variable, value)
    else:
        config.sets.append(line)
        start_time = time.monotonic_ns()
        conn.execute(text(line))

        conn.execute(text("commit;"))

        if not config.quiet:
            if re.search("^set", line, flags=re.I):
                sys.stdout.write("SET\n")
            elif re.search("^select", line, flags=re.I):
                sys.stdout.write("SELECT\n")

        total_time = time.monotonic_ns() - start_time
        if config.timing:
            write_time(total_time)

        sys.stdout.flush()


config = Configuration()
