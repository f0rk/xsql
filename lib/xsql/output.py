import csv
import itertools
import re
import shlex
import shutil
import subprocess
import sys
import time
from decimal import Decimal

from .config import config
from .formatters import as_str
from .time import write_time


def should_use_pager():

    is_tty = sys.stdin.isatty()

    use_pager = (
        config.pager
        and not config.format_ == "csv"
        and is_tty
    )

    return use_pager


def get_pager():
    args = shlex.split(config.pager)
    pager = subprocess.Popen(args, stdin=subprocess.PIPE, text=True)
    output = pager.stdin

    return pager, output


def get_output():

    pager = None

    if config.output is not sys.stdout:
        output = config.output
    else:
        if should_use_pager():
            pager, output = get_pager()
        else:
            output = config.output

    return pager, output


def write(records, title=None, show_rowcount=False, extra_content=None, total_time=0):

    pager, output = get_output()

    start_time = time.monotonic_ns()
    write_title = True
    write_header = not config.tuples_only
    total_rows = 0
    for batch in itertools.batched(records, 10000):

        total_time += time.monotonic_ns() - start_time
        start_time = time.monotonic_ns()

        if config.extended_display:
            total_rows += write_extended(
                output,
                batch,
                records,
                total_rows,
                title=title,
                write_title=write_title,
            )
            write_title = False
        elif config.format_ == "csv":
            total_rows += write_csv(
                output,
                batch,
                records,
                write_header=write_header,
            )
            write_header = False
        elif config.format_ == "unaligned":
            total_rows += write_unaligned(
                output,
                batch,
                records,
                title=title,
                write_title=write_title,
                write_header=write_header,
            )
            write_title = False
            write_header = False
        else:
            total_rows += write_aligned(
                output,
                batch,
                records,
                title=title,
                write_title=write_title,
                write_header=write_header,
            )
            write_title = False
            write_header = False

    if extra_content is not None:
        shutil.copyfileobj(extra_content, output)

    do_write_row_count = (
        show_rowcount
        and (
            not config.extended_display
            and not config.tuples_only
            and not config.format_ == "csv"
        )
    )

    if do_write_row_count:
        output.write("({} row".format(total_rows))
        if total_rows != 1:
            output.write("s")
        output.write(")\n")

    if not config.format_ == "csv":
        output.write("\n")

    if pager is not None:
        pager.communicate()

    do_write_timing = config.timing

    if do_write_timing:
        write_time(total_time)


def write_aligned(output, records, result, title=None, write_title=True, write_header=True):

    max_field_sizes = {}
    number_looking_fields = {}

    row_count = 0

    fieldnames = list(result.keys())

    for raw in records:

        row_count += 1

        for idx, key in enumerate(fieldnames):

            value = raw[idx]

            str_value = as_str(value)

            if idx not in max_field_sizes:
                max_field_sizes[idx] = len(str_value)
            if max_field_sizes[idx] < len(str_value):
                max_field_sizes[idx] = len(str_value)
            if max_field_sizes[idx] < len(key):
                max_field_sizes[idx] = len(key)

            if key not in number_looking_fields:
                number_looking_fields[idx] = True

            if isinstance(value, (int, float, Decimal)):
                pass
            elif not re.search(r'^-?([1-9]+[0-9]*|0)(\.[0-9]+)?$', str_value):
                number_looking_fields[idx] = False

    header_fmt_parts = []
    record_fmt_parts = []
    sep_parts = []
    for idx, _ in enumerate(fieldnames):
        header_fmt_parts.append(" {:^" + str(max_field_sizes[idx]) + "} ")

        align = "<"
        if number_looking_fields[idx]:
            align = ">"

        record_fmt_parts.append(" {:" + align + str(max_field_sizes[idx]) + "} ")
        sep_parts.append("-" + ("-" * max_field_sizes[idx]) + "-")

    header_fmt_str = "|".join(header_fmt_parts)
    record_fmt_str = "|".join(record_fmt_parts)

    if write_title and title is not None:
        title_width = len("+".join(sep_parts))
        title_fmt_str = "{:^" + str(title_width) + "}"
        output.write(title_fmt_str.format(title))
        output.write("\n")

    if write_header:
        output.write(header_fmt_str.format(*fieldnames))
        output.write("\n")
        output.write("+".join(sep_parts))
        output.write("\n")

    for raw in records:
        values = [as_str(v) for v in raw]
        output.write(record_fmt_str.format(*values))
        output.write("\n")

    return row_count


def write_unaligned(output, records, result, title=None, write_title=True, write_header=True):

    fieldnames = list(result.keys())

    if write_title and title is not None:
        output.write(title)
        output.write(config.record_separator)

    if write_header:
        output.write(config.field_separator.join(fieldnames))
        output.write(config.record_separator)

    row_count = 0

    for raw in records:

        row_count += 1

        values = [as_str(v) for v in raw]
        output.write(config.field_separator.join(values))
        output.write(config.record_separator)

    return row_count


def write_extended(output, records, result, total_rows, title=None, write_title=True):

    row_count = 0

    rotated = []

    max_column_size = None
    max_value_size = None

    fieldnames = list(result.keys())

    for raw in records:

        row_count += 1

        row_number = row_count + total_rows

        rotated.append(
            {
                "row_number": row_number,
            },
        )

        max_column_size = len("[ RECORD {} ]".format(row_number))
        max_value_size = 0

        for idx, key in enumerate(fieldnames):

            value = raw[idx]

            str_value = as_str(value)

            if len(key) > max_column_size:
                max_column_size = len(key)
            if len(str_value) > max_value_size:
                max_value_size = len(str_value)

            rotated.append(
                {
                    "column": key,
                    "value": str_value,
                },
            )

    for record in rotated:

        if record.get("row_number"):

            record_str = "-[ RECORD {} ]".format(record["row_number"])
            record_str = record_str + ("-" * ((max_column_size - len(record_str)) + 1))

            output.write(record_str)
            output.write("+")
            output.write("-" * (max_value_size + 2))
            output.write("\n")

        else:

            record_fmt_str = "{:<" + str(max_column_size + 1) + "}| {:<" + str(max_value_size + 1) + "}"

            output.write(record_fmt_str.format(record["column"], record["value"]))
            output.write("\n")

    return row_count


def write_csv(output, records, result, write_header=True, delimiter=","):

    fieldnames = list(result.keys())

    writer = csv.writer(output, delimiter=delimiter)
    if write_header:
        writer.writerow(fieldnames)

    row_count = 0

    for raw in records:
        row_count += 1

        writer.writerow([as_str(v) for v in raw])

    return row_count
