import csv
import itertools
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from decimal import Decimal

from .config import config


def write(records, title=None, show_rowcount=False):

    pager = None

    use_pager = (
        config.pager
        and not config.format_ == "csv"
    )

    if config.output is not sys.stdout:
        output = config.output
    else:
        if use_pager:
            args = shlex.split(config.pager)
            pager = subprocess.Popen(args, stdin=subprocess.PIPE, text=True)
            output = pager.stdin
        else:
            output = config.output

    start_time = time.monotonic()
    total_time = 0
    write_title = True
    write_header = not config.tuples_only
    total_rows = 0
    for batch in itertools.batched(records, 10000):

        total_time += time.monotonic() - start_time
        start_time = time.monotonic()

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
        sys.stdout.write("Time: {:.3f} ms\n".format(total_time * 1000))


def as_str(v):
    if v is None:
        return config.null
    if isinstance(v, bool):
        if v is True:
            return "t"
        elif v is False:
            return "f"
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Decimal):
        # see https://stackoverflow.com/questions/11093021/python-decimal-to-string
        # for why str(obj) isn't just used
        return "{0:f}".format(v)
    if isinstance(v, set):
        v = [*v]
        return list_to_array(v)
    if isinstance(v, list):
        return list_to_array(v)
    if isinstance(v, bytes):
        return v.hex()
    return str(v)


def write_aligned(output, records, result, title=None, write_title=True, write_header=True):

    max_field_sizes = {}
    number_looking_fields = {}

    row_count = 0

    for raw in records:

        row_count += 1

        record = raw._asdict()

        for key, value in record.items():

            str_value = as_str(value)

            if key not in max_field_sizes:
                max_field_sizes[key] = len(str_value)
            if max_field_sizes[key] < len(str_value):
                max_field_sizes[key] = len(str_value)
            if max_field_sizes[key] < len(key):
                max_field_sizes[key] = len(key)

            if key not in number_looking_fields:
                number_looking_fields[key] = True

            if isinstance(value, (int, float, Decimal)):
                pass
            elif not re.search(r'^-?([1-9]+[0-9]*|0)(\.[0-9]+)?$', str_value):
                number_looking_fields[key] = False

    fieldnames = list(result.keys())

    header_fmt_parts = []
    record_fmt_parts = []
    sep_parts = []
    for field in fieldnames:
        header_fmt_parts.append(" {:^" + str(max_field_sizes[field]) + "} ")

        align = "<"
        if number_looking_fields[field]:
            align = ">"

        record_fmt_parts.append(" {:" + align + str(max_field_sizes[field]) + "} ")
        sep_parts.append("-" + ("-" * max_field_sizes[field]) + "-")

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

        record = raw._asdict()

        values = [as_str(record[f]) for f in fieldnames]
        output.write(record_fmt_str.format(*values))
        output.write("\n")

    return row_count


def write_unaligned(output, records, result, title=None, write_title=True, write_header=True):

    fieldnames = list(result.keys())

    if write_title and title is not None:
        output.write(title)
        output.write("\n")

    if write_header:
        output.write("|".join(fieldnames))
        output.write("\n")

    row_count = 0

    for raw in records:

        row_count += 1

        record = raw._asdict()

        values = [as_str(record[f]) for f in fieldnames]
        output.write("|".join(values))
        output.write("\n")

    return row_count


def write_extended(output, records, result, total_rows, title=None, write_title=True):

    row_count = 0

    rotated = []

    max_column_size = None
    max_value_size = None

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

        record = raw._asdict()

        for key, value in record.items():

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


def write_csv(output, records, result, write_header=True):

    fieldnames = list(result.keys())

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    row_count = 0

    for raw in records:
        row_count += 1

        data = {key: as_str(value) for key, value in raw._asdict().items()}

        writer.writerow(data)

    return row_count


def format_array_entry(value):
    if re.search("[a-zA-Z0-9_-]", value):
        return value
    return '"' + re.sub(r'"', '\\"', value) + '"'


def convert_array_value(value):
    if value is None:
        return "NULL"

    return format_array_entry(str(value))


def list_to_array(values):
    converted = [convert_array_value(v) for v in values]
    return "{" + ",".join(converted) + "}"
