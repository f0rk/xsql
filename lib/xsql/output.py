import itertools
import re
import shlex
import subprocess
import sys
from datetime import datetime
from decimal import Decimal

from .config import config


def write(records):

    pager = None

    if config.pager:
        args = shlex.split(config.pager)
        pager = subprocess.Popen(args, stdin=subprocess.PIPE, text=True)
        output = pager.stdin
    else:
        output = sys.stdout

    write_header = True
    for batch in itertools.batched(records, 10000):
        _write(output, batch, records, write_header=write_header)
        write_header = False

    if pager is not None:
        pager.communicate()


def as_str(v):
    if v is None:
        return config.null
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Decimal):
        # see https://stackoverflow.com/questions/11093021/python-decimal-to-string
        # for why str(obj) isn't just used
        return "{0:f}".format(v)
    if isinstance(v, set):
        v = [*v]
        return str(v)
    return str(v)


def _write(output, records, result, write_header=True):

    max_field_sizes = {}
    number_looking_fields = {}

    for raw in records:

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
