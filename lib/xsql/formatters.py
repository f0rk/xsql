import json
import re
from datetime import datetime
from decimal import Decimal

from .config import config


def reformat_datetime(match):

    microseconds = match.group(2)
    microseconds = microseconds.rstrip("0")
    if microseconds.endswith("."):
        microseconds = microseconds.rstrip(".")

    zone = match.group(3)
    if zone:
        if zone.endswith(":00"):
            zone = zone[:-3]
    else:
        zone = ""

    return match.group(1) + microseconds + zone


def as_str(v):
    if v is None:
        return config.null
    if isinstance(v, bool):
        if v is True:
            return "t"
        elif v is False:
            return "f"
    if isinstance(v, datetime):
        v = v.isoformat(sep=" ")

        v = re.sub(
            r"^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})([.][0-9]+)(([+-])([0-9]+:[0-9]+))?$",
            reformat_datetime,
            v,
        )

        return v
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
    if isinstance(v, dict):
        return json.dumps(v)
    return str(v)


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


def copy_data_escape(value):
    value = re.sub(r"([\\])", r"\\\1", value)

    value = value.replace("\n", "\\n")
    value = value.replace("\r", "\\r")
    value = value.replace("\t", "\\t")

    return value


class CopyWriter:

    def __init__(self, fp, null="\\N", newline="\n", delimiter="\t"):
        self.fp = fp
        self.null = null
        self.newline = newline
        self.delimiter = delimiter

    def format(self, row):

        values = []

        for value in row:
            if value is None:
                value = self.null
            else:
                if isinstance(value, (list, dict)):
                    if isinstance(value, list):
                        use_json = False
                        if value:
                            if isinstance(value[0], (list, dict)):
                                use_json = True
                    else:
                        use_json = True

                    if use_json:
                        value = json.dumps(value)
                    else:
                        value = list_to_array(value)
                else:
                    value = as_str(value)

                value = copy_data_escape(value)

                values.append(value)

        data = self.delimiter.join(values) + self.newline
        return data

    def writerow(self, row):
        data = self.format(row)
        self.fp.write(data)
