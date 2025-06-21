import json
import re


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
                    value = str(value)

                value = copy_data_escape(value)

                values.append(value)

        data = self.delimiter.join(values) + self.newline
        return data

    def writerow(self, row):
        data = self.format(row)
        self.fp.write(data)
