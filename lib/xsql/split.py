import io
import re

from .quote import get_quote_positions, is_in_quote


def is_empty(query):

    query = query.strip()

    lines = 0

    for line in io.StringIO(query):
        if re.search(r"^\s*--", line):
            continue

        lines += 1

    return lines == 0


def split_command(data, dialect):

    allow_dollar_quoting = False
    if dialect in ("postgresql", "redshift"):
        allow_dollar_quoting = True

    quote_positions = get_quote_positions(
        data,
        allow_dollar_quoting=allow_dollar_quoting,
    )

    parts = []

    buffer = ""

    for idx, c in enumerate(data):
        buffer += c

        if c == ";":
            if not is_in_quote(quote_positions, idx):
                if not is_empty(buffer):
                    parts.append(buffer)
                    buffer = ""

    if not is_empty(buffer):
        parts.append(buffer)

    return parts
