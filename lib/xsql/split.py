from .quote import get_quote_positions, is_in_quote


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
                parts.append(buffer)
                buffer = ""

    return parts
