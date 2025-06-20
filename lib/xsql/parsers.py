import re

from lark import Lark, Transformer


options_parser = Lark(r"""
	_STRING_INNER: /.*?/
	_STRING_ESC_INNER: _STRING_INNER /(?<!\\)(\\\\)*?/

	ESCAPED_STRING : "'" _STRING_ESC_INNER "'"

    filename: ESCAPED_STRING
    program: "program"i ESCAPED_STRING
    stdin: "stdin"i
    stdout: "stdout"i

    to_target: filename | program | stdout
    from_target: filename | program | stdin

    csv: "csv"i
    format_: "format"i ("csv"i | "text"i)
    delimiter: "delimiter"i ESCAPED_STRING
    null: "null"i ESCAPED_STRING
    header: "header"i
    quote: "quote"i ESCAPED_STRING
    escape: "escape"i ESCAPED_STRING

    with_: "with"i
    option: csv | format_ | delimiter | null | header | quote | escape
    options: with_? [option*]

    to: ("to"i to_target)? options
    from: ("from"i from_target)? options

    direction: to | from

    %import common.WS
    %ignore WS
""", start="direction")


class Options:

    def __init__(
        self,
        direction=None,
        target_type=None,
        target=None,
        format_="text",
        delimiter=None,
        null=None,
        header=None,
        quote=None,
        escape=None,
    ):
        self.direction = direction
        self.target_type = target_type
        self.target = target
        self.format_ = format_
        self.delimiter = delimiter
        self.null = null
        self.header = header
        self.quote = quote
        self.escape = escape


class OptionsTransformer(Transformer):

    def __init__(self, options):
        self._options = options

    def to_target(self, s):
        self._options.direction = "to"

    def from_target(self, s):
        self._options.direction = "from"

    def filename(self, s):
        (s,) = s
        self._options.target_type = "file"
        self._options.target = s[1:-1]

    def program(self, s):
        (s,) = s
        self._options.target_type = "program"
        self._options.target = s

    def stdin(self, s):
        self._options.target_type = "pipe"
        self._options.target = "stdin"

    def stdout(self, s):
        self._options.target_type = "pipe"
        self._options.target = "stdout"

    def csv(self, s):
        self._options.format_ = "csv"

    def format_(self, s):
        (s,) = s
        self._options.format_ = s

    def null(self, s):
        (s,) = s
        self._options.null = s[1:-1]

    def header(self, s):
        self._options.header = True

    def quote(self, s):
        (s,) = s
        self._options.quote = s[1:-1]

    def escape(self, s):
        self._options.escape = s[1:-1]


def parse_options(options):

    parsed = Options()

    options = options.strip()
    options = options.lstrip(")")
    options = options.strip()

    direction = None
    target = None

    # unquoted filename
    if not re.search(r"^(to|from)\s+('|program\b|stdout\b|stdin\b)", options):
        res = re.split(r"\s+", options, maxsplit=2)
        direction = res[0]
        target = res[1]
        if len(res) > 2:
            rest = res[2]
        else:
            rest = ""

        parsed.direction = direction
        parsed.target_type = "file"
        parsed.target = target

        options = rest

    if options:
        tree = options_parser.parse(options)
        transformer = OptionsTransformer(parsed)
        transformer.transform(tree)

    return parsed


table_parser = Lark(r"""
    schema: ESCAPED_STRING | CNAME
    table: ESCAPED_STRING | CNAME
    target: (schema ".")? table

    column: ESCAPED_STRING | CNAME
    columns: [column ("," column)*]

    directive: target ("(" columns ")")?

    %import common.ESCAPED_STRING
    %import common.CNAME
    %import common.WS
    %ignore WS
""", start="directive")


class Table:

    def __init__(
        self,
        schema=None,
        table=None,
        columns=None,
    ):
        self.schema = schema
        self.table = table
        self.columns = columns


class TableTransformer(Transformer):

    def __init__(self, result):
        self.result = result

    def table(self, s):
        (s,) = s
        self.result.table = s

    def schema(self, s):
        (s,) = s
        self.result.schema = s

    def column(self, s):
        if self.result.columns is None:
            self.result.columns = []

        (s,) = s
        self.result.columns.append(s)


def query_from_table_directive(table):

    result = Table()

    tree = table_parser.parse(table)
    transformer = TableTransformer(result)
    transformer.transform(tree)

    target = result.table
    if result.schema:
        target = result.schema + "." + result.table

    if result.columns:
        query = "select " + ", ".join(result.columns) + " from " + target
    else:
        query = "select * from " + target

    return query


def parse_copy(command):

    # remove copy from front
    command = re.sub(r"^\s*copy\s*", "", command, flags=re.I)

    # it's a query
    if command.startswith("("):

        match = re.search(r"\)\s*(to|from)\s+.+?$", command)

        query = command[1:match.start()]
        rest = command[match.start():]

    # i guess just a direct table reference
    else:
        match = re.search(r"\s*(to|from)\s+.+?$", command)

        table = command[:match.start()]
        rest = command[match.start():]

        query = query_from_table_directive(table)

    options = parse_options(rest)

    return query, options


# parse("COPY plans (id, name) from /tmp/derp.copy csv header")
# parse("COPY (select * from plans join (select * from jimmy where name = '(foo''bas') as s on plans.id = jimmy.id) to '/tmp/derp.copy' header quote '\"'")
