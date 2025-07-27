import re

from lark import Lark, Transformer

options_parser = Lark(r"""
	_STRING_INNER: /.*?/
	_STRING_ESC_INNER: _STRING_INNER /(?<!\\)(\\\\)*?/

	ESCAPED_STRING : "'" _STRING_ESC_INNER "'"

    STAR: "*"
    BOOLEAN: "true"i | "false"i

    filename: ESCAPED_STRING
    program: "program"i ESCAPED_STRING
    stdin: "stdin"i
    stdout: "stdout"i
    pstdin: "pstdin"i
    pstdout: "pstdout"i

    to_target: filename | program | stdout | pstdout
    from_target: filename | program | stdin | pstdin

    force_quote_column: QUOTED_IDENTIFIER | CNAME
    force_quote_cols: [force_quote_column ("," force_quote_column)*]
    force_not_null_column: QUOTED_IDENTIFIER | CNAME
    force_not_null_cols: [force_not_null_column ("," force_not_null_column)*]
    force_null_column: QUOTED_IDENTIFIER | CNAME
    force_null_cols: [force_null_column ("," force_null_column)*]

    header_boolean: "header"i BOOLEAN?
    header_match: "header"i "match"i
    on_error_stop: "on_error stop"i
    on_error_ignore: "on_error ignore"i
    log_verbosity_default: "default"i
    log_verbosity_verbose: "verbose"i
    log_verbosity_silent: "silent"i
    log_verbosity_options: log_verbosity_default | log_verbosity_verbose | log_verbosity_silent

    csv: "csv"i
    format_: "format"i ("csv"i | "text"i)
    freeze: "freeze"i BOOLEAN?
    delimiter: "delimiter"i ESCAPED_STRING
    null: "null"i ESCAPED_STRING
    default: "default"i ESCAPED_STRING
    header: header_boolean | header_match
    quote: "quote"i ESCAPED_STRING
    escape: "escape"i ESCAPED_STRING
    force_quote_columns: "force_quote"i "(" force_quote_cols ")"
    force_quote_all: "force_quote"i STAR
    force_quote: force_quote_columns | force_quote_all
    force_not_null: "force_not_null"i "(" force_not_null_cols ")"
    force_null: "force_null"i "(" force_null_cols ")"
    on_error: on_error_stop | on_error_ignore
    reject_limit: "reject_limit"i NUMBER
    encoding: "encoding"i ESCAPED_STRING
    log_verbosity: "log_verbosity"i log_verbosity_options

    with_: "with"i
    option: csv | format_ | freeze | delimiter | null | default | header | quote | escape | force_quote | force_not_null | force_null | on_error | reject_limit | encoding | log_verbosity
    options_parens: [option (", " option)*]
    options_bare: [option (option)*]
    with_options_parens: with_? "(" options_parens ")"
    with_options_bare: with_? options_bare
    with_options: with_options_parens | with_options_bare

    to: ("to"i to_target)? with_options
    from: ("from"i from_target)? with_options

    direction: to | from

    %import common.ESCAPED_STRING -> QUOTED_IDENTIFIER
    %import common.CNAME
    %import common.NUMBER
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
        freeze=None,
        delimiter=None,
        null=None,
        default=None,
        header=None,
        quote=None,
        escape=None,
        force_quote=None,
        force_not_null=None,
        force_null=None,
        on_error=None,
        reject_limit=None,
        encoding=None,
        log_verbosity=None,
    ):
        self.direction = direction
        self.target_type = target_type
        self.target = target
        self.format_ = format_
        self.freeze = freeze
        self.delimiter = delimiter
        self.null = null
        self.default = default
        self.header = header
        self.quote = quote
        self.escape = escape
        self.force_quote = force_quote
        self.force_not_null = force_not_null
        self.force_null = force_null
        self.on_error = on_error
        self.reject_limit = reject_limit
        self.encoding = encoding
        self.log_verbosity = log_verbosity


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

    def pstdin(self, s):
        self._options.target_type = "pipe"
        self._options.target = "pstdin"

    def pstdout(self, s):
        self._options.target_type = "pipe"
        self._options.target = "pstdout"

    def csv(self, s):
        self._options.format_ = "csv"

    def format_(self, s):
        if not s:
            return

        (s,) = s
        self._options.format_ = s

    def freeze(self, s):
        (s,) = s
        if s in ("true", "on"):
            self._options.freeze = True
        else:
            self._options.freeze = False

    def null(self, s):
        (s,) = s
        self._options.null = s[1:-1]

    def header_boolean(self, s):
        self._options.header = True

    def header_match(self, s):
        self._options.header = "match"

    def quote(self, s):
        (s,) = s
        self._options.quote = s[1:-1]

    def escape(self, s):
        self._options.escape = s[1:-1]

    def force_quote_all(self, s):
        self._options.force_quote = ["*"]

    def force_quote_column(self, s):
        if self._options.force_quote is None:
            self._options.force_quote = []
        (s,) = s
        self._options.force_quote.append(s)

    def force_not_null_column(self, s):
        if self._options.force_not_null is None:
            self._options.force_not_null = []
        (s,) = s
        self._options.force_not_null.append(s)

    def force_null_column(self, s):
        if self._options.force_null is None:
            self._options.force_null = []
        (s,) = s
        self._options.force_null.append(s)

    def on_error_stop(self, s):
        self._options.on_error = "stop"

    def on_error_ignore(self, s):
        self._options.on_error = "ignore"

    def reject_limit(self, s):
        (s,) = s
        self._options.reject_limit = s

    def log_verbosity_default(self, s):
        self._options.log_verbosity = "default"

    def log_verbosity_verbose(self, s):
        self._options.log_verbosity = "verbose"

    def log_verbosity_silent(self, s):
        self._options.log_verbosity = "silent"

    def encoding(self, s):
        self._options.encoding = s[1:-1]


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

    if re.search(r"\b(format)?\s+csv", options, flags=re.I):
        parsed.format_ = "csv"
        options = re.sub(r"\b(format)?\s+csv,?", "", options)

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
