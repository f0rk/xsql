import os
import re
import sys

import sqlglot
import sqlglot.expressions
from prompt_toolkit.application.current import get_app
from prompt_toolkit.completion import (
    Completer,
    Completion,
    DynamicCompleter,
)
from prompt_toolkit.shortcuts import CompleteStyle
from sqlalchemy import text

from .config import config


completion_cache = {}


def clear_completions():
    completion_cache.clear()


def maybe_refresh_completions(conn):
    if not completion_cache:
        refresh_completions(conn)


def refresh_completions(conn):

    if conn.dialect.name == "postgresql":
        names_query = """
        select
            pg_catalog.pg_namespace.nspname as schema_name,
            pg_catalog.pg_class.relname as table_name,
            pg_catalog.pg_attribute.attname as column_name,
            null as function_name
        from
            pg_catalog.pg_namespace
        join
            pg_catalog.pg_class
        on
            pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
        join
            pg_catalog.pg_attribute
        on
            pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid
        where
            pg_catalog.pg_class.relkind in ('r', 'v')
        union
        select
            pg_catalog.pg_namespace.nspname as schema_name,
            null as table_name,
            null as column_name,
            pg_catalog.pg_proc.proname || '(' as function_name
        from
            pg_catalog.pg_namespace
        join
            pg_catalog.pg_proc
        on
            pg_catalog.pg_namespace.oid = pg_catalog.pg_proc.pronamespace
        """

    elif conn.dialect.name == "sqlite":
        names_query = """
        select
            null as schema_name,
            sqlite_master.tbl_name as table_name,
            info.name as column_name,
            null as function_name
        from
            sqlite_master,
            pragma_table_info(sqlite_master.tbl_name) as info
        """
    elif conn.dialect.name == "snowflake":
        if config.verbosity:
            sys.stdout.write("refreshing autocomplete cache\n")
            sys.stdout.flush()

        names_query = """
        select
            table_schema as schema_name,
            table_name as table_name,
            column_name as column_name,
            null as function_name
        from
            information_schema.columns
        union
        select
            procedure_schema as schema_name,
            null as table_name,
            null as column_name,
            procedure_name || '(' as function_name
        from
            information_schema.procedures
        """
    else:
        if conn.dialect.name == "redshift":
            if config.verbosity:
                sys.stdout.write("refreshing autocomplete cache\n")
                sys.stdout.flush()

        names_query = """
        select
            table_schema as schema_name,
            table_name as table_name,
            column_name as column_name,
            null as function_name
        from
            information_schema.columns
        union
        select
            routine_schema as schema_name,
            null as table_name,
            null as column_name,
            routine_name || '(' as function_name
        from
            information_schema.routines
        """

    new_values = {}
    name_results = conn.execute(text(names_query))
    for name_result in name_results:
        new_values.setdefault(name_result.schema_name, {})
        if name_result.function_name:
            new_values[name_result.schema_name].setdefault("functions", [])
            new_values[name_result.schema_name]["functions"].append(name_result.function_name)
        else:
            new_values[name_result.schema_name].setdefault("tables", {})
            new_values[name_result.schema_name]["tables"].setdefault(name_result.table_name, [])
            new_values[name_result.schema_name]["tables"][name_result.table_name].append(name_result.column_name)

    completion_cache.clear()
    completion_cache.update(new_values)


sql_keywords = [
    "a",
    "abort",
    "abs",
    "absent",
    "absolute",
    "access",
    "according",
    "acos",
    "action",
    "ada",
    "add",
    "admin",
    "after",
    "aggregate",
    "all",
    "allocate",
    "also",
    "alter",
    "always",
    "analyse",
    "analyze",
    "and",
    "any",
    "any_value",
    "are",
    "array",
    "array_agg",
    "as",
    "asc",
    "asensitive",
    "asin",
    "assertion",
    "assignment",
    "asymmetric",
    "at",
    "atan",
    "atomic",
    "attach",
    "attribute",
    "attributes",
    "authorization",
    "avg",
    "backward",
    "base64",
    "before",
    "begin",
    "begin_frame",
    "begin_partition",
    "bernoulli",
    "between",
    "bigint",
    "binary",
    "bit",
    "bit_length",
    "blob",
    "blocked",
    "bom",
    "boolean",
    "both",
    "breadth",
    "btrim",
    "by",
    "c",
    "cache",
    "call",
    "called",
    "cardinality",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "catalog_name",
    "ceil",
    "ceiling",
    "chain",
    "chaining",
    "char",
    "character",
    "characteristics",
    "characters",
    "character_length",
    "character_",
    "character_set_name",
    "character_set_schema",
    "char_length",
    "check",
    "checkpoint",
    "class",
    "classifier",
    "class_origin",
    "clob",
    "close",
    "cluster",
    "coalesce",
    "cobol",
    "collate",
    "collation",
    "collation_catalog",
    "collation_name",
    "collation_schema",
    "collect",
    "column",
    "columns",
    "column_name",
    "command_function",
    "command_",
    "comment",
    "comments",
    "commit",
    "committed",
    "compression",
    "concurrently",
    "condition",
    "conditional",
    "condition_number",
    "configuration",
    "conflict",
    "connect",
    "connection",
    "connection_name",
    "constraint",
    "constraints",
    "constraint_catalog",
    "constraint_name",
    "constraint_schema",
    "constructor",
    "contains",
    "content",
    "continue",
    "control",
    "conversion",
    "convert",
    "copartition",
    "copy",
    "corr",
    "corresponding",
    "cos",
    "cosh",
    "cost",
    "count",
    "covar_pop",
    "covar_samp",
    "create",
    "cross",
    "csv",
    "cube",
    "cume_dist",
    "current",
    "current_catalog",
    "current_date",
    "current_",
    "current_path",
    "current_role",
    "current_row",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_",
    "current_user",
    "cursor",
    "cursor_name",
    "cycle",
    "data",
    "database",
    "datalink",
    "date",
    "datetime_",
    "datetime_",
    "day",
    "db",
    "deallocate",
    "dec",
    "decfloat",
    "decimal",
    "declare",
    "default",
    "defaults",
    "deferrable",
    "deferred",
    "define",
    "defined",
    "definer",
    "degree",
    "delete",
    "delimiter",
    "delimiters",
    "dense_rank",
    "depends",
    "depth",
    "deref",
    "derived",
    "desc",
    "describe",
    "descriptor",
    "detach",
    "deterministic",
    "diagnostics",
    "dictionary",
    "disable",
    "discard",
    "disconnect",
    "dispatch",
    "distinct",
    "dlnewcopy",
    "dlpreviouscopy",
    "dlurlcomplete",
    "dlurlcompleteonly",
    "dlurlcompletewrite",
    "dlurlpath",
    "dlurlpathonly",
    "dlurlpathwrite",
    "dlurlscheme",
    "dlurlserver",
    "dlvalue",
    "do",
    "document",
    "domain",
    "double",
    "drop",
    "dynamic",
    "dynamic_function",
    "dynamic_",
    "each",
    "element",
    "else",
    "empty",
    "enable",
    "encoding",
    "encrypted",
    "end",
    "end",
    "end_frame",
    "end_partition",
    "enforced",
    "enum",
    "equals",
    "error",
    "escape",
    "event",
    "every",
    "except",
    "exception",
    "exclude",
    "excluding",
    "exclusive",
    "exec",
    "execute",
    "exists",
    "exp",
    "explain",
    "expression",
    "extension",
    "external",
    "extract",
    "false",
    "family",
    "fetch",
    "file",
    "filter",
    "final",
    "finalize",
    "finish",
    "first",
    "first_value",
    "flag",
    "float",
    "floor",
    "following",
    "for",
    "force",
    "foreign",
    "format",
    "fortran",
    "forward",
    "found",
    "frame_row",
    "free",
    "freeze",
    "from",
    "fs",
    "fulfill",
    "full",
    "function",
    "functions",
    "fusion",
    "g",
    "general",
    "generated",
    "get",
    "global",
    "go",
    "goto",
    "grant",
    "granted",
    "greatest",
    "group",
    "grouping",
    "groups",
    "handler",
    "having",
    "header",
    "hex",
    "hierarchy",
    "hold",
    "hour",
    "id",
    "identity",
    "if",
    "ignore",
    "ilike",
    "immediate",
    "immediately",
    "immutable",
    "implementation",
    "implicit",
    "import",
    "in",
    "include",
    "including",
    "increment",
    "indent",
    "index",
    "indexes",
    "indicator",
    "inherit",
    "inherits",
    "initial",
    "initially",
    "inline",
    "inner",
    "inout",
    "input",
    "insensitive",
    "insert",
    "instance",
    "instantiable",
    "instead",
    "int",
    "integer",
    "integrity",
    "intersect",
    "intersection",
    "interval",
    "into",
    "invoker",
    "is",
    "isnull",
    "isolation",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_table_primitive",
    "json_value",
    "k",
    "keep",
    "key",
    "keys",
    "key_member",
    "key_type",
    "label",
    "lag",
    "language",
    "large",
    "last",
    "last_value",
    "lateral",
    "lead",
    "leading",
    "leakproof",
    "least",
    "left",
    "length",
    "level",
    "library",
    "like",
    "like_regex",
    "limit",
    "link",
    "listagg",
    "listen",
    "ln",
    "load",
    "local",
    "localtime",
    "localtimestamp",
    "location",
    "locator",
    "lock",
    "locked",
    "log",
    "log10",
    "logged",
    "lower",
    "lpad",
    "ltrim",
    "m",
    "map",
    "mapping",
    "match",
    "matched",
    "matches",
    "match_number",
    "match_recognize",
    "materialized",
    "max",
    "maxvalue",
    "measures",
    "member",
    "merge",
    "merge_action",
    "message_length",
    "message_octet_length",
    "message_text",
    "method",
    "min",
    "minute",
    "minvalue",
    "mod",
    "mode",
    "modifies",
    "module",
    "month",
    "more",
    "move",
    "multiset",
    "mumps",
    "name",
    "names",
    "namespace",
    "national",
    "natural",
    "nchar",
    "nclob",
    "nested",
    "nesting",
    "new",
    "next",
    "nfc",
    "nfd",
    "nfkc",
    "nfkd",
    "nil",
    "no",
    "none",
    "normalize",
    "normalized",
    "not",
    "nothing",
    "notify",
    "notnull",
    "nowait",
    "nth_value",
    "ntile",
    "null",
    "nullable",
    "nullif",
    "nulls",
    "null_ordering",
    "number",
    "numeric",
    "object",
    "occurrence",
    "occurrences_regex",
    "octets",
    "octet_length",
    "of",
    "off",
    "offset",
    "oids",
    "old",
    "omit",
    "on",
    "one",
    "only",
    "open",
    "operator",
    "option",
    "options",
    "or",
    "order",
    "ordering",
    "ordinality",
    "others",
    "out",
    "outer",
    "output",
    "over",
    "overflow",
    "overlaps",
    "overlay",
    "overriding",
    "owned",
    "owner",
    "p",
    "pad",
    "parallel",
    "parameter",
    "parameter_mode",
    "parameter_name",
    "parameter_",
    "parameter_",
    "parameter_",
    "parameter_",
    "parser",
    "partial",
    "partition",
    "pascal",
    "pass",
    "passing",
    "passthrough",
    "password",
    "past",
    "path",
    "pattern",
    "per",
    "percent",
    "percentile_cont",
    "percentile_disc",
    "percent_rank",
    "period",
    "permission",
    "permute",
    "pipe",
    "placing",
    "plan",
    "plans",
    "pli",
    "policy",
    "portion",
    "position",
    "position_regex",
    "power",
    "precedes",
    "preceding",
    "precision",
    "prepare",
    "prepared",
    "preserve",
    "prev",
    "primary",
    "prior",
    "private",
    "privileges",
    "procedural",
    "procedure",
    "procedures",
    "program",
    "prune",
    "ptf",
    "public",
    "publication",
    "quote",
    "quotes",
    "range",
    "rank",
    "read",
    "reads",
    "real",
    "reassign",
    "recheck",
    "recovery",
    "recursive",
    "ref",
    "references",
    "referencing",
    "refresh",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "reindex",
    "relative",
    "release",
    "rename",
    "repeatable",
    "replace",
    "replica",
    "requiring",
    "reset",
    "respect",
    "restart",
    "restore",
    "restrict",
    "result",
    "return",
    "returned_cardinality",
    "returned_length",
    "returned_",
    "returned_sqlstate",
    "returning",
    "returns",
    "revoke",
    "right",
    "role",
    "rollback",
    "rollup",
    "routine",
    "routines",
    "routine_catalog",
    "routine_name",
    "routine_schema",
    "row",
    "rows",
    "row_count",
    "row_number",
    "rpad",
    "rtrim",
    "rule",
    "running",
    "savepoint",
    "scalar",
    "scale",
    "schema",
    "schemas",
    "schema_name",
    "scope",
    "scope_catalog",
    "scope_name",
    "scope_schema",
    "scroll",
    "search",
    "second",
    "section",
    "security",
    "seek",
    "select",
    "selective",
    "self",
    "semantics",
    "sensitive",
    "sequence",
    "sequences",
    "serializable",
    "server",
    "server_name",
    "session",
    "session_user",
    "set",
    "setof",
    "sets",
    "share",
    "show",
    "similar",
    "simple",
    "sin",
    "sinh",
    "size",
    "skip",
    "smallint",
    "snapshot",
    "some",
    "sort_direction",
    "source",
    "space",
    "specific",
    "specifictype",
    "specific_name",
    "sql",
    "sqlcode",
    "sqlerror",
    "sqlexception",
    "sqlstate",
    "sqlwarning",
    "sqrt",
    "stable",
    "standalone",
    "start",
    "state",
    "statement",
    "static",
    "statistics",
    "stddev_pop",
    "stddev_samp",
    "stdin",
    "stdout",
    "storage",
    "stored",
    "strict",
    "string",
    "strip",
    "structure",
    "style",
    "subclass_origin",
    "submultiset",
    "subscription",
    "subset",
    "substring",
    "substring_regex",
    "succeeds",
    "sum",
    "support",
    "symmetric",
    "sysid",
    "system",
    "system_time",
    "system_user",
    "t",
    "table",
    "tables",
    "tablesample",
    "tablespace",
    "table_name",
    "tan",
    "tanh",
    "target",
    "temp",
    "template",
    "temporary",
    "text",
    "then",
    "through",
    "ties",
    "time",
    "timestamp",
    "timezone_hour",
    "timezone_minute",
    "to",
    "token",
    "top_level_count",
    "trailing",
    "transaction",
    "transactions_",
    "transactions_",
    "transaction_active",
    "transform",
    "transforms",
    "translate",
    "translate_regex",
    "translation",
    "treat",
    "trigger",
    "trigger_catalog",
    "trigger_name",
    "trigger_schema",
    "trim",
    "trim_array",
    "true",
    "truncate",
    "trusted",
    "type",
    "types",
    "uescape",
    "unbounded",
    "uncommitted",
    "unconditional",
    "under",
    "unencrypted",
    "union",
    "unique",
    "unknown",
    "unlink",
    "unlisten",
    "unlogged",
    "unmatched",
    "unnamed",
    "unnest",
    "until",
    "untyped",
    "update",
    "upper",
    "uri",
    "usage",
    "user",
    "user_",
    "user_",
    "user_",
    "user_",
    "using",
    "utf16",
    "utf32",
    "utf8",
    "vacuum",
    "valid",
    "validate",
    "validator",
    "value",
    "values",
    "value_of",
    "varbinary",
    "varchar",
    "variadic",
    "varying",
    "var_pop",
    "var_samp",
    "verbose",
    "version",
    "versioning",
    "view",
    "views",
    "volatile",
    "when",
    "whenever",
    "where",
    "whitespace",
    "width_bucket",
    "window",
    "with",
    "within",
    "without",
    "work",
    "wrapper",
    "write",
    "xml",
    "xmlagg",
    "xmlattributes",
    "xmlbinary",
    "xmlcast",
    "xmlcomment",
    "xmlconcat",
    "xmldeclaration",
    "xmldocument",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmliterate",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlquery",
    "xmlroot",
    "xmlschema",
    "xmlserialize",
    "xmltable",
    "xmltext",
    "xmlvalidate",
    "year",
    "yes",
    "zone",
]


def generator():

    if not config.autocomplete:
        return

    yielded = set()

    for sql_keyword in sql_keywords:
        if sql_keyword not in yielded:
            yielded.add(sql_keyword)
            yield sql_keyword

    for schema_name in completion_cache.keys():
        if schema_name is not None:
            if schema_name not in yielded:
                yielded.add(schema_name)
                yield schema_name

        for function_name in completion_cache[schema_name].get("functions") or []:
            if function_name not in yielded:
                yielded.add(function_name)
                yield function_name

        for table_name in completion_cache[schema_name].get("tables", {}).keys():
            if table_name not in yielded:
                yielded.add(table_name)
                yield table_name

            for column_name in completion_cache[schema_name]["tables"][table_name]:
                if column_name not in yielded:
                    yielded.add(column_name)
                    yield column_name


def is_file_completion(text):
    match = re.search(r"^\\(i|o|cd)(\b|\s)", text)
    return match


def is_exec_completion(text):
    match = re.search(r"^\\([!])(\b|\s)", text)
    return match


class PathCompleter(Completer):

    get_paths = None
    file_filter = None

    def get_completions(self, document, complete_event):

        text = document.text_before_cursor.strip()

        match = is_file_completion(text)
        text = text[match.span()[1]:].strip()

        return self.get_completions_for_text(text)

    def get_completions_for_text(self, text):
        try:

            dirname = os.path.dirname(text)
            if dirname:
                directories = [dirname]
            elif self.get_paths:
                directories = self.get_paths()
            else:
                directories = [os.getcwd()]

            prefix = os.path.basename(text)

            filenames = []

            for directory in directories:

                try:
                    to_search = os.listdir(os.path.expanduser(directory))
                except OSError:
                    continue

                for filename in to_search:
                    if filename.startswith(prefix):
                        filenames.append((directory, filename))

            filenames.sort(key=lambda e: e[1])

            for directory, filename in filenames:

                completion = filename[len(prefix):]
                file_path = os.path.join(directory, filename)

                if os.path.isdir(os.path.expanduser(file_path)):
                    file_path += "/"

                if self.file_filter:
                    if not self.file_filter(file_path):
                        continue

                yield Completion(
                    text=completion,
                    start_position=0,
                    display=filename,
                )
        except OSError:
            pass


class ExecutableCompleter(PathCompleter):

    def get_paths(self):
        return os.environ.get("PATH", "").split(os.pathsep)

    def file_filter(self, path):
        return os.access(os.path.expanduser(path), os.X_OK)

    def get_completions(self, document, complete_event):

        text = document.text_before_cursor.strip()

        match = is_exec_completion(text)
        text = text[match.span()[1]:].strip()

        return self.get_completions_for_text(text)


def get_completer():

    app = get_app()

    if is_file_completion(app.current_buffer.text.strip()):
        return PathCompleter()
    elif is_exec_completion(app.current_buffer.text.strip()):
        return ExecutableCompleter()
    else:
        return SQLCompleter()


context_free_keywords = [
    "ABORT",
    "COMMENT",
    "DO",
    "INSERT",
    "REFRESH MATERIALIZED VIEW",
    "SELECT",
    "VACUUM",
    "ALTER",
    "COMMIT",
    "DROP",
    "LISTEN",
    "REINDEX",
    "SET",
    "VALUES",
    "ANALYZE",
    "COPY",
    "END",
    "LOAD",
    "RELEASE",
    "SHOW",
    "WITH",
    "BEGIN",
    "CREATE",
    "EXECUTE",
    "LOCK",
    "RESET",
    "START",
    "CALL",
    "DEALLOCATE",
    "EXPLAIN",
    "MOVE",
    "REVOKE",
    "TABLE",
    "CHECKPOINT",
    "DECLARE",
    "FETCH",
    "NOTIFY",
    "ROLLBACK",
    "TRUNCATE",
    "CLOSE",
    "DELETE FROM",
    "GRANT",
    "PREPARE",
    "SAVEPOINT",
    "UNLISTEN",
    "CLUSTER",
    "DISCARD",
    "IMPORT",
    "REASSIGN",
    "SECURITY LABEL",
    "UPDATE",
]


def basic_completion(text):
    return Completion(
        text=text,
        start_position=0,
        display=text,
    )


class SQLCompleter(Completer):

    def get_completions(self, document, complete_event):

        text = document.text_before_cursor.strip()

        if not text:
            for keyword in context_free_keywords:

                keyword = keyword + " "

                yield Completion(
                    text=keyword[len(text):],
                    start_position=0,
                    display=keyword,
                )
        elif re.search(r"^\w+$", text):
            for keyword in context_free_keywords:

                keyword = keyword + " "

                if not keyword.lower().startswith(text.lower()):
                    continue

                if text[-1:].islower():
                    keyword = keyword.lower()

                yield Completion(
                    text=keyword[len(text):],
                    start_position=0,
                    display=keyword,
                )
        elif select_match := re.search(r"(select)\s+[*]\s*$", text, flags=re.I):
            keyword = "FROM "
            if "t" in select_match.groups()[0]:
                keyword = "from "

            if text.endswith("*"):
                keyword = " " + keyword

            yield basic_completion(keyword)
        else:
            statements = sqlglot.parse(
                document.text_before_cursor.strip(),
                error_level=sqlglot.ErrorLevel.IGNORE,
            )

            if not statements:
                return []

            statement = statements[-1]

            expressions = list(statement.walk(bfs=False))
            last_expression = expressions[-1]

            yielded = set()

            select = None

            parent = last_expression

            while parent:
                if select is None and isinstance(parent, sqlglot.expressions.Select):
                    select = parent

                parent = parent.parent

            available_tables = None
            if select is not None:
                available_tables = {}
                for table in select.find_all(sqlglot.expressions.Table):
                    schema = None
                    if table.db:
                        schema = table.db.this

                    available_tables.setdefault(schema, {})

                    available_tables[schema][table.this.this] = True

            if isinstance(last_expression, sqlglot.expressions.Table):
                for schema_name in completion_cache.keys():
                    if schema_name is not None:
                        if schema_name not in yielded:
                            yielded.add(schema_name)
                            yield basic_completion(schema_name)

                    for table_name in completion_cache[schema_name].get("tables", {}).keys():
                        if table_name not in yielded:
                            yielded.add(table_name)
                            yield basic_completion(table_name)

            elif isinstance(last_expression, sqlglot.expressions.Where):

                for schema_name in completion_cache.keys():

                    if schema_name not in available_tables and None not in available_tables:
                        continue

                    if schema_name not in available_tables:
                        yielded.add(schema_name)
                        yield basic_completion(schema_name)

                    for table_name in completion_cache[schema_name].get("tables", {}).keys():

                        is_matching_table = False
                        if available_tables.get(schema_name) and table_name in available_tables[schema_name]:
                            is_matching_table = True
                        elif available_tables.get(None) and table_name in available_tables[None]:
                            is_matching_table = True

                        if not is_matching_table:
                            continue

                        yielded.add(table_name)
                        yield basic_completion(table_name)

                        for column_name in completion_cache[schema_name]["tables"][table_name]:
                            if column_name not in yielded:
                                yielded.add(column_name)
                                yield basic_completion(column_name)

                return []

            elif isinstance(last_expression, sqlglot.expressions.Identifier):

                is_column = False

                checked_expressions = [last_expression]
                try:
                    checked_expressions.append(expressions[-2])
                except IndexError:
                    pass

                for checked_expression in checked_expressions:
                    if isinstance(checked_expression, sqlglot.expressions.Column):
                        is_column = True
                    elif isinstance(checked_expression.parent, sqlglot.expressions.Column):
                        is_column = True

                if is_column:
                    for schema_name in completion_cache.keys():

                        if schema_name not in available_tables and None not in available_tables:
                            continue

                        for table_name in completion_cache[schema_name].get("tables", {}).keys():

                            is_matching_table = False
                            if available_tables.get(schema_name) and table_name in available_tables[schema_name]:
                                is_matching_table = True
                            elif available_tables.get(None) and table_name in available_tables[None]:
                                is_matching_table = True

                            if not is_matching_table:
                                continue

                            for column_name in completion_cache[schema_name]["tables"][table_name]:

                                if not column_name.startswith(last_expression.this):
                                    continue

                                if column_name not in yielded:
                                    yielded.add(column_name)
                                    yield Completion(
                                        text=column_name[len(last_expression.this):],
                                        start_position=0,
                                        display=column_name,
                                    )

                    return []

                for schema_name in completion_cache.keys():
                    if schema_name is not None:

                        if schema_name not in yielded:
                            if schema_name.startswith(last_expression.this):
                                yielded.add(schema_name)
                                yield Completion(
                                    text=schema_name[len(last_expression.this):],
                                    start_position=0,
                                    display=schema_name,
                                )

                    for table_name in completion_cache[schema_name].get("tables", {}).keys():

                        if table_name not in yielded:
                            if table_name.startswith(last_expression.this):
                                yielded.add(table_name)
                                yield Completion(
                                    text=table_name[len(last_expression.this):],
                                    start_position=0,
                                    display=table_name,
                                )

                            for column_name in completion_cache[schema_name]["tables"][table_name]:
                                if not column_name.startswith(last_expression.this):
                                    continue

                                if column_name not in yielded:
                                    yielded.add(column_name)
                                    yield Completion(
                                        text=column_name[len(last_expression.this):],
                                        start_position=0,
                                        display=column_name,
                                    )

            return []


completer = DynamicCompleter(get_completer)


def get_complete_style():
    if config.autocomplete == "readline":
        return CompleteStyle.READLINE_LIKE
    elif config.autocomplete == "column":
        return CompleteStyle.COLUMN
    elif config.autocomplete == "multi_column":
        return CompleteStyle.MULTI_COLUMN
    elif config.autocomplete in (None, "auto"):
        if os.environ.get("SHELL") and os.environ["SHELL"].endswith("fish"):
            return CompleteStyle.COLUMN
        elif os.environ.get("FISH_SHELL"):
            return CompleteStyle.COLUMN
        else:
            return CompleteStyle.READLINE_LIKE
    elif getattr(CompleteStyle, config.autocomplete, None):
        return getattr(CompleteStyle, config.autocomplete)
    else:
        return CompleteStyle.READLINE_LIKE
