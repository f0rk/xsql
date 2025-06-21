xsql
====

A psql-like command line interface to any database.

Usage
=====

```!sh
~$ xsql postgresql://postgres@/db
[db]> select 1
> ;
 ?column? 
----------
        1 
[db]>^D
~$
```

For interactive help and a list of available commands, use \?.

See also `xsql --help`.

Config
======

Config should be placed in `~/.xsqlrc`.

Configure specific commands to execute on startup. `[default]` runs for all
dialects.

```
[default]
\set PROMPT1 '(%n@%M:%> %`date +%H:%M:%S`) [%/]> '
\set PROMPT2 '> '
\pset null '<NÜLLZØR>'
\timing
\syntax on
\color on

[postgresql]
set time zone 'America/New_York';
set search_path = public, tiger;
set application_name = 'xsql - ryan';

[redshift]
set time zone 'America/New_York';
set application_name = 'xsql - ryan';
```

The above config results in:
```!sh
~$ xsql postgresql://postgres@/db
Null display is "<NÜLLZØR>".
Timing is on.
SET
Time: 3.184 ms
SET
Time: 1.703 ms
SET
Time: 1.536 ms
SET
Time: 0.412 ms
(postgres@[local]:5432 06:37:20) [db]> select
> 1;
 ?column? 
----------
        1 
Time: 0.130 ms
(postgres@[local]:5432 06:37:25) [db]>
```

xsql supports named connections, via the `~/.xsql/aliases` file.
Enter each line as `<name>: <url>`, for example:
```
~$ cat ~/.xsql/aliases
local: postgresql://postgres@/db
remote: arn:aws:ssm:us-east-1:666666666666:parameter/db/url
```

You can then run:
```
~$ xsql test
(postgres@[local]:5432 06:37:20) [db]>
```

Note as well support for AWS SSM and Secrets Manager parameters, which will be
resolved automatically. Comes with extra `aws`.

Translation
===========

xsql can translate queries user a user-defined function in `~/.xsql/tranlsate.py`.

For example:
```
~$ cat ~/.xsql/translate.py
import re


def translate(from_, to, conn, query, options):
    if from_ == "redshift" and to in ("postgresql", "snowflake"):
        return re.sub(r"\bsome_custom_func[(]", r"other_custom_func(, query)
    elif from_ == ("postgresql", "snowflake") and to == "redshift":
        return re.sub(r"\bother_custom_func[(]", r"some_custom_func(", query)
    else:
        return query
```

If you operate multiple different databases, this can allow you to run the same
queries with automatic translation. You can add arbitrary comments, hints, use
tools like sqlglot, etc.

Set translation using the `\translate` metacommand, for example:
```
(postgres@[local]:5432 06:37:25) [db]> \translate postgresql snowflake
Translate is from "postgresql to "snowflake".
```

Options is a string set by `\set translate_options format=true`. `options` to
`def translate` will be passed as `"format=true"`.
