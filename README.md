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

See also `xsql --help`

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
Time: 0.002 ms
SET
Time: 0.001 ms
SET
Time: 0.000 ms
SET
Time: 0.000 ms
(postgres@[local]:5432 06:37:20) [db]> select
> 1;
 ?column? 
----------
        1 
Time: 0.130 ms
(postgres@[local]:5432 06:37:25) [capitalrx_adjudication]>
```
