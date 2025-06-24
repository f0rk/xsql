import importlib
import os
import subprocess
import sys

from .config import config


def translate(conn, query, from_=None, to=None):

    translate_dir = os.path.expanduser("~/.xsql/")
    if not os.path.exists(translate_dir):
        return query

    if from_ is None:
        from_ = config.translate_from
    else:
        from_ = conn.dialect.name

    if to is None:
        to = config.translate_to
    else:
        to = conn.dialect.name

    if from_ == to and (from_ != "auto" or to != "auto"):
        return query

    translate_py = os.path.join(translate_dir, "translate.py")
    translate_script = os.path.join(translate_dir, "translate")

    if os.path.exists(translate_py):
        if translate_dir not in sys.path:
            sys.path.append(translate_dir)

        try:
            try:
                sys.path.insert(0, translate_dir)
                mod = importlib.import_module("translate")
            finally:
                sys.path.remove(translate_dir)
        except ImportError:
            sys.stdout.write("xsql: error: unable to import translator, but translation requested\n")
            sys.stdout.flush()
            return None

        return mod.translate(
            from_,
            to,
            conn,
            query,
            config.variables.get("translate_options"),
        )
    elif os.path.exists(translate_script) and os.access(translate_script, os.X_OK):

        translate_args = [
            translate_script,
        ]

        if config.variables.get("translate_options"):
            translate_args.extend(
                [
                    "--options",
                    config.variables.get("translate_options"),
                ],
            )

        translate_args.extend(
            [
                from_,
                to,
            ],
        )

        try:
            translate_process = subprocess.Popen(
                translate_args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
        except OSError as oex:
            sys.stdout.write(
                "xsql: error: error running {}: {} {}\n"
                .format(
                    translate_script,
                    oex.args[0],
                    oex.args[1],
                )
            )
            sys.stdout.flush()
            return None

        query, stderr = translate_process.communicate(query.encode("utf-8"))

        if translate_process.returncode:
            sys.stdout.write(
                "xsql: error: error running {}: {}\n"
                .format(
                    translate_script,
                    translate_process.returncode,
                )
            )

            if stderr:
                stderr = stderr.decode("utf-8")
                sys.stdout.write(stderr)
                if not stderr.endswith("\n"):
                    sys.stdout.write("\n")
                sys.stdout.flush()
                return None

        return query.decode("utf-8")
