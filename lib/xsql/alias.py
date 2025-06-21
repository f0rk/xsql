import os


def load_aliases():
    alias_path = os.path.expanduser("~/.xsql/aliases")

    aliases = {}

    if os.path.exists(alias_path):
        with open(alias_path, "rt") as fp:
            for line in fp:
                alias, value = line.split(":", maxsplit=1)
                alias = alias.strip()
                value = value.strip()

                aliases[alias] = value

    return aliases
