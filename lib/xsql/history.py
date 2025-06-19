import itertools
import os

from prompt_toolkit.history import FileHistory as BaseFileHistory

from .config import config


class FileHistory(BaseFileHistory):

    def load_history_strings(self):
        return itertools.islice(super().load_history_strings(), config.history_size)


history = FileHistory(os.path.expanduser("~/.xsql_history"))
