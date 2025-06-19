import itertools

from prompt_toolkit.history import FileHistory as BaseFileHistory

from .config import config


class FileHistory(BaseFileHistory):

    def load_history_strings(self):
        return itertools.islice(super().load_history_strings(), config.history_size)
