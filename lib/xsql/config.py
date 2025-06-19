import os


class Configuration:

    def __init__(
        self,
        autocommit=True,
        null="<NÜLLZØR>",
        pager=None,
        highlight=True,
    ):

        self.autocommit = autocommit
        self.null = null

        if pager is None:
            pager = os.environ.get("PAGER")

        self.pager = pager
        self.highlight = True


config = Configuration()
