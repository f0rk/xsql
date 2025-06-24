from prompt_toolkit.lexers import PygmentsLexer, SimpleLexer
from pygments.lexers import sql


simple_lexer = SimpleLexer()


class Lexer:

    _selected_lexer = None

    def __init__(self):
        self._selected_lexer = simple_lexer

    def lex_document(self, document):
        return self._selected_lexer.lex_document(document)

    def invalidation_hash(self):
        return id(self._selected_lexer)

    def set_selected_by_name(self, name):
        if name is None:
            self.set_selected_lexer(simple_lexer)
        else:
            lexer_class = sql.SqlLexer
            if name in ("postgresql", "redshift"):
                lexer_class = sql.PostgresLexer
            elif name == "mysql":
                lexer_class = sql.MySqlLexer

            self.set_selected_lexer(PygmentsLexer(lexer_class))

    def set_selected_lexer(self, new_lexer):
        self._selected_lexer = new_lexer


lexer = Lexer()
