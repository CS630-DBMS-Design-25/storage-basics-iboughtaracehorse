from lark import Transformer
from sqlast import CreateTable, Insert, Select, SelectAll


class SQLTransformer(Transformer):
    def NAME(self, token):
        return str(token)

    def number(self, token):
        return int(token[0])

    def string(self, token):
        return token[0][1:-1]

    def value_list(self, items):
        return items

    def column_list(self, items):
        return items

    def column_def_list(self, items):
        return items

    def create_stmt(self, items):
        table_name = items[0]
        columns = items[1:]
        return CreateTable(table_name, columns)

    def insert_stmt(self, items):
        table_name = items[0]
        values = items[1]
        return Insert(table_name, values)

    def select_all(self, _):
        return SelectAll()

    def select_stmt(self, items):
        columns = items[1]
        table_name = items[0]
        return Select(table_name, columns)

    def stmt(self, items):
        return items[0]

    def start(self, items):
        return items