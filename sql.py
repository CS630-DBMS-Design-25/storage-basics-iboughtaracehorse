from lark import Transformer
from ast import CreateTable, Insert, Select

class SQLTransformer(Transformer):
    def NAME(self, token):
        return str(token)

    def number(self, token):
        return int(token[0])

    def string(self, token):
        return str(token[0])[1:-1]

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
        return ["*"]

    def select_stmt(self, items):
        columns = items[0]
        table_name = items[1]
        return Select(columns, table_name)

    def stmt(self, items):
        return items[0]

    def start(self, items):
        return items