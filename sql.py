from lark import Transformer
from sqlast import CreateTable, Insert, Select, Condition

class SQLTransformer(Transformer):
    def NAME(self, token):
        return str(token)

    def number(self, token):
        return int(token[0])

    def string(self, token):
        return token[0][1:-1]

    def value_list(self, items):
        return items

    def column_def_list(self, items):
        return items

    def create_stmt(self, items):
        table_name = items[0]
        columns = items[1]
        return CreateTable(table_name, columns)

    def insert_stmt(self, items):
        table_name = items[0]
        values = items[1]
        return Insert(table_name, values)

    def where_clause(self, items):
        return items[0]

    def condition(self, items):
        column, operation, value = items
        return Condition(column, operation, value)

    def eq(self, _):
        return "="

    def neq(self, _):
        return "!="

    def gt(self, _):
        return ">"

    def lt(self, _):
        return "<"

    def select_all(self, _):
        return ["*"]

    def select_columns(self, items):
        return [str(item) for item in items]

    def select_stmt(self, items):
        columns = items[0]
        table_name = items[1]
        condition = items[2] if len(items) > 2 else None
        return Select(columns, table_name, condition)

    def stmt(self, items):
        return items[0]

    def start(self, items):
        return items
