class ASTNode: pass

class CreateTable(ASTNode):
    def __init__(self, table_name: str, columns: list[str]):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return f"Created table({self.table_name}, {self.columns})"

class Condition:
    def __init__(self, column: str, op: str, value: str | int):
        self.column = column
        self.op = op
        self.value = value

class Insert(ASTNode):
    def __init__(self, table_name: str, values: list[str]):
        self.table_name = table_name
        self.values = values

class Select(ASTNode):
    def __init__(self, columns, table_name, condition=None, order_by=None, limit=None):
        self.table_name = table_name
        self.columns = columns
        self.condition = condition
        self.order_by = order_by
        self.limit = limit

class Delete:
    def __init__(self, table_name, condition=None):
        self.table_name = table_name
        self.condition = condition


class SelectAll(ASTNode): pass