class ASTNode: pass

class CreateTable(ASTNode):
    def __init__(self, table_name: str, columns: list[str]):
        self.table_name = table_name
        self.columns = columns

class Insert(ASTNode):
    def __init__(self, table_name: str, values: list[str]):
        self.table_name = table_name
        self.values = values

class Select(ASTNode):
    def __init__(self, columns: list[str], table_name: str):
        self.table_name = table_name
        self.columns = columns
