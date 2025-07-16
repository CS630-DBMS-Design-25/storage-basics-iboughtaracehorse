class PhysicalOperator:
    def execute(self):
        raise NotImplementedError

class SeqScanOperator(PhysicalOperator):
    def __init__(self, table_name, storage):
        self.table_name = table_name
        self.storage = storage
        self.schema = storage.schemas[table_name]

    def execute(self):
        return self.storage.scan(self.table_name)

class ProjectionOperator(PhysicalOperator):
    def __init__(self, columns, child, schema):
        self.child = child
        self.schema = schema
        if columns == ["*"]:
            self.column_indexes = list(range(len(schema)))
        else:
            self.column_indexes = [schema.index(c) for c in columns]

    def execute(self):
        for row in self.child.execute():
            yield [row[i] for i in self.column_indexes]

class FilterOperator(PhysicalOperator):

    def __init__(self, condition, child_op, schema):
        self.condition = condition
        self.child_op = child_op
        self.schema = schema

    def execute(self):

        input_rows = self.child_op.execute()
        index = self.schema.index(self.condition.column)
        op = self.condition.op
        val = self.condition.value
        result = []

        for row in input_rows:

            parts = row.decode().split("\n")

            if index >= len(parts):
                continue
            cell = parts[index]

            try:
                cell_val = int(cell)
                compare_val = int(val)

            except:
                cell_val = cell
                compare_val = val

            if eval(f"{repr(cell_val)} {op} {repr(compare_val)}"):
                result.append(row)

        return result

class LimitOperator:

    def __init__(self, count, child):
        self.count = count
        self.child = child

    def execute(self):
        return list(self.child.execute())[:self.count]

class OrderByOperator:

    def __init__(self, column, direction, child):
        self.column = column
        self.direction = direction
        self.child = child

    def execute(self):
        rows = list(self.child.execute())
        idx = self.child.schema.index(self.column)
        reverse = self.direction == "desc"
        return sorted(rows, key=lambda row: row[idx], reverse=reverse)