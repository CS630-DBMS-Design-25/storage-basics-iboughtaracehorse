import os
import struct


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
            if isinstance(row, bytes):
                parts = row.decode().split("\n")
            else:
                parts = row
            yield [parts[i] for i in self.column_indexes]

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
        #result = []

        for row in input_rows:

            if isinstance(row, bytes):
                parts = row.decode().split("\n")
            else:
                parts = row

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
                yield parts

class LimitOperator:

    def __init__(self, count, child):
        self.count = count
        self.child = child

    def execute(self):
        return list(self.child.execute())[:self.count]

class OrderByOperator:

    def __init__(self, column, direction, child):
        self.column = column
        self.direction = direction.lower() if direction else "asc"
        self.child = child

    def execute(self):
        rows = list(self.child.execute())

        if not hasattr(self.child, "schema") or self.column not in self.child.schema:
            print("Schema or column not found in child operator")
            return rows

        idx = self.child.schema.index(self.column)
        reverse = self.direction == "desc"

        split_rows = [row.decode().split("\n") for row in rows]

        sorted_rows = sorted(split_rows, key=lambda row: row[idx], reverse=reverse)

        for fields in sorted_rows:
            yield "\n".join(fields).encode()

class DeleteOperator(PhysicalOperator):
    def __init__(self, table_name, condition, storage, schema):
        self.table_name = table_name
        self.condition = condition
        self.storage = storage
        self.schema = schema

    def execute(self):
        def condition_fn(record):
            if not self.condition:
                return True
            fields = record.decode().split("\n")
            row = dict(zip(self.schema, fields))
            op = self.condition.op
            col = self.condition.column
            val = self.condition.value
            cell = row.get(col)
            if cell is None:
                return False
            try:
                cell_val = int(cell)
                compare_val = int(val)
            except:
                cell_val = cell
                compare_val = val
            if op == "=":
                return cell_val == compare_val
            elif op == "!=":
                return cell_val != compare_val
            elif op == ">":
                return cell_val > compare_val
            elif op == "<":
                return cell_val < compare_val
            else:
                return False

        buffer = self.storage.buffer.get(self.table_name, {})
        to_delete = [rid for rid, rec in buffer.items() if condition_fn(rec)]
        for record_id in to_delete:
            print(f"Deleting record ID from buffer: {record_id}")
            self.storage.delete(self.table_name, record_id)

        disk_records = []
        path = os.path.join(self.storage.storage_path, self.table_name)
        if os.path.exists(path):
            with open(path, "rb") as f:
                while True:
                    b_id = f.read(4)
                    if not b_id:
                        break
                    record_id = struct.unpack(">I", b_id)[0]
                    b_len = f.read(4)
                    if not b_len:
                        break
                    length = struct.unpack(">I", b_len)[0]
                    record = f.read(length)
                    disk_records.append((record_id, record))

        to_delete_disk = [rid for rid, rec in disk_records if condition_fn(rec)]

        for record_id in to_delete_disk:
            if not (self.table_name in self.storage.buffer and record_id in self.storage.buffer[self.table_name]):
                print(f"Deleting record ID from disk: {record_id}")
                self.storage.delete(self.table_name, record_id)

        return []