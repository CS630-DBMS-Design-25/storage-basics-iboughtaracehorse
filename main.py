import argparse
import os
import struct
from abc import ABC, abstractmethod
from typing import Callable, Optional, List
from tabulate import tabulate

from lark import Lark
from sql import SQLTransformer
from sqlast import CreateTable, Insert, Select, Delete

from logical_plan import TableScan, Projection, Selection, Filter, OrderBy, Limit
from physical_plan import SeqScanOperator, ProjectionOperator, FilterOperator, OrderByOperator, LimitOperator, DeleteOperator

def print_table(schema, rows):
    print(tabulate(rows, headers=schema, tablefmt="grid"))

class StorageLayer(ABC):
    """Abstract base class that defines the interface for a simple storage system.
    Students will need to implement a concrete subclass of this interface."""

    @abstractmethod
    def open(self, path: str) -> None:
        """Initialize or open existing storage at the given path."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close storage safely and ensure all data is persisted."""
        pass

    @abstractmethod
    def insert(self, table: str, record: bytes) -> int:
        """Insert a new record into the specified table, returning a unique record ID."""
        pass

    @abstractmethod
    def get(self, table: str, record_id: int) -> bytes:
        """Retrieve a record by its unique ID from the specified table."""
        pass

    @abstractmethod
    def update(self, table: str, record_id: int, updated_record: bytes) -> None:
        """Update an existing record identified by record ID."""
        pass

    @abstractmethod
    def delete(self, table: str, record_id: int) -> None:
        """Delete a record identified by its unique ID."""
        pass

    @abstractmethod
    def scan(self, table: str, callback: Optional[Callable[[int, bytes], bool]] = None,
             projection: Optional[List[int]] = None, filter_func: Optional[Callable[[bytes], bool]] = None) -> List[
        bytes]:
        """Scan records in a table optionally using projection and filter. Callback is optional."""
        pass

    @abstractmethod
    def flush(self) -> None:
        """Persist all buffered data immediately to disk."""
        pass



class FileStorageLayer(StorageLayer):
    """Example implementation of the StorageLayer interface.
    Students should fill in the method implementations."""

    def __init__(self):
        self.is_open = False
        #self.file = None
        self.buffer = {}
        self.next_r_id = {}
        self.storage_path = None
        self.schemas = {}

        # Add any other necessary instance variables here

    def open(self, path: str) -> None:

        """TODO: Implement this method to open storage at the specified path"""

        os.makedirs(path, exist_ok=True)
        self.storage_path = path
        self.is_open = True

        self.load_schemas()

        print("Opened storage at", self.storage_path)
        # Implement storage initialization/opening logic

    def close(self) -> None:
        """TODO: Implement this method to close the storage safely"""

        if self.is_open:

            self.is_open = False
            self.storage_path = None
            print("Closed storage safely")
            # Implement closing logic#

        else:
            print("Already closed storage")
            return

    def schema_path(self, table_name: str) -> str:
        return os.path.join(self.storage_path, f"{table_name}.schema")

    def save_schema(self, table_name: str) -> None:
        path = self.schema_path(table_name)

        with open(path, "w") as f:
            f.write(",".join(self.schemas[table_name]))

    def load_schemas(self) -> None:
        self.schemas = {}

        for filename in os.listdir(self.storage_path):

            if filename.endswith(".schema"):

                table_name = filename[:-7]
                path = self.schema_path(table_name)

                with open(path, "r") as f:
                    columns = f.read().strip().split(",")
                    self.schemas[table_name] = columns

    def insert(self, table: str, record: bytes) -> int:
        """TODO: Implement this method to insert a record and return its ID"""
        # Implement insert logic

        # path = os.path.join(self.storage_path, f"{table}.{record}")
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        # id = 1
        #
        # try:
        #     with open(path, "rb") as file:
        #         while file.read(4):
        #             file.seek(len(record), 1)
        #             id += 1
        # except FileNotFoundError:
        #     pass
        #
        # with open(path, "ab") as file:
        #     file.write(struct.pack("I", id))
        #     file.write(record)
        #
        # return id  # Replace with actual implementation

        if table not in self.buffer:
            self.buffer[table] = {}
            self.next_r_id[table] = 1 #this is so much easier. should have started with implementing flush first and not this

        r_id = self.next_r_id[table]
        self.next_r_id[table] += 1
        self.buffer[table][r_id] = record

        return r_id

    def get(self, table: str, record_id: int) -> bytes:
        """TODO: Implement this method to retrieve a record by ID"""

        if table in self.buffer and record_id in self.buffer[table]:
            return self.buffer[table][record_id]

        path = os.path.join(self.storage_path, table)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Table '{table}' not found on disk. FLUSH FIRST PLEASE")

        with open(path, "rb") as file:
                while True:
                    b_id = file.read(4)
                    if not b_id:
                        break
                    r_id = struct.unpack(">I", b_id)[0]

                    b_len = file.read(4)
                    if not b_len:
                        break
                    len = struct.unpack(">I", b_len)[0]

                    record = file.read(len)

                    if r_id == record_id:
                        return record

        print("Record not found")

    def update(self, table: str, record_id: int, updated_record: bytes) -> None:
        """TODO: Implement this method to update a record"""

        if table in self.buffer and record_id in self.buffer[table]:
            self.buffer[table][record_id] = updated_record
            return

        path = os.path.join(self.storage_path, table)
        cur_path = path + ".tmp"
        can_update = False

        with open(path, "rb") as input_file, open(cur_path, "wb") as output_file:
            while True:
                b_id = input_file.read(4)
                if not b_id:
                    break

                r_id = struct.unpack(">I", b_id)[0]
                b_len = input_file.read(4)

                if not b_len:
                    break

                length = struct.unpack(">I", b_len)[0]
                record = input_file.read(length)

                if r_id == record_id:
                    can_update = True
                    output_file.write(struct.pack(">I", r_id))
                    output_file.write(struct.pack(">I", len(updated_record)))
                    output_file.write(updated_record)

                else:
                    output_file.write(b_id)
                    output_file.write(b_len)
                    output_file.write(record)

        if not can_update:
            os.remove(cur_path)
            print("Record cannot be updatesd")

        else:
            os.replace(cur_path, path)


        # Implement update logic

    def delete(self, table: str, record_id: int) -> None:

        if table in self.buffer and record_id in self.buffer[table]:
            del self.buffer[table][record_id]

        path = os.path.join(self.storage_path, table)
        tmp_path = path + ".tmp"
        found = False

        if not os.path.exists(path):
            open(path, "ab").close()

        if os.path.exists(path):
            with open(path, "rb") as f_in, open(tmp_path, "wb") as f_out:
                while True:
                    b_id = f_in.read(4)
                    if not b_id:
                        break
                    r_id = struct.unpack(">I", b_id)[0]
                    b_len = f_in.read(4)
                    if not b_len:
                        break
                    length = struct.unpack(">I", b_len)[0]
                    record = f_in.read(length)

                    if r_id == record_id:
                        found = True
                        continue

                    f_out.write(b_id)
                    f_out.write(b_len)
                    f_out.write(record)

            if found:
                os.replace(tmp_path, path)
            else:
                os.remove(tmp_path)
                print("Record not found")

    def scan(self, table: str, callback: Optional[Callable[[int, bytes], bool]] = None,
             projection: Optional[List[int]] = None, filter_func: Optional[Callable[[bytes], bool]] = None) -> List[
        bytes]:
        """TODO: Implement this method to scan records in a table"""

        path = os.path.join(self.storage_path, table)
        result = []
        seen_ids = set()

        if table in self.buffer:
            for r_id, record in self.buffer[table].items():
                if filter_func and not filter_func(record):
                    continue
                if callback and not callback(r_id, record):
                    return result
                if projection:
                    parts = record.decode().split("\n")
                    projected = [parts[i] for i in projection if i < len(parts)]
                    result.append(" | ".join(projected).encode())
                else:
                    result.append(record)
                seen_ids.add(r_id)

        with open(path, "rb") as file:
            while True:
                b_id = file.read(4)

                if not b_id:
                    break

                r_id = struct.unpack(">I", b_id)[0]
                b_len = file.read(4)

                if not b_len:
                    break

                length = struct.unpack(">I", b_len)[0]
                record = file.read(length)

                if callback:
                    if not callback(r_id, record):
                        break

                if r_id in seen_ids:
                    continue

                if filter_func:
                    if not filter_func(record):
                        break

                if projection:
                    parts = record.decode().split("\n") #not sure if this is the correct one
                    projected = []

                    for i in projection:
                        if i < len(parts):
                            projected.append(parts[i])
                    new_record = "".join(projected)
                    result.append(new_record)

                else:
                    result.append(record)

        # Implement scan logic
        return result

    def flush(self) -> None:
        """TODO: Implement this method to flush data to disk"""

        for table, records in self.buffer.items():
            peth = os.path.join(self.storage_path, table)

            with open(peth, "wb") as file:

                for r_id, record in records.items():
                    file.write(struct.pack(">I", r_id))
                    file.write(struct.pack(">I", len(record)))
                    file.write(record)

        self.buffer = {}
        # Implement flush logic

def convert_ast_to_logical(ast_node, storage=None, schema=None):

    if isinstance(ast_node, Select):
        plan = TableScan(ast_node.table_name)

        if ast_node.condition:
            plan = Filter(plan, ast_node.condition)

        if ast_node.order_by:
            column, direction = ast_node.order_by
            plan = OrderBy(column, direction, plan)

        if ast_node.limit is not None:
            plan = LimitOperator(ast_node.limit, plan)

        plan = Projection(ast_node.columns, plan)

        return plan

    elif isinstance(ast_node, Delete):
        return Delete(ast_node.table_name, ast_node.condition)

def convert_logical_to_physical(plan, storage, schema):

    if isinstance(plan, Projection):
        child = convert_logical_to_physical(plan.child, storage, schema)
        return ProjectionOperator(plan.columns, child, schema)

    elif isinstance(plan, TableScan):
        return SeqScanOperator(plan.table_name, storage)

    elif isinstance(plan, Filter):
        child = convert_logical_to_physical(plan.child, storage, schema)
        return FilterOperator(plan.condition, child, schema)

    elif isinstance(plan, OrderBy):
        child = convert_logical_to_physical(plan.child, storage, schema)
        return OrderByOperator(plan.column, plan.direction, child)

    elif isinstance(plan, LimitOperator):
        child = convert_logical_to_physical(plan.child, storage, schema)
        return LimitOperator(plan.count, child)

    elif isinstance(plan, Delete):
        return DeleteOperator(plan.table_name, plan.condition, storage, schema)

    else:
        print("Unknown plan node:", type(plan))
        return None

def execute_sql(stmt, storage):
    if isinstance(stmt, CreateTable):

        if len(stmt.columns) != len(set(stmt.columns)):
            print(f"Error: Duplicate columns in CREATE TABLE {stmt.table_name}")
            return

        print(f"Creating table '{stmt.table_name}' with columns: {stmt.columns}")

        storage.schemas[stmt.table_name] = stmt.columns
        storage.buffer[stmt.table_name] = {}
        storage.next_r_id[stmt.table_name] = 1
        storage.save_schema(stmt.table_name)

    elif isinstance(stmt, Insert):
        if stmt.table_name not in storage.schemas:
            print(f"Error: Table '{stmt.table_name}' does not exist")
            return

        schema = storage.schemas[stmt.table_name]

        if len(stmt.values) != len(schema):
            print(f"Error: Insert column count does not match schema for table '{stmt.table_name}'")
            return

        values_joined = "\n".join(map(str, stmt.values)).encode()
        r_id = storage.insert(stmt.table_name, values_joined)
        print(f"Inserted into {stmt.table_name} with ID {r_id}")


    elif isinstance(stmt, Select):

        #print(f"DEBUG: columns = {stmt.columns}, type = {type(stmt.columns)}")

        #print(f"DEBUG: stmt.table_name = {stmt.table_name}, type = {type(stmt.table_name)}")
        #print(f"DEBUG: stmt.columns = {stmt.columns}, type = {type(stmt.columns)}")

        schema = storage.schemas.get(stmt.table_name)

        if schema is None:
            print(f"Schema not found for table {stmt.table_name}")

            return

        logical_plan = convert_ast_to_logical(stmt, storage, schema)
        physical_plan = convert_logical_to_physical(logical_plan, storage, schema)

        if physical_plan is None:
            print("Error: Failed to build physical plan")
            return

        rows = list(physical_plan.execute())

        decoded_rows = []
        for row in rows:
            decoded_row = []
            for field in row:
                if isinstance(field, bytes):
                    decoded_row.extend(field.decode().split("\n"))
                else:
                    decoded_row.append(str(field))
            decoded_rows.append(decoded_row)

        if stmt.columns == ['*']:
            display_schema = schema
        else:
            display_schema = stmt.columns

        print_table(schema, decoded_rows)


    elif isinstance(stmt, Delete):

        schema = storage.schemas.get(stmt.table_name)

        if schema is None:
            print(f"Error: Table '{stmt.table_name}' not found")

            return

        logical_plan = convert_ast_to_logical(stmt, storage, schema)

        physical_plan = convert_logical_to_physical(logical_plan, storage, schema)

        if physical_plan is None:
            print("Error: Failed to build physical plan for DELETE")

            return

        physical_plan.execute()

        print("Delete executed")

    else:
        print(f"Unknown statement type: {type(stmt)}")

def main():

    cli_parser = argparse.ArgumentParser(description="Storage Layer CLI")
    cli_parser.add_argument("--query", help="SQL query to execute")
    cli_parser.add_argument("--storage-path", required=True, help="Path to storage directory")

    args = cli_parser.parse_args()

    storage = FileStorageLayer()
    storage.open(args.storage_path)

    with open("sql.lark") as f:
        grammar = f.read()

    sql_parser = Lark(grammar, parser="lalr", transformer=SQLTransformer())

    if args.query:
        try:
            stmts = sql_parser.parse(args.query)
            for stmt in stmts:
                execute_sql(stmt, storage)
        except Exception as e:
            print("SQL Error:", e)
        return

    parser = argparse.ArgumentParser(description="CLI for StorageLayer Testing")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    open_parser = subparsers.add_parser("open", help="Open storage at specified path")
    open_parser.add_argument("path", help="Path to storage location")

    subparsers.add_parser("close", help="Close the storage")
    subparsers.add_parser("flush", help="Flush data to disk")

    insert_parser = subparsers.add_parser("insert", help="Insert a record")
    insert_parser.add_argument("table", help="Table name")
    insert_parser.add_argument("record", help="Record data as string (should be encoded to bytes)")

    get_parser = subparsers.add_parser("get", help="Get a record by ID")
    get_parser.add_argument("table", help="Table name")
    get_parser.add_argument("record_id", type=int, help="Record ID")

    update_parser = subparsers.add_parser("update", help="Update a record")
    update_parser.add_argument("table", help="Table name")
    update_parser.add_argument("record_id", type=int, help="Record ID")
    update_parser.add_argument("record", help="Updated record data")

    delete_parser = subparsers.add_parser("delete", help="Delete a record")
    delete_parser.add_argument("table", help="Table name")
    delete_parser.add_argument("record_id", type=int, help="Record ID")

    scan_parser = subparsers.add_parser("scan", help="Scan records in a table")
    scan_parser.add_argument("table", help="Table name")
    scan_parser.add_argument("--projection", type=int, nargs="*", help="Fields to project")

    print("Storage Layer CLI - Type 'help' for available commands or 'exit' to quit")

    while True:
        try:
            command_input = input("storage-cli> ").strip()
            if not command_input:
                continue
            if command_input in ['exit', 'quit']:
                break
            if command_input == 'help':
                parser.print_help()
                continue

            if command_input.lower().startswith("sql "):
                sql_command = command_input[4:]
                try:
                    stmts = sql_parser.parse(sql_command)
                    for stmt in stmts:
                        execute_sql(stmt, storage)
                except Exception as e:
                    print("SQL Error:", e)
                continue

            try:
                args = parser.parse_args(command_input.split())
            except SystemExit:
                continue

            if args.command == 'open':
                storage.open(args.path)
                print(f"Storage opened at {args.path}")
            elif args.command == 'close':
                storage.close()
                print("Storage closed")
            elif args.command == 'insert':
                record_id = storage.insert(args.table, args.record.encode())
                print(f'Record inserted with ID {record_id}')
            elif args.command == 'get':
                record = storage.get(args.table, args.record_id)
                print(f'Retrieved record: {record.decode()}')
            elif args.command == 'update':
                storage.update(args.table, args.record_id, args.record.encode())
                print('Record updated')
            elif args.command == 'delete':
                storage.delete(args.table, args.record_id)
                print('Record deleted')
            elif args.command == 'scan':
                records = storage.scan(args.table, projection=args.projection)
                if records:
                    print("Scan results:")
                    for i, rec in enumerate(records):
                        print(f"{i}: {rec.decode()}")
                else:
                    print("No records found")
            elif args.command == 'flush':
                storage.flush()
                print('Storage flushed')
            else:
                print(f"Unknown command: {args.command}")


        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == '__main__':
    main()