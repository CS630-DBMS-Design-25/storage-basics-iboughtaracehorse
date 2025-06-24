import argparse
import os
import struct
from abc import ABC, abstractmethod
from typing import Callable, Optional, List


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


# Example implementation stub for students to complete
class FileStorageLayer(StorageLayer):
    """Example implementation of the StorageLayer interface.
    Students should fill in the method implementations."""

    def __init__(self):
        self.is_open = False
        #self.file = None
        self.buffer = {}
        self.next_r_id = {}

        # Add any other necessary instance variables here

    def open(self, path: str) -> None:

        """TODO: Implement this method to open storage at the specified path"""

        os.makedirs(path, exist_ok=True)
        self.storage_path = path
        self.is_open = True
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

        path = os.path.join(self.storage_path, table)

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
                    # Implement retrieval logic
        print("Record not found")

    def update(self, table: str, record_id: int, updated_record: bytes) -> None:
        """TODO: Implement this method to update a record"""

        if table not in self.buffer:
            self.buffer[table] = {}

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

                len = struct.unpack(">I", b_len)[0]
                record = input_file.read(len)

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
        """TODO: Implement this method to delete a record"""

        path = os.path.join(self.storage_path, table)
        cur_path = path + ".tmp"
        can_delete = False

        with open(path, "rb") as input_file, open(cur_path, "wb") as output_file:
            while True:

                b_id = input_file.read(4)

                if not b_id:
                    break

                r_id = struct.unpack(">I", b_id)[0]
                b_len = input_file.read(4)

                if not b_len:
                    break

                len = struct.unpack(">I", b_len)[0]
                record = input_file.read(len)

                if r_id == record_id:
                    can_delete = True
                    continue

                output_file.write(b_id)
                output_file.write(b_len)
                output_file.write(record)

        if not can_delete:
            os.remove(cur_path)
            print("Record not found")
        else:
            os.replace(cur_path, path)

    def scan(self, table: str, callback: Optional[Callable[[int, bytes], bool]] = None,
             projection: Optional[List[int]] = None, filter_func: Optional[Callable[[bytes], bool]] = None) -> List[
        bytes]:
        """TODO: Implement this method to scan records in a table"""

        path = os.path.join(self.storage_path, table)
        result = []

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

                if callback:
                    if not callback(r_id, record):
                        break

                if filter_func:
                    if not filter_func(r_id, record):
                        break

                if projection:
                    parts = record.split("\n") #not sure if this is the correct one
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

                for r_id, record in records:
                    file.write(struct.pack(">I", r_id))
                    file.write(struct.pack(">I", len(record)))
                    file.write(record)

        self.buffer = {}
        # Implement flush logic

def main():
    storage = FileStorageLayer()  # Students will implement this class

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

            # Parse the command
            args = parser.parse_args(command_input.split())

            # Execute the appropriate command
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

        except SystemExit:
            # Catch the SystemExit exception that argparse raises
            continue
        except Exception as e:
            print(f"Error: {e}")


if __name__ == '__main__':
    main()