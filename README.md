
# FileStorageLayer

A simple file-based storage layer that supports basic operations like `insert`, `get`, `update`, `delete`, `flush`, and `scan`. The storage engine is built to mimic a simple table-based database where records are stored in binary format.

## Features

- Create and open a file-based storage directory
- Insert binary records into a named table
- Retrieve records by ID
- Update and delete records
- Flush buffered records to disk
- Scan all or filtered records with optional projection or callback

## Usage

```python
from main import FileStorageLayer

storage = FileStorageLayer()
storage.open("storage_dir")

# Insert record
record_id = storage.insert("products", b"Gadget")

# Retrieve record
record = storage.get("products", record_id)

# Update record
storage.update("products", record_id, b"Updated Gadget")

# Delete record
storage.delete("products", record_id)

# Scan with filter
def filter_func(data):
    return data.startswith(b"G")

results = storage.scan("products", filter_func=filter_func)

storage.flush()
storage.close()
````

## Scan Function

`scan(table, filter_func=None, projection_func=None, callback=None)`

* `filter_func(record) -> bool`: Only return records where this returns True.
* `projection_func(record) -> any`: Transform the record (e.g., extract a field).
* `callback(record_id, record) -> bool`: Called for each matching record. Stop if returns False.

Examples:

```python
storage.scan("products")


storage.scan("products", filter_func=lambda r: r.startswith(b"A"))


storage.scan("products", projection_func=lambda r: r.upper())


def my_callback(r_id, r):
    print(f"{r_id}: {r}")
    return True

storage.scan("products", callback=my_callback)
```

## Testing

Run unit tests using:

```bash
python -m unittest test_storage_layer.py
```

The test suite includes:

* `test_insert_and_get`: Ensures records can be inserted and retrieved.
* `test_update`: Verifies updating existing records.
* `test_delete`: Tests record deletion and its effect.
* `test_scan_all`: Checks that all records are returned by scan.
* `test_scan_with_filter`: Validates filtering logic.
* `test_scan_with_projection`: Ensures projection works as expected.
* `test_scan_with_custom_callback`: Verifies callback behavior.
* `test_scan_filter_and_projection_combined`: Combines all scan options.
* `test_flush_clears_buffer`: Ensures flushing clears the buffer correctly.