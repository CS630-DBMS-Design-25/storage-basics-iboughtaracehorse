# FileStorageLayer System

This module defines an abstract `StorageLayer` interface and a concrete implementation `FileStorageLayer` that supports:

- Persistent storage via flat binary files
- Basic CRUD operations: insert, get, update, delete
- In-memory buffering (flush required to persist)
- Scanning with support for filtering, projection, and callbacks
- Command-line interface for manual testing

### File Format
Each record is stored as:

```bash
[4-byte record ID][4-byte record length][record content...]
```

### CLI Commands:
- `open <path>`: Open or create a new storage
- `close`: Safely close storage
- `insert <table> <record>`: Add a new record
- `get <table> <record_id>`: Retrieve a record
- `update <table> <record_id> <record>`: Update a record
- `delete <table> <record_id>`: Delete a record
- `scan <table> [--projection 0 1 ...]`: Scan all records
- `flush`: Persist buffered records to disk

## Test Usage

The test suite is provided in `test_storage_layer.py` and covers the most important functionality.

### Requirements
- Python 3.7+
- No third-party libraries required

### Running the tests

To run all tests using `unittest`:

```bash
python test_storage_layer.py
```
Or, using python -m unittest:

```bash
python -m unittest test_storage_layer.py
```

### What is tested:

-- insert() and get() correctness

-- update() behavior and persistence

-- delete() logic and result

-- scan() full table

-- scan() with filters

-- flush() clears in-memory buffer