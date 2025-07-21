import os
import shutil
import unittest
from main import FileStorageLayer

class TestFileStorageLayer(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_storage"
        self.table = "test_table"
        self.storage = FileStorageLayer()
        self.storage.open(self.test_dir)

    def tearDown(self):
        self.storage.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_insert_and_get(self):
        record = b"record1"
        record_id = self.storage.insert(self.table, record)
        self.storage.flush()
        result = self.storage.get(self.table, record_id)
        self.assertEqual(result, record)

    def test_update(self):
        original = b"original"
        updated = b"updated"
        record_id = self.storage.insert(self.table, original)
        self.storage.flush()
        self.storage.update(self.table, record_id, updated)
        result = self.storage.get(self.table, record_id)
        self.assertEqual(result, updated)

    def test_delete(self):
        record_id = self.storage.insert(self.table, b"todelete")
        self.storage.flush()
        self.storage.delete(self.table, record_id)
        result = self.storage.get(self.table, record_id)
        self.assertIsNone(result)

    def test_scan_all(self):
        records = [b"a", b"b", b"c"]
        for r in records:
            self.storage.insert(self.table, r)
        self.storage.flush()
        scanned = self.storage.scan(self.table)
        self.assertEqual(len(scanned), len(records))

    def test_scan_with_filter(self):
        self.storage.insert(self.table, b"apple")
        self.storage.insert(self.table, b"banana")
        self.storage.insert(self.table, b"apricot")
        self.storage.flush()
        def starts_with_a(rec):
            return rec.startswith(b"a")
        result = self.storage.scan(self.table, filter_func=starts_with_a)
        self.assertTrue(all(r.startswith(b"a") for r in result))

    def test_flush_clears_buffer(self):
        self.storage.insert(self.table, b"buffered")
        self.assertTrue(self.table in self.storage.buffer)
        self.storage.flush()
        self.assertFalse(self.storage.buffer)

    def test_scan_with_projection(self):
        self.storage.insert(self.table, b"first_record")
        self.storage.insert(self.table, b"second_record")
        self.storage.flush()
        result = self.storage.scan(self.table, projection=[0])
        self.assertEqual(len(result), 2)
        for record in result:
            self.assertIsInstance(record, bytes)

    def test_scan_with_custom_callback(self):
        records = [b"x", b"y", b"z"]
        for r in records:
            self.storage.insert(self.table, r)
        self.storage.flush()

        collected = []

        def callback(record_id, record):
            collected.append((record_id, record))
            return True

        self.storage.scan(self.table, callback=callback)

        self.assertEqual(len(collected), len(records))
        for _, r in collected:
            self.assertIn(r, records)

    def test_scan_filter_and_projection_combined(self):
        self.storage.insert(self.table, b"zebra")
        self.storage.insert(self.table, b"zoo")
        self.storage.insert(self.table, b"apple")
        self.storage.flush()

        def starts_with_z(r): return r.startswith(b"z")
        result = self.storage.scan(self.table, filter_func=starts_with_z, projection=[0])
        self.assertEqual(len(result), 2)
        self.assertTrue(all(r.startswith("z") for r in result))

    def test_delete_from_buffer(self):
        record = b"in-buffer"
        record_id = self.storage.insert(self.table, record)
        self.storage.delete(self.table, record_id)
        result = self.storage.get(self.table, record_id)
        self.assertIsNone(result)

    def test_delete_from_disk(self):
        record = b"on-disk"
        record_id = self.storage.insert(self.table, record)
        self.storage.flush()
        self.storage.delete(self.table, record_id)
        result = self.storage.get(self.table, record_id)
        self.assertIsNone(result)

    def test_deleted_record_not_in_scan(self):
        id1 = self.storage.insert(self.table, b"keep")
        id2 = self.storage.insert(self.table, b"delete_me")
        self.storage.flush()
        self.storage.delete(self.table, id2)
        scanned = self.storage.scan(self.table)
        self.assertIn(b"keep", scanned)
        self.assertNotIn(b"delete_me", scanned)

if __name__ == '__main__':
    unittest.main()