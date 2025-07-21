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

if __name__ == '__main__':
    unittest.main()