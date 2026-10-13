"""Comprehensive test suite for Luna core streaming engine and connectors."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import pyarrow as pa

from luna.connectors.sqlite import connect, sqlite_to_parquet, sqlite_to_polars, sqlite_to_pandas
from luna.connectors.core.engine import (
    _accumulate_to_row_groups,
    cursor_to_polars,
    stream_cursor_to_batches,
    stream_cursor_to_parquet,
)
from luna.connectors.bigquery import bigquery_to_parquet
from luna.connectors.gcs import gcs_to_parquet


class TestLunaEngine(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.conn = connect(":memory:")
        self.conn.execute("CREATE TABLE users (id INT, name TEXT, salary REAL)")
        self.conn.execute("INSERT INTO users VALUES (1, 'Alice', 1000.5), (2, 'Bob', 2500.0), (3, 'Charlie', 3000.75)")

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sqlite_streaming_parquet(self):
        target = Path(self.temp_dir) / "users.parquet"
        res = sqlite_to_parquet(query="SELECT * FROM users", target_path=target, connection=self.conn)
        self.assertEqual(res.row_count, 3)
        self.assertEqual(res.batch_count, 1)
        self.assertTrue(target.exists())
        self.assertGreater(target.stat().st_size, 0)

    def test_sqlite_to_polars(self):
        df = sqlite_to_polars(query="SELECT * FROM users", connection=self.conn)
        self.assertEqual(df.shape, (3, 3))
        self.assertEqual(df.columns, ["id", "name", "salary"])

    def test_sqlite_to_pandas(self):
        try:
            import pandas
        except ImportError:
            self.skipTest("Pandas not installed")
        df = sqlite_to_pandas(query="SELECT * FROM users", connection=self.conn)
        self.assertEqual(df.shape, (3, 3))

    def test_custom_batch_size(self):
        # 1. Verify stream_cursor_to_batches produces 1-row batches when batch_size=1
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM users")
        batches = list(stream_cursor_to_batches(cur, batch_size=1))
        self.assertEqual(len(batches), 3)
        self.assertEqual([b.num_rows for b in batches], [1, 1, 1])
        cur.close()

        # 2. Verify sqlite_to_parquet streams correctly with batch_size=1
        target = Path(self.temp_dir) / "users_batch1.parquet"
        res = sqlite_to_parquet(
            query="SELECT * FROM users",
            target_path=target,
            connection=self.conn,
            batch_size=1,
        )
        self.assertEqual(res.row_count, 3)
        self.assertTrue(target.exists())

        # 3. Verify polars with batch_size=2
        df_pl = sqlite_to_polars(query="SELECT * FROM users", connection=self.conn, batch_size=2)
        self.assertEqual(df_pl.shape, (3, 3))

        # 4. Verify pandas with batch_size=2
        try:
            import pandas
            df_pd = sqlite_to_pandas(query="SELECT * FROM users", connection=self.conn, batch_size=2)
            self.assertEqual(df_pd.shape, (3, 3))
        except ImportError:
            pass

    def test_empty_query_allow_empty_true(self):
        target = Path(self.temp_dir) / "empty.parquet"
        res = sqlite_to_parquet(query="SELECT * FROM users WHERE 1=0", target_path=target, connection=self.conn, allow_empty=True)
        self.assertEqual(res.row_count, 0)
        self.assertTrue(target.exists())

    def test_empty_query_allow_empty_false(self):
        target = Path(self.temp_dir) / "empty.parquet"
        with self.assertRaises(ValueError):
            sqlite_to_parquet(query="SELECT * FROM users WHERE 1=0", target_path=target, connection=self.conn, allow_empty=False)
        self.assertFalse(target.exists())

    def test_turbodbc_cursor_dispatch(self):
        class MockTurbodbcCursor:
            def fetcharrowbatches(self):
                return [pa.RecordBatch.from_arrays([pa.array(["  hello   ", "  world "])], names=["x"])]

        df = cursor_to_polars(MockTurbodbcCursor(), strip_strings=True)
        self.assertEqual(df.shape, (2, 1))
        self.assertEqual(df["x"].to_list(), ["hello", "world"])

    def test_adbc_cursor_dispatch(self):
        class MockAdbcCursor:
            def fetch_record_batch(self):
                return [pa.RecordBatch.from_arrays([pa.array(["a", "b"])], names=["y"])]

        df = cursor_to_polars(MockAdbcCursor())
        self.assertEqual(df.shape, (2, 1))
        self.assertEqual(df.columns, ["y"])

    def test_bigquery_uri_validation(self):
        class MockBQClient:
            pass

        with self.assertRaises(ValueError):
            bigquery_to_parquet(query="SELECT 1", client=MockBQClient(), gcs_export_uri="s3://bucket/data.parquet")

        with self.assertRaises(ValueError):
            bigquery_to_parquet(query="SELECT 1", client=MockBQClient(), gcs_export_uri="gs://bucket/data'; DROP TABLE t;--")

    def test_gcs_uri_validation(self):
        target = Path(self.temp_dir) / "out.parquet"
        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="s3://bucket/data.parquet", target_path=target)

        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="gs://bucket/data.parquet", target_path=target, columns=["id", "name; DROP TABLE t;"])

    def test_query_params(self):
        target = Path(self.temp_dir) / "params.parquet"
        res = sqlite_to_parquet(
            query="SELECT * FROM users WHERE salary > ?",
            target_path=target,
            connection=self.conn,
            params=(1500,),
        )
        self.assertEqual(res.row_count, 2)

        df = sqlite_to_polars(
            query="SELECT name FROM users WHERE id = ?",
            connection=self.conn,
            params=(1,),
        )
        self.assertEqual(df.shape, (1, 1))
        self.assertEqual(df["name"][0], "Alice")

    def test_cleanup_on_error(self):
        target = Path(self.temp_dir) / "corrupt.parquet"

        def _faulty_batches():
            yield pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["id"])
            raise RuntimeError("Network connection dropped abruptly")

        from luna.connectors.core.engine import stream_batches_to_parquet

        with self.assertRaises(RuntimeError):
            stream_batches_to_parquet(batch_source=_faulty_batches(), target_path=target)

        self.assertFalse(target.exists(), "Corrupted partial file must be unlinked on error")

    def test_log_progress(self):
        target = Path(self.temp_dir) / "logged.parquet"
        res = sqlite_to_parquet(
            query="SELECT * FROM users",
            target_path=target,
            connection=self.conn,
            log_progress=True,
        )
        self.assertEqual(res.row_count, 3)
        self.assertTrue(target.exists())

    def test_auto_connect_lifecycle(self):
        db_path = Path(self.temp_dir) / "auto.db"
        init_conn = connect(db_path)
        try:
            init_conn.execute("CREATE TABLE t (x INT)")
            init_conn.execute("INSERT INTO t VALUES (10), (20)")
            init_conn.commit()
        finally:
            init_conn.close()

        target = Path(self.temp_dir) / "auto.parquet"
        # Connection omitted: run_cursor must auto-connect and auto-close
        res = sqlite_to_parquet(query="SELECT * FROM t", target_path=target, database=db_path)
        self.assertEqual(res.row_count, 2)
        self.assertTrue(target.exists())

        # Missing both connection and database must raise ValueError
        with self.assertRaises(ValueError):
            sqlite_to_parquet(query="SELECT 1", target_path=target)

    def test_strip_strings(self):
        class MockCharCursor:
            def __init__(self):
                self.description = [("code", 1), ("val", 2), ("opt", 3)]
            def fetchmany(self, size=50000):
                if hasattr(self, "_done"):
                    return []
                self._done = True
                return [("  ABC   ", 42, None), ("   XYZ ", 99, "  hello  ")]

        from luna.connectors.core.engine import cursor_to_batches

        # strip_strings=True trims both leading and trailing whitespace on string columns
        batches = list(cursor_to_batches(MockCharCursor(), strip_strings=True))
        self.assertEqual(len(batches), 1)
        batch = batches[0]
        self.assertEqual(batch.column("code").to_pylist(), ["ABC", "XYZ"])
        self.assertEqual(batch.column("val").to_pylist(), [42, 99])
        self.assertEqual(batch.column("opt").to_pylist(), [None, "hello"])

        # strip_strings=False preserves original leading and trailing spaces
        batches_raw = list(cursor_to_batches(MockCharCursor(), strip_strings=False))
        self.assertEqual(batches_raw[0].column("code").to_pylist(), ["  ABC   ", "   XYZ "])
        self.assertEqual(batches_raw[0].column("opt").to_pylist(), [None, "  hello  "])

    def test_special_unicode_whitespace(self):
        from luna.connectors.core.engine import strip_record_batch

        # \u00A0 (NBSP), tabs and newlines are trimmed
        # \u200B (Zero-width space) and \uFEFF (BOM) are format characters (White_Space=No) and preserved
        raw_values = [
            "\u00A0hello\u00A0",
            " \u00A0mixed\u00A0 ",
            "\t\nspaced\r\n",
            "\u200Bzero_width\u200B",
            "\uFEFFbom\uFEFF",
        ]
        batch = pa.RecordBatch.from_arrays([pa.array(raw_values)], names=["txt"])
        stripped = strip_record_batch(batch)
        expected = [
            "hello",
            "mixed",
            "spaced",
            "\u200Bzero_width\u200B",
            "\uFEFFbom\uFEFF",
        ]
        self.assertEqual(stripped.column("txt").to_pylist(), expected)

    def test_accumulate_to_row_groups_multi_batch(self):
        b1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["id"])
        b2 = pa.RecordBatch.from_arrays([pa.array([3, 4])], names=["id"])
        b3 = pa.RecordBatch.from_arrays([pa.array([5])], names=["id"])
        empty = pa.RecordBatch.from_arrays([pa.array([], type=pa.int64())], names=["id"])

        # max_rows=3: b1(2)+b2(2)=4 rows (>=3) -> yields Table 1 (4 rows); b3(1) -> yields Table 2 (1 row)
        tables = list(_accumulate_to_row_groups([empty, b1, b2, b3], max_rows=3))
        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0].num_rows, 4)
        self.assertEqual(tables[1].num_rows, 1)
        self.assertEqual(sum(t.num_rows for t in tables), 5)


if __name__ == "__main__":
    unittest.main()
