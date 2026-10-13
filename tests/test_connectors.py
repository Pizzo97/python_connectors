"""Unit and interface test suite covering all 11 Luna connectors with driver mocks."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import pyarrow as pa


# ------------------------------------------------------------------------------
# Mock Cursor Classes (explicit attributes, no MagicMock attribute bleed)
# ------------------------------------------------------------------------------
class MockDbApiCursor:
    """Simulates a standard DB-API 2.0 cursor (Oracle, Postgres, MySQL)."""
    def __init__(self, description, rows):
        self.description = description
        self._rows = list(rows)
        self.executed_query = None
        self.arraysize = 0
        self.prefetchrows = 0

    def execute(self, query):
        self.executed_query = query

    def fetchmany(self, size=50000):
        if not self._rows:
            return []
        chunk = self._rows[:size]
        self._rows = self._rows[size:]
        return chunk

    def close(self):
        pass


class MockTurbodbcCursor:
    """Simulates a Turbodbc C++ cursor (MSSQL, DB2, Teradata)."""
    def __init__(self, batches):
        self._batches = batches
        self.description = [("col", 1)]
        self.executed_query = None

    def execute(self, query):
        self.executed_query = query

    def fetcharrowbatches(self):
        return iter(self._batches)

    def close(self):
        pass


class MockAdbcCursor:
    """Simulates an Apache Arrow Flight SQL ADBC cursor (Dremio)."""
    def __init__(self, batches):
        self._batches = batches
        self.description = [("col", 1)]
        self.executed_query = None

    def execute(self, query):
        self.executed_query = query

    def fetch_record_batch(self):
        return iter(self._batches)

    def close(self):
        pass


class MockConnection:
    """Generic database connection wrapper."""
    def __init__(self, cursor_obj):
        self._cursor = cursor_obj
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class TestAllConnectors(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # --------------------------------------------------------------------------
    # 1. Oracle Connector
    # --------------------------------------------------------------------------
    def test_oracle_connector(self):
        mock_oracledb = MagicMock()
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            from luna.connectors.oracle import connect, oracle_to_parquet, oracle_to_polars

            # Test connect DSN resolution
            connect(user="scott", password="pwd", host="oracle.lan", port=1521, service_name="FREEPDB1")
            mock_oracledb.connect.assert_called_with(
                user="scott", password="pwd", dsn="oracle.lan:1521/FREEPDB1"
            )

            # Test oracle_to_parquet lifecycle and cursor network tuning
            cur = MockDbApiCursor(description=[("emp_id", 1), ("name", 2)], rows=[(101, "Alice"), (102, "Bob")])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "oracle.parquet"
            res = oracle_to_parquet(query="SELECT * FROM emp", target_path=target, connection=conn)
            self.assertEqual(cur.arraysize, 50_000)
            self.assertEqual(cur.prefetchrows, 50_000)
            self.assertEqual(res.row_count, 2)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockDbApiCursor(description=[("emp_id", 1)], rows=[(101,)])
            conn2 = MockConnection(cur2)
            df = oracle_to_polars(query="SELECT emp_id FROM emp", connection=conn2)
            self.assertEqual(df.shape, (1, 1))

            # Positional call must raise TypeError
            with self.assertRaises(TypeError):
                oracle_to_parquet("SELECT * FROM emp", target)

    # --------------------------------------------------------------------------
    # 2. PostgreSQL Connector
    # --------------------------------------------------------------------------
    def test_postgres_connector(self):
        mock_psycopg = MagicMock()
        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            from luna.connectors.postgres import connect, postgres_to_parquet, postgres_to_polars

            connect(host="pghost", port=5432, database="dwh", user="pguser", password="pwd")
            mock_psycopg.connect.assert_called_with(
                host="pghost", port=5432, dbname="dwh", user="pguser", password="pwd"
            )

            cur = MockDbApiCursor(description=[("id", 1)], rows=[(1,), (2,)])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "postgres.parquet"
            res = postgres_to_parquet(query="SELECT id FROM t", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 2)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockDbApiCursor(description=[("id", 1)], rows=[(1,)])
            conn2 = MockConnection(cur2)
            df = postgres_to_polars(query="SELECT id FROM t", connection=conn2)
            self.assertEqual(df.shape, (1, 1))

    # --------------------------------------------------------------------------
    # 3. MySQL Connector
    # --------------------------------------------------------------------------
    def test_mysql_connector(self):
        mock_pymysql = MagicMock()
        with patch.dict("sys.modules", {"pymysql": mock_pymysql}):
            from luna.connectors.mysql import connect, mysql_to_parquet, mysql_to_polars

            connect(host="myhost", port=3306, database="app", user="root", password="pwd")
            mock_pymysql.connect.assert_called_with(
                host="myhost", port=3306, user="root", password="pwd", database="app"
            )

            cur = MockDbApiCursor(description=[("val", 1)], rows=[("xyz",)])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "mysql.parquet"
            res = mysql_to_parquet(query="SELECT val FROM t", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 1)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockDbApiCursor(description=[("val", 1)], rows=[("xyz",)])
            conn2 = MockConnection(cur2)
            df = mysql_to_polars(query="SELECT val FROM t", connection=conn2)
            self.assertEqual(df.shape, (1, 1))

    # --------------------------------------------------------------------------
    # 4. Microsoft SQL Server (MSSQL) Connector
    # --------------------------------------------------------------------------
    def test_mssql_connector(self):
        mock_turbodbc = MagicMock()
        with patch.dict("sys.modules", {"turbodbc": mock_turbodbc}):
            from luna.connectors.mssql import connect, mssql_to_parquet, mssql_to_polars

            connect(server="sqlserver.lan", database="DWH", user="sa", password="pwd")
            call_kwargs = mock_turbodbc.connect.call_args[1]
            conn_str = call_kwargs["connection_string"]
            self.assertIn("DRIVER={ODBC Driver 18 for SQL Server}", conn_str)
            self.assertIn("SERVER=sqlserver.lan", conn_str)
            self.assertIn("DATABASE=DWH", conn_str)
            self.assertIn("UID=sa", conn_str)
            self.assertIn("PWD=pwd", conn_str)
            self.assertIn("TrustServerCertificate=yes", conn_str)

            batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], names=["num"])
            cur = MockTurbodbcCursor([batch])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "mssql.parquet"
            res = mssql_to_parquet(query="SELECT * FROM t", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 3)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockTurbodbcCursor([batch])
            conn2 = MockConnection(cur2)
            df = mssql_to_polars(query="SELECT * FROM t", connection=conn2)
            self.assertEqual(df.shape, (3, 1))

    # --------------------------------------------------------------------------
    # 5. IBM DB2 Connector
    # --------------------------------------------------------------------------
    def test_db2_connector(self):
        mock_turbodbc = MagicMock()
        with patch.dict("sys.modules", {"turbodbc": mock_turbodbc}):
            from luna.connectors.db2 import connect, db2_to_parquet, db2_to_polars

            connect(host="db2.lan", port=50000, database="PROD", user="db2inst", password="pwd")
            call_kwargs = mock_turbodbc.connect.call_args[1]
            conn_str = call_kwargs["connection_string"]
            self.assertIn("DRIVER={IBM DB2 ODBC DRIVER}", conn_str)
            self.assertIn("HOSTNAME=db2.lan", conn_str)
            self.assertIn("PORT=50000", conn_str)
            self.assertIn("DATABASE=PROD", conn_str)
            self.assertIn("UID=db2inst", conn_str)

            batch = pa.RecordBatch.from_arrays([pa.array(["X", "Y"])], names=["code"])
            cur = MockTurbodbcCursor([batch])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "db2.parquet"
            res = db2_to_parquet(query="SELECT * FROM t", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 2)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockTurbodbcCursor([batch])
            conn2 = MockConnection(cur2)
            df = db2_to_polars(query="SELECT * FROM t", connection=conn2)
            self.assertEqual(df.shape, (2, 1))

    # --------------------------------------------------------------------------
    # 6. Teradata Vantage Connector
    # --------------------------------------------------------------------------
    def test_teradata_connector(self):
        mock_turbodbc = MagicMock()
        with patch.dict("sys.modules", {"turbodbc": mock_turbodbc}):
            from luna.connectors.teradata import connect, teradata_to_parquet, teradata_to_polars

            connect(host="vantage.lan", user="dbc", password="pwd", database="analytics", mechanism="TD2")
            call_kwargs = mock_turbodbc.connect.call_args[1]
            conn_str = call_kwargs["connection_string"]
            self.assertIn("DRIVER={Teradata Database ODBC Driver 17.20}", conn_str)
            self.assertIn("DBCNAME=vantage.lan", conn_str)
            self.assertIn("UID=dbc", conn_str)
            self.assertIn("DATABASE=analytics", conn_str)
            self.assertIn("AUTHENTICATION=TD2", conn_str)

            batch = pa.RecordBatch.from_arrays([pa.array([99.9])], names=["price"])
            cur = MockTurbodbcCursor([batch])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "teradata.parquet"
            res = teradata_to_parquet(query="SELECT * FROM t", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 1)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockTurbodbcCursor([batch])
            conn2 = MockConnection(cur2)
            df = teradata_to_polars(query="SELECT * FROM t", connection=conn2)
            self.assertEqual(df.shape, (1, 1))

    # --------------------------------------------------------------------------
    # 7. Dremio Lakehouse Connector
    # --------------------------------------------------------------------------
    def test_dremio_connector(self):
        mock_flightsql = MagicMock()
        mock_adbc = MagicMock()
        mock_adbc.dbapi = mock_flightsql
        with patch.dict("sys.modules", {"adbc_driver_flightsql": mock_adbc, "adbc_driver_flightsql.dbapi": mock_flightsql}):
            from luna.connectors.dremio import connect, dremio_to_parquet, dremio_to_polars

            connect(host="dremio.corp", port=32010, token="secret_token", use_ssl=True)
            mock_flightsql.connect.assert_called_with(
                "grpc+tls://dremio.corp:32010",
                dbapi_kwargs={"adbc.flight.sql.rpc.call_header.authorization": "Bearer secret_token"}
            )

            batch = pa.RecordBatch.from_arrays([pa.array([1000])], names=["id"])
            cur = MockAdbcCursor([batch])
            conn = MockConnection(cur)

            target = Path(self.temp_dir) / "dremio.parquet"
            res = dremio_to_parquet(query="SELECT * FROM space.tbl", target_path=target, connection=conn)
            self.assertEqual(res.row_count, 1)
            self.assertTrue(target.exists())

            # Test in-memory Polars conversion
            cur2 = MockAdbcCursor([batch])
            conn2 = MockConnection(cur2)
            df = dremio_to_polars(query="SELECT * FROM space.tbl", connection=conn2)
            self.assertEqual(df.shape, (1, 1))

    # --------------------------------------------------------------------------
    # 8. SAS Connector (.sas7bdat, .xpt)
    # --------------------------------------------------------------------------
    def test_sas_connector(self):
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["A", "B"])], names=["id", "label"])
        with patch("luna.connectors.sas._stream_sas_batches", return_value=iter([batch])):
            from luna.connectors.sas import sas_to_parquet, sas_to_polars, sas_to_pandas

            target = Path(self.temp_dir) / "sas.parquet"
            res = sas_to_parquet(sas_file="dataset.sas7bdat", target_path=target)
            self.assertEqual(res.row_count, 2)
            self.assertTrue(target.exists())

        with patch("luna.connectors.sas._stream_sas_batches", return_value=iter([batch])):
            df = sas_to_polars(sas_file="dataset.sas7bdat")
            self.assertEqual(df.shape, (2, 2))
            self.assertEqual(df.columns, ["id", "label"])

    # --------------------------------------------------------------------------
    # 9. Google BigQuery Connector
    # --------------------------------------------------------------------------
    def test_bigquery_connector(self):
        mock_bq_mod = MagicMock()
        mock_cloud_mod = MagicMock()
        mock_cloud_mod.bigquery = mock_bq_mod
        mock_google_mod = MagicMock()
        mock_google_mod.cloud = mock_cloud_mod

        with patch.dict("sys.modules", {
            "google": mock_google_mod,
            "google.cloud": mock_cloud_mod,
            "google.cloud.bigquery": mock_bq_mod,
        }):
            from luna.connectors.bigquery import connect, bigquery_to_parquet, bigquery_to_polars

            connect(project="my-gcp-project")
            mock_bq_mod.Client.assert_called_with(project="my-gcp-project", credentials=None)

            # Direct stream extraction
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_client.query.return_value = mock_job
            mock_page = MagicMock()
            mock_page.to_arrow.return_value = pa.Table.from_arrays([pa.array([1, 2])], names=["row_id"])
            mock_iterator = MagicMock()
            mock_iterator.pages = [mock_page]
            mock_job.result.return_value = mock_iterator

            target = Path(self.temp_dir) / "bq.parquet"
            res = bigquery_to_parquet(query="SELECT * FROM dataset.table", target_path=target, client=mock_client)
            self.assertEqual(res.row_count, 2)
            self.assertTrue(target.exists())

            # In-memory Polars conversion
            mock_job.to_arrow.return_value = pa.Table.from_arrays([pa.array([1, 2])], names=["row_id"])
            df = bigquery_to_polars(query="SELECT * FROM dataset.table", client=mock_client)
            self.assertEqual(df.shape, (2, 1))

    # --------------------------------------------------------------------------
    # 10. Google Cloud Storage (GCS) Connector
    # --------------------------------------------------------------------------
    def test_gcs_connector(self):
        from luna.connectors.gcs import gcs_to_parquet

        target = Path(self.temp_dir) / "gcs_out.parquet"

        # Security validations
        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="s3://bucket/data.parquet", target_path=target)

        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="gs://bucket/data'; DROP TABLE t;--", target_path=target)

        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="gs://bucket/data.parquet", target_path=target, service_account_key="key';--")

        with self.assertRaises(ValueError):
            gcs_to_parquet(gcs_uri="gs://bucket/data.parquet", target_path=target, columns=["id", "name;--"])

    # --------------------------------------------------------------------------
    # 11. SQLite Connector (Keyword-only Enforcement)
    # --------------------------------------------------------------------------
    def test_sqlite_connector(self):
        from luna.connectors.sqlite import connect, sqlite_to_parquet, sqlite_to_polars

        conn = connect(":memory:")
        conn.execute("CREATE TABLE items (id INT, item TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'hammer'), (2, 'wrench')")

        target = Path(self.temp_dir) / "items.parquet"
        res = sqlite_to_parquet(query="SELECT * FROM items", target_path=target, connection=conn)
        self.assertEqual(res.row_count, 2)

        df = sqlite_to_polars(query="SELECT * FROM items", connection=conn)
        self.assertEqual(df.shape, (2, 2))

        # Positional call must raise TypeError
        with self.assertRaises(TypeError):
            sqlite_to_parquet("SELECT * FROM items", target, connection=conn)

        conn.close()


if __name__ == "__main__":
    unittest.main()
