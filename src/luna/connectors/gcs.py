"""Google Cloud Storage (GCS) connector for Luna.

Streams Parquet files directly from GCS buckets (gs://...) into local Parquet files
out-of-core via DuckDB httpfs engine.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Sequence, Union

import duckdb

from luna.connectors.core.models import ExtractionResult


def gcs_to_parquet(
    *,
    gcs_uri: str,
    target_path: Union[str, Path],
    columns: Sequence[str] | None = None,
    service_account_key: str | None = None,
    allow_empty: bool = True,
    log_progress: bool = False,
) -> ExtractionResult:
    """Download and stream Parquet files directly from GCS into a local Parquet file out-of-core.

    Args:
        gcs_uri: GCS path (e.g. 'gs://my-bucket/data/*.parquet').
        target_path: Local destination file path.
        columns: Optional column projection pushdown.
        service_account_key: Path to optional Google Service Account JSON key.
        allow_empty: If False, raises ValueError when 0 rows are extracted.
        log_progress: If True, logs completion status.

    Returns:
        ExtractionResult summarizing rows, size, duration, and target path.
    """
    start_time = time.perf_counter()
    target = Path(target_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    if not gcs_uri.startswith("gs://") or "'" in gcs_uri or "\n" in gcs_uri:
        raise ValueError(f"Invalid GCS URI: {gcs_uri!r}. Must start with 'gs://' and contain no quotes or newlines.")
    if service_account_key and ("'" in service_account_key or "\n" in service_account_key):
        raise ValueError("Invalid service account key path. Must not contain quotes or newlines.")
    if columns:
        for col in columns:
            if "'" in col or '"' in col or ";" in col or "\n" in col:
                raise ValueError(f"Invalid column name: {col!r}.")

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        if service_account_key:
            con.execute(f"CREATE SECRET (TYPE GCS, KEY_PATH '{Path(service_account_key).resolve().as_posix()}');")

        select_clause = ", ".join(columns) if columns else "*"
        copy_sql = (
            f"COPY (SELECT {select_clause} FROM '{gcs_uri}') "
            f"TO '{target.as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD')"
        )
        con.execute(copy_sql)
        duration = round(time.perf_counter() - start_time, 4)
        file_size = target.stat().st_size
        row_count = con.execute(f"SELECT count(*) FROM '{target.as_posix()}'").fetchone()[0]

        if row_count == 0 and not allow_empty:
            if target.exists():
                target.unlink()
            raise ValueError("Extraction yielded 0 rows and allow_empty is False.")

        if log_progress:
            print(f"[luna] GCS copy completed: {row_count:,} rows in {duration}s -> {target.name}", flush=True)

        return ExtractionResult(
            target_path=target,
            row_count=row_count,
            batch_count=1,
            file_size_bytes=file_size,
            duration_seconds=duration,
        )
    except Exception:
        if target.exists():
            target.unlink(missing_ok=True)
        raise
    finally:
        con.close()


def gcs_to_polars(
    gcs_uri: str,
    columns: list[str] | None = None,
) -> Any:
    """Read Parquet data from GCS into an in-memory Polars DataFrame (for light datasets)."""
    import polars as pl
    return pl.read_parquet(gcs_uri, columns=columns)


def gcs_scan(
    gcs_uri: str,
) -> Any:
    """Create a Polars LazyFrame over GCS Parquet files (zero RAM until execution)."""
    import polars as pl
    return pl.scan_parquet(gcs_uri)
