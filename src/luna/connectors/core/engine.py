"""Core PyArrow streaming engine for Luna platform.

Enforces platform-standard Parquet optimizations:
- 50,000 rows per database fetch (saturates network throughput with minimal Python tuple overhead)
- 128 MB uncompressed per Parquet Row Group (measured via Arrow batch.nbytes)
- 250,000 rows maximum per Row Group (guarantees multi-core scaling on narrow tables)
- Zstandard level 3 compression
- 1MB data page size
- Pure single-pass streaming with zero temporary files and deterministic RAM usage (~130MB max).
"""

from __future__ import annotations

import itertools
import logging
import time
from pathlib import Path
from typing import Any, Generator, Iterable, Union

import pyarrow as pa
import pyarrow.parquet as pq

from luna.connectors.core.models import ExtractionResult

logger = logging.getLogger("luna")

# ==============================================================================
# Platform Optimization Constants (Governed by platform, not by user)
# ==============================================================================
PLATFORM_FETCH_BATCH_SIZE = 50_000                  # Optimal DB network fetch size (rows)
PLATFORM_ROW_GROUP_BYTES = 128 * 1024 * 1024       # 128 MB uncompressed target per Row Group
PLATFORM_MAX_ROW_GROUP_ROWS = 250_000              # Sweet spot cap for multi-core parallelism and predicate pushdown
PLATFORM_COMPRESSION = "zstd"                      # Best compression/decompression ratio
PLATFORM_COMPRESSION_LEVEL = 3                     # Industrial standard
PLATFORM_DATA_PAGE_SIZE = 1024 * 1024              # 1MB Parquet data page size


def _accumulate_to_row_groups(
    batch_iterator: Iterable[pa.RecordBatch],
    target_bytes: int = PLATFORM_ROW_GROUP_BYTES,
    max_rows: int = PLATFORM_MAX_ROW_GROUP_ROWS,
) -> Generator[pa.Table, None, None]:
    """Combines batches until reaching ~128 MB in uncompressed Arrow memory before writing to disk."""
    buffer: list[pa.RecordBatch] = []
    current_bytes = current_rows = 0

    for batch in batch_iterator:
        if batch.num_rows == 0:
            continue
        buffer.append(batch)
        current_bytes += batch.nbytes
        current_rows += batch.num_rows

        if current_bytes >= target_bytes or current_rows >= max_rows:
            yield pa.Table.from_batches(buffer)
            buffer, current_bytes, current_rows = [], 0, 0

    if buffer:
        yield pa.Table.from_batches(buffer)


def stream_batches_to_parquet(
    batch_source: Union[Iterable[pa.RecordBatch], pa.RecordBatchReader],
    target_path: Union[str, Path],
    schema: pa.Schema | None = None,
    allow_empty: bool = True,
    log_progress: bool = False,
    strip_strings: bool = False,
) -> ExtractionResult:
    """Write an Arrow batch stream directly to a Parquet file in a single pass."""
    start_time = time.perf_counter()
    target = Path(target_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    try:
        iterator = iter(batch_source)
        first_item = next(iterator, None)

        if first_item is None:
            if not allow_empty:
                raise ValueError("Extraction yielded 0 rows and allow_empty is False.")
            if schema is None:
                raise ValueError("The batch source is empty and no schema was provided to write Parquet.")
            pq.write_table(pa.Table.from_batches([], schema=schema), target, compression=PLATFORM_COMPRESSION)
            total_rows = row_groups_written = 0
        else:
            full_items = itertools.chain([first_item], iterator)
            batches = (b for item in full_items for b in (item.to_batches() if isinstance(item, pa.Table) else [item]))
            if strip_strings:
                batches = (strip_record_batch(b) for b in batches)
            total_rows = row_groups_written = 0

            with pq.ParquetWriter(
                target,
                schema=first_item.schema,
                compression=PLATFORM_COMPRESSION,
                compression_level=PLATFORM_COMPRESSION_LEVEL,
                use_dictionary=True,
                write_statistics=True,
                data_page_size=PLATFORM_DATA_PAGE_SIZE,
            ) as writer:
                for rg in _accumulate_to_row_groups(batches):
                    writer.write_table(rg)
                    total_rows += rg.num_rows
                    row_groups_written += 1
                    if log_progress:
                        msg = f"[luna] Extracted {total_rows:,} rows ({row_groups_written} row groups) -> {target.name}"
                        if logger.hasHandlers() or logging.getLogger().hasHandlers():
                            logger.info(msg)
                        else:
                            print(msg, flush=True)

            if total_rows == 0 and not allow_empty:
                if target.exists():
                    target.unlink()
                raise ValueError("Extraction yielded 0 rows and allow_empty is False.")

        if log_progress and total_rows > 0:
            duration = round(time.perf_counter() - start_time, 2)
            msg = f"[luna] Completed: {total_rows:,} rows in {duration}s -> {target.name}"
            if logger.hasHandlers() or logging.getLogger().hasHandlers():
                logger.info(msg)
            else:
                print(msg, flush=True)

        return ExtractionResult(
            target_path=target,
            row_count=total_rows,
            batch_count=row_groups_written,
            file_size_bytes=target.stat().st_size if target.exists() else 0,
            duration_seconds=round(time.perf_counter() - start_time, 4),
        )
    except Exception:
        if target.exists():
            target.unlink(missing_ok=True)
        raise


def strip_record_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Trim leading and trailing whitespace from string columns using PyArrow Compute in C++."""
    import pyarrow.compute as pc
    new_cols = [
        pc.utf8_trim_whitespace(col)
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type)
        else col
        for col in batch.columns
    ]
    return pa.RecordBatch.from_arrays(new_cols, schema=batch.schema)


def stream_cursor_to_batches(
    cursor: Any,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
) -> Generator[pa.RecordBatch, None, None]:
    """Fetch rows from a standard Python DB-API cursor directly into Arrow RecordBatches."""
    cols = [d[0].lower() for d in cursor.description]
    while rows := cursor.fetchmany(batch_size):
        yield pa.RecordBatch.from_arrays([pa.array(col) for col in zip(*rows)], names=cols)


def cursor_to_batches(
    cursor: Any,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Iterable[pa.RecordBatch]:
    """Universal dispatcher: extracts Arrow record batches from any supported cursor type."""
    if hasattr(cursor, "fetcharrowbatches"):
        # Turbodbc C++ Arrow buffers (used by: Microsoft SQL Server, IBM DB2, Teradata Vantage)
        batches = cursor.fetcharrowbatches()
    elif hasattr(cursor, "fetch_record_batch"):
        # Apache Arrow ADBC Flight SQL gRPC stream (used by: Dremio Lakehouse)
        batches = cursor.fetch_record_batch()
    else:
        # Standard Python DB-API 2.0 cursor (used by: Oracle, PostgreSQL, MySQL, SQLite)
        batches = stream_cursor_to_batches(cursor, batch_size=batch_size)

    if strip_strings:
        return (strip_record_batch(b) for b in batches)
    return batches


def stream_cursor_to_parquet(
    cursor: Any,
    target_path: Union[str, Path],
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    schema: pa.Schema | None = None,
) -> ExtractionResult:
    """Stream cursor directly into a Parquet file via PyArrow in a single pass."""
    inferred_schema = schema
    if inferred_schema is None and hasattr(cursor, "description") and cursor.description:
        inferred_schema = pa.schema([pa.field(d[0].lower(), pa.string()) for d in cursor.description])

    return stream_batches_to_parquet(
        batch_source=cursor_to_batches(cursor, batch_size=batch_size, strip_strings=strip_strings),
        target_path=target_path,
        schema=inferred_schema,
        allow_empty=allow_empty,
        log_progress=log_progress,
    )


def cursor_to_arrow_table(
    cursor: Any,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> pa.Table:
    """Consume cursor into an in-memory PyArrow Table."""
    batches = list(cursor_to_batches(cursor, batch_size=batch_size, strip_strings=strip_strings))
    if batches:
        return pa.Table.from_batches(batches)
    cols = [d[0].lower() for d in cursor.description] if hasattr(cursor, "description") and cursor.description else []
    return pa.Table.from_arrays([pa.array([]) for _ in cols], names=cols)


def batches_to_polars(batches_or_table: Any) -> Any:
    """Convert Arrow batches or Table into an in-memory Polars DataFrame.

    Note: Materializes the entire result set in RAM. For large datasets,
    prefer streaming to Parquet and scanning with `pl.scan_parquet()`.
    """
    import polars as pl
    if isinstance(batches_or_table, pa.Table):
        return pl.from_arrow(batches_or_table)
    batches = list(batches_or_table)
    return pl.from_arrow(pa.Table.from_batches(batches)) if batches else pl.DataFrame()


def batches_to_pandas(batches_or_table: Any) -> Any:
    """Convert Arrow batches or Table into an in-memory Pandas DataFrame.

    Note: Materializes the entire result set in RAM. For large datasets,
    prefer streaming to Parquet and reading with DuckDB or Polars.
    """
    import pandas as pd
    if isinstance(batches_or_table, pa.Table):
        return batches_or_table.to_pandas()
    batches = list(batches_or_table)
    return pa.Table.from_batches(batches).to_pandas() if batches else pd.DataFrame()


def cursor_to_polars(
    cursor: Any,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Any:
    """Consume cursor into an in-memory Polars DataFrame (for light reference tables)."""
    return batches_to_polars(cursor_to_arrow_table(cursor, batch_size=batch_size, strip_strings=strip_strings))


def cursor_to_pandas(
    cursor: Any,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Any:
    """Consume cursor into an in-memory Pandas DataFrame (for light reference tables)."""
    return batches_to_pandas(cursor_to_arrow_table(cursor, batch_size=batch_size, strip_strings=strip_strings))


def run_cursor(
    query: str,
    connection: Any,
    connect_fn: Any,
    connect_kwargs: dict[str, Any],
    action_fn: Any,
    configure_cursor: Any = None,
    params: Any = None,
) -> Any:
    """Manage the connection and cursor lifecycle cleanly: open, execute, and guaranteed cleanup."""
    should_close = connection is None
    conn = connect_fn(**connect_kwargs) if should_close else connection
    try:
        cursor = conn.cursor()
        try:
            if configure_cursor is not None:
                configure_cursor(cursor)
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return action_fn(cursor)
        finally:
            cursor.close()
    finally:
        if should_close:
            conn.close()
