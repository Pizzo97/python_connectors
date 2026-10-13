"""SAS dataset (.sas7bdat, .xpt) connector for Luna.

Streams legacy SAS files directly into modern Parquet format via PyArrow
with platform-optimized row groups and zero intermediate files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Generator, Sequence, Union

import pyarrow as pa

from luna.connectors.core.engine import (
    PLATFORM_FETCH_BATCH_SIZE,
    batches_to_pandas,
    batches_to_polars,
    strip_record_batch,
    stream_batches_to_parquet,
)
from luna.connectors.core.models import ExtractionResult


def _stream_sas_batches(
    sas_file_path: Union[str, Path],
    columns: Sequence[str] | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Generator[pa.RecordBatch, None, None]:
    """Generator yielding Arrow RecordBatches directly from SAS files in chunks."""
    try:
        import pyreadstat
    except ImportError as exc:
        raise ImportError(
            "pyreadstat is required to read SAS files. Install it via: pip install pyreadstat"
        ) from exc

    file_str = str(Path(sas_file_path).resolve())
    is_xpt = file_str.lower().endswith(".xpt")
    read_fn = pyreadstat.read_xport if is_xpt else pyreadstat.read_sas7bdat

    reader = read_fn(
        file_str,
        chunksize=batch_size,
        usecols=columns,
        multiprocess=False,
    )

    for df_pandas, _meta in reader:
        arrow_table = pa.Table.from_pandas(df_pandas)
        batches = arrow_table.to_batches()
        if strip_strings:
            yield from (strip_record_batch(b) for b in batches)
        else:
            yield from batches


def sas_to_parquet(
    *,
    sas_file: Union[str, Path],
    target_path: Union[str, Path],
    columns: Sequence[str] | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
) -> ExtractionResult:
    """Stream a SAS (.sas7bdat, .xpt) dataset directly into a Parquet file with constant RAM (~130MB max).

    Args:
        sas_file: Path to the .sas7bdat or .xpt file.
        target_path: Output Parquet file path.
        columns: Optional list of column names to extract.
        batch_size: Number of rows per read chunk (default 50,000; lower if rows are very wide).
        strip_strings: If True, trims leading and trailing whitespace from character variables.
        allow_empty: If False, raises ValueError when 0 rows are extracted.
        log_progress: If True, logs progress at each Row Group.

    Returns:
        ExtractionResult summarizing rows, row groups, size, duration, and target path.
    """
    target = Path(target_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    batch_gen = _stream_sas_batches(
        sas_file_path=sas_file, columns=columns, batch_size=batch_size, strip_strings=strip_strings
    )
    return stream_batches_to_parquet(
        batch_source=batch_gen,
        target_path=target,
        allow_empty=allow_empty,
        log_progress=log_progress,
    )


def sas_to_polars(
    *,
    sas_file: Union[str, Path],
    columns: Sequence[str] | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Any:
    """Read a SAS dataset into an in-memory Polars DataFrame (for light files)."""
    return batches_to_polars(
        _stream_sas_batches(sas_file, columns=columns, batch_size=batch_size, strip_strings=strip_strings)
    )


def sas_to_pandas(
    *,
    sas_file: Union[str, Path],
    columns: Sequence[str] | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
) -> Any:
    """Read a SAS dataset into an in-memory Pandas DataFrame (for light files)."""
    return batches_to_pandas(
        _stream_sas_batches(sas_file, columns=columns, batch_size=batch_size, strip_strings=strip_strings)
    )
