"""SQLite connector for Luna.

Streams query results directly from SQLite to Parquet via PyArrow ParquetWriter.
Uses standard library `sqlite3` with zero external dependencies.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Union

from luna.connectors.core.engine import (
    PLATFORM_FETCH_BATCH_SIZE,
    cursor_to_pandas,
    cursor_to_polars,
    run_cursor,
    stream_cursor_to_parquet,
)
from luna.connectors.core.models import ExtractionResult


def connect(database: Union[str, Path, sqlite3.Connection] = ":memory:", **connect_kwargs: Any) -> sqlite3.Connection:
    """Create or return a SQLite connection using standard library sqlite3."""
    if isinstance(database, sqlite3.Connection):
        return database
    return sqlite3.connect(database, **connect_kwargs)


def sqlite_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: sqlite3.Connection | None = None,
    database: Union[str, Path, None] = None,
    params: Any = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from SQLite directly to a Parquet file.

    Placeholder style: '?' (qmark), e.g. query="SELECT * FROM t WHERE id = ?", params=(10,)
    """
    db = connection or database
    if db is None:
        raise ValueError("Either 'connection' or 'database' must be provided.")
    active_conn = db if isinstance(db, sqlite3.Connection) else None
    return run_cursor(
        query=query,
        connection=active_conn,
        connect_fn=connect,
        connect_kwargs={"database": db, **connect_kwargs},
        params=params,
        action_fn=lambda cur: stream_cursor_to_parquet(
            cur, target_path, batch_size=batch_size, strip_strings=strip_strings, allow_empty=allow_empty, log_progress=log_progress
        ),
    )


def sqlite_to_polars(
    *,
    query: str,
    connection: sqlite3.Connection | None = None,
    database: Union[str, Path, None] = None,
    params: Any = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on SQLite and return an in-memory Polars DataFrame."""
    db = connection or database
    if db is None:
        raise ValueError("Either 'connection' or 'database' must be provided.")
    active_conn = db if isinstance(db, sqlite3.Connection) else None
    return run_cursor(
        query=query,
        connection=active_conn,
        connect_fn=connect,
        connect_kwargs={"database": db, **connect_kwargs},
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def sqlite_to_pandas(
    *,
    query: str,
    connection: sqlite3.Connection | None = None,
    database: Union[str, Path, None] = None,
    params: Any = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on SQLite and return an in-memory Pandas DataFrame."""
    db = connection or database
    if db is None:
        raise ValueError("Either 'connection' or 'database' must be provided.")
    active_conn = db if isinstance(db, sqlite3.Connection) else None
    return run_cursor(
        query=query,
        connection=active_conn,
        connect_fn=connect,
        connect_kwargs={"database": db, **connect_kwargs},
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
