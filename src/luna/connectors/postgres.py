"""PostgreSQL connector for Luna.

Streams query results directly from PostgreSQL to Parquet via PyArrow ParquetWriter.
Uses modern `psycopg` (v3) driver with binary C/Rust wire protocol.
"""

from __future__ import annotations

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


def connect(
    conn_info: str | None = None,
    *,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 5432,
    **connect_kwargs: Any,
) -> Any:
    """Create a PostgreSQL connection using psycopg (v3)."""
    try:
        import psycopg as pg_driver
    except ImportError:
        import psycopg2 as pg_driver

    if conn_info:
        return pg_driver.connect(conn_info, **connect_kwargs)
    return pg_driver.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        **connect_kwargs,
    )


def postgres_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    conn_info: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 5432,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from PostgreSQL directly to a Parquet file.

    Placeholder style: '%s' (format) or '%(name)s' (pyformat),
    e.g. query="SELECT * FROM t WHERE id = %s", params=(10,)
    or query="SELECT * FROM t WHERE id = %(id)s", params={"id": 10}
    """
    kwargs = {"conn_info": conn_info, "user": user, "password": password, "database": database, "host": host, "port": port, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: stream_cursor_to_parquet(
            cur, target_path, batch_size=batch_size, strip_strings=strip_strings, allow_empty=allow_empty, log_progress=log_progress
        ),
    )


def postgres_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    conn_info: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 5432,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on PostgreSQL and return an in-memory Polars DataFrame."""
    kwargs = {"conn_info": conn_info, "user": user, "password": password, "database": database, "host": host, "port": port, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def postgres_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    conn_info: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 5432,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on PostgreSQL and return an in-memory Pandas DataFrame."""
    kwargs = {"conn_info": conn_info, "user": user, "password": password, "database": database, "host": host, "port": port, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
