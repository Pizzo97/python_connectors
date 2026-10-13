"""MySQL connector for Luna.

Streams query results directly from MySQL/MariaDB to Parquet via PyArrow ParquetWriter.
Uses pure-Python `pymysql` driver with zero compilation friction.
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
    host: str = "localhost",
    port: int = 3306,
    *,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create a MySQL/MariaDB connection using pymysql."""
    import pymysql

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        **connect_kwargs,
    )


def mysql_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str = "localhost",
    port: int = 3306,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from MySQL/MariaDB directly to a Parquet file.

    Placeholder style: '%s' (format) or '%(name)s' (pyformat),
    e.g. query="SELECT * FROM t WHERE id = %s", params=(10,)
    or query="SELECT * FROM t WHERE id = %(id)s", params={"id": 10}
    """
    kwargs = {"host": host, "port": port, "user": user, "password": password, "database": database, **connect_kwargs}
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


def mysql_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str = "localhost",
    port: int = 3306,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on MySQL and return an in-memory Polars DataFrame."""
    kwargs = {"host": host, "port": port, "user": user, "password": password, "database": database, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def mysql_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    host: str = "localhost",
    port: int = 3306,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on MySQL and return an in-memory Pandas DataFrame."""
    kwargs = {"host": host, "port": port, "user": user, "password": password, "database": database, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
