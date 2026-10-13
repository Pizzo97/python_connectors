"""IBM DB2 connector for Luna.

Streams query results directly from IBM DB2 to Parquet via Turbodbc C++ Arrow buffers.
Requires unixODBC and IBM DB2 ODBC Driver installed at the OS level.
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
    dsn: str | None = None,
    connection_string: str | None = None,
    *,
    database: str | None = None,
    host: str | None = None,
    port: int = 50000,
    user: str | None = None,
    password: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create a high-performance DB2 ODBC connection using turbodbc."""
    import turbodbc

    if connection_string:
        return turbodbc.connect(connection_string=connection_string, **connect_kwargs)

    if dsn and not (database or host or user):
        return turbodbc.connect(dsn=dsn, **connect_kwargs)

    conn_parts: list[str] = [f"DSN={dsn}"] if dsn else ["DRIVER={IBM DB2 ODBC DRIVER}"]
    if database:
        conn_parts.append(f"DATABASE={database}")
    if host:
        conn_parts.append(f"HOSTNAME={host}")
    if port:
        conn_parts.append(f"PORT={port}")
    if user:
        conn_parts.append(f"UID={user}")
    if password:
        conn_parts.append(f"PWD={password}")

    conn_str = ";".join(conn_parts) + ";"
    return turbodbc.connect(connection_string=conn_str, **connect_kwargs)


def db2_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 50000,
    user: str | None = None,
    password: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from IBM DB2 directly to Parquet using C++ turbodbc Arrow buffers.

    Placeholder style: '?' (qmark), e.g. query="SELECT * FROM t WHERE id = ?", params=(10,)
    """
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "database": database,
        "host": host, "port": port, "user": user, "password": password, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: stream_cursor_to_parquet(
            cur,
            target_path,
            batch_size=batch_size,
            strip_strings=strip_strings,
            allow_empty=allow_empty,
            log_progress=log_progress,
        ),
    )


def db2_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 50000,
    user: str | None = None,
    password: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on IBM DB2 and return an in-memory Polars DataFrame."""
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "database": database,
        "host": host, "port": port, "user": user, "password": password, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def db2_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    database: str | None = None,
    host: str | None = None,
    port: int = 50000,
    user: str | None = None,
    password: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on IBM DB2 and return an in-memory Pandas DataFrame."""
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "database": database,
        "host": host, "port": port, "user": user, "password": password, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
