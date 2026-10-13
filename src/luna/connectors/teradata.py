"""Teradata Vantage connector for Luna.

Streams query results directly from Teradata Vantage to Parquet via Turbodbc C++ Arrow buffers.
Requires unixODBC and Teradata ODBC Driver installed at the OS level.
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
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    mechanism: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create a high-performance Teradata ODBC connection using turbodbc."""
    import turbodbc

    if connection_string:
        return turbodbc.connect(connection_string=connection_string, **connect_kwargs)

    if dsn and not (host or user or password):
        return turbodbc.connect(dsn=dsn, **connect_kwargs)

    conn_parts: list[str] = [f"DSN={dsn}"] if dsn else ["DRIVER={Teradata Database ODBC Driver 17.20}"]
    if host:
        conn_parts.append(f"DBCNAME={host}")
    if user:
        conn_parts.append(f"UID={user}")
    if password:
        conn_parts.append(f"PWD={password}")
    if database:
        conn_parts.append(f"DATABASE={database}")
    if mechanism:
        conn_parts.append(f"AUTHENTICATION={mechanism}")

    conn_str = ";".join(conn_parts) + ";"
    return turbodbc.connect(connection_string=conn_str, **connect_kwargs)


def teradata_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    mechanism: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from Teradata Vantage directly to Parquet via turbodbc C++ Arrow buffers.

    Placeholder style: '?' (qmark), e.g. query="SELECT * FROM t WHERE id = ?", params=(10,)
    """
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "host": host,
        "user": user, "password": password, "database": database, "mechanism": mechanism, **connect_kwargs,
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


def teradata_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    mechanism: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Teradata and return an in-memory Polars DataFrame."""
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "host": host,
        "user": user, "password": password, "database": database, "mechanism": mechanism, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def teradata_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    dsn: str | None = None,
    connection_string: str | None = None,
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    mechanism: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Teradata and return an in-memory Pandas DataFrame."""
    kwargs = {
        "dsn": dsn, "connection_string": connection_string, "host": host,
        "user": user, "password": password, "database": database, "mechanism": mechanism, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
