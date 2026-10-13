"""Oracle Database connector for Luna.

Streams query results directly from Oracle to Parquet via PyArrow ParquetWriter.
Uses official `oracledb` driver with optimal bulk network buffering.
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
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
    host: str | None = None,
    port: int = 1521,
    service_name: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create an Oracle connection using official oracledb."""
    import oracledb

    if not dsn and host and service_name:
        dsn = f"{host}:{port}/{service_name}"

    return oracledb.connect(user=user, password=password, dsn=dsn, **connect_kwargs)


def _configure_cursor(cursor: Any, batch_size: int = PLATFORM_FETCH_BATCH_SIZE) -> None:
    """Tune Oracle cursor network buffers for bulk streaming."""
    if hasattr(cursor, "arraysize"):
        cursor.arraysize = batch_size
    if hasattr(cursor, "prefetchrows"):
        cursor.prefetchrows = batch_size


def oracle_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
    host: str | None = None,
    port: int = 1521,
    service_name: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from Oracle directly to a Parquet file.

    Placeholder style: ':1', ':2' (positional) or ':name' (named dictionary),
    e.g. query="SELECT * FROM emp WHERE id = :1", params=(10,)
    or query="SELECT * FROM emp WHERE id = :id", params={"id": 10}
    """
    kwargs = {"user": user, "password": password, "dsn": dsn, "host": host, "port": port, "service_name": service_name, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        configure_cursor=lambda cur: _configure_cursor(cur, batch_size=batch_size),
        params=params,
        action_fn=lambda cur: stream_cursor_to_parquet(
            cur, target_path, batch_size=batch_size, strip_strings=strip_strings, allow_empty=allow_empty, log_progress=log_progress
        ),
    )


def oracle_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
    host: str | None = None,
    port: int = 1521,
    service_name: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Oracle and return an in-memory Polars DataFrame."""
    kwargs = {"user": user, "password": password, "dsn": dsn, "host": host, "port": port, "service_name": service_name, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        configure_cursor=lambda cur: _configure_cursor(cur, batch_size=batch_size),
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def oracle_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
    host: str | None = None,
    port: int = 1521,
    service_name: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Oracle and return an in-memory Pandas DataFrame."""
    kwargs = {"user": user, "password": password, "dsn": dsn, "host": host, "port": port, "service_name": service_name, **connect_kwargs}
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        configure_cursor=lambda cur: _configure_cursor(cur, batch_size=batch_size),
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
