"""Dremio Lakehouse connector for Luna.

Streams query results directly from Dremio to Parquet via Apache Arrow Flight SQL (ADBC).
Pure Arrow gRPC streaming with zero Python tuple serialization.
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
    port: int = 32010,
    *,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    use_ssl: bool = False,
    uri: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create a high-performance Arrow Flight SQL (ADBC) connection to Dremio."""
    from adbc_driver_flightsql import dbapi as flightsql

    if uri:
        endpoint = uri
    else:
        scheme = "grpc+tls" if use_ssl else "grpc+tcp"
        endpoint = f"{scheme}://{host}:{port}"

    dbapi_kwargs: dict[str, Any] = {}
    if user and password:
        dbapi_kwargs["username"] = user
        dbapi_kwargs["password"] = password
    if token:
        dbapi_kwargs["adbc.flight.sql.rpc.call_header.authorization"] = f"Bearer {token}"

    dbapi_kwargs.update(connect_kwargs)
    return flightsql.connect(endpoint, dbapi_kwargs=dbapi_kwargs)


def dremio_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    host: str = "localhost",
    port: int = 32010,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    use_ssl: bool = False,
    uri: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from Dremio directly to Parquet via Arrow Flight SQL (ADBC).

    Placeholder style: '?' (qmark), e.g. query="SELECT * FROM space.t WHERE id = ?", params=(10,)
    """
    kwargs = {
        "host": host, "port": port, "user": user, "password": password,
        "token": token, "use_ssl": use_ssl, "uri": uri, **connect_kwargs,
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


def dremio_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    host: str = "localhost",
    port: int = 32010,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    use_ssl: bool = False,
    uri: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Dremio and return an in-memory Polars DataFrame (zero-copy from Arrow)."""
    kwargs = {
        "host": host, "port": port, "user": user, "password": password,
        "token": token, "use_ssl": use_ssl, "uri": uri, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def dremio_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    host: str = "localhost",
    port: int = 32010,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    use_ssl: bool = False,
    uri: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Dremio and return an in-memory Pandas DataFrame."""
    kwargs = {
        "host": host, "port": port, "user": user, "password": password,
        "token": token, "use_ssl": use_ssl, "uri": uri, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
