"""Microsoft SQL Server (MSSQL) connector for Luna.

Streams query results directly from SQL Server to Parquet via Turbodbc C++ Arrow buffers.
Requires unixODBC and Microsoft ODBC Driver for SQL Server (msodbcsql18) installed.
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
    server: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int = 1433,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_server_certificate: bool = True,
    dsn: str | None = None,
    connection_string: str | None = None,
    **connect_kwargs: Any,
) -> Any:
    """Create a high-performance Microsoft SQL Server connection using turbodbc."""
    import turbodbc

    if connection_string:
        return turbodbc.connect(connection_string=connection_string, **connect_kwargs)

    if dsn and not (server or database or user):
        return turbodbc.connect(dsn=dsn, **connect_kwargs)

    conn_parts: list[str] = [f"DSN={dsn}"] if dsn else [f"DRIVER={{{driver}}}"]
    if server:
        server_str = f"{server},{port}" if port != 1433 else server
        conn_parts.append(f"SERVER={server_str}")
    if database:
        conn_parts.append(f"DATABASE={database}")
    if user:
        conn_parts.append(f"UID={user}")
    if password:
        conn_parts.append(f"PWD={password}")
    if trust_server_certificate:
        conn_parts.append("TrustServerCertificate=yes")

    conn_str = ";".join(conn_parts) + ";"
    return turbodbc.connect(connection_string=conn_str, **connect_kwargs)


def mssql_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path],
    connection: Any = None,
    params: Any = None,
    server: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int = 1433,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_server_certificate: bool = True,
    dsn: str | None = None,
    connection_string: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **connect_kwargs: Any,
) -> ExtractionResult:
    """Stream query results from Microsoft SQL Server directly to Parquet via turbodbc C++ Arrow buffers.

    Placeholder style: '?' (qmark), e.g. query="SELECT * FROM t WHERE id = ?", params=(10,)
    """
    kwargs = {
        "server": server, "database": database, "user": user, "password": password,
        "port": port, "driver": driver, "trust_server_certificate": trust_server_certificate,
        "dsn": dsn, "connection_string": connection_string, **connect_kwargs,
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


def mssql_to_polars(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    server: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int = 1433,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_server_certificate: bool = True,
    dsn: str | None = None,
    connection_string: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Microsoft SQL Server and return an in-memory Polars DataFrame."""
    kwargs = {
        "server": server, "database": database, "user": user, "password": password,
        "port": port, "driver": driver, "trust_server_certificate": trust_server_certificate,
        "dsn": dsn, "connection_string": connection_string, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_polars(cur, batch_size=batch_size, strip_strings=strip_strings),
    )


def mssql_to_pandas(
    *,
    query: str,
    connection: Any = None,
    params: Any = None,
    server: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int = 1433,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_server_certificate: bool = True,
    dsn: str | None = None,
    connection_string: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    **connect_kwargs: Any,
) -> Any:
    """Execute query on Microsoft SQL Server and return an in-memory Pandas DataFrame."""
    kwargs = {
        "server": server, "database": database, "user": user, "password": password,
        "port": port, "driver": driver, "trust_server_certificate": trust_server_certificate,
        "dsn": dsn, "connection_string": connection_string, **connect_kwargs,
    }
    return run_cursor(
        query=query,
        connection=connection,
        connect_fn=connect,
        connect_kwargs=kwargs,
        params=params,
        action_fn=lambda cur: cursor_to_pandas(cur, batch_size=batch_size, strip_strings=strip_strings),
    )
