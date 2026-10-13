"""Google BigQuery connector for Luna.

Features:
1. Serverless Cloud Export: exports directly from BigQuery to GCS in Parquet format.
2. Direct PyArrow streaming: pulls row batches directly into Parquet files.
Supports either an active bigquery.Client or direct project/credentials parameters.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Union

from luna.connectors.core.engine import (
    PLATFORM_FETCH_BATCH_SIZE,
    strip_record_batch,
    stream_batches_to_parquet,
)
from luna.connectors.core.models import ExtractionResult


def connect(
    project: str | None = None,
    credentials: Any = None,
    **client_kwargs: Any,
) -> Any:
    """Create a BigQuery Client instance."""
    from google.cloud import bigquery

    return bigquery.Client(project=project, credentials=credentials, **client_kwargs)


def _get_job_config(params: Any) -> Any:
    if params is None:
        return None
    from google.cloud import bigquery
    return bigquery.QueryJobConfig(query_parameters=params)


def bigquery_to_parquet(
    *,
    query: str,
    target_path: Union[str, Path, None] = None,
    client: Any = None,
    params: Any = None,
    project: str | None = None,
    credentials: Any = None,
    gcs_export_uri: str | None = None,
    batch_size: int = PLATFORM_FETCH_BATCH_SIZE,
    strip_strings: bool = False,
    allow_empty: bool = True,
    log_progress: bool = False,
    **client_kwargs: Any,
) -> ExtractionResult:
    """Extract BigQuery query results to Parquet.

    Placeholder style: '@name' (named parameters),
    e.g. query="SELECT * FROM dataset.t WHERE id = @id",
    params=[bigquery.ScalarQueryParameter("id", "INT64", 10)]
    """
    start_time = time.perf_counter()
    bq_client = client or connect(project=project, credentials=credentials, **client_kwargs)

    if gcs_export_uri:
        if not gcs_export_uri.startswith("gs://") or "'" in gcs_export_uri or "\n" in gcs_export_uri:
            raise ValueError(
                f"Invalid GCS export URI: {gcs_export_uri!r}. Must start with 'gs://' and contain no quotes or newlines."
            )
        export_query = f"""
        EXPORT DATA OPTIONS(
            uri='{gcs_export_uri}',
            format='PARQUET',
            compression='ZSTD',
            overwrite=true
        ) AS
        {query}
        """
        job_config = _get_job_config(params)
        job = bq_client.query(export_query, job_config=job_config) if job_config else bq_client.query(export_query)
        job.result()
        duration = round(time.perf_counter() - start_time, 4)
        row_count = job.num_dml_affected_rows or 0
        if row_count == 0 and not allow_empty:
            raise ValueError("Extraction yielded 0 rows and allow_empty is False.")
        return ExtractionResult(
            target_path=Path(gcs_export_uri),
            row_count=row_count,
            batch_count=1,
            file_size_bytes=0,
            duration_seconds=duration,
        )

    if target_path is None:
        raise ValueError("Either 'target_path' or 'gcs_export_uri' must be provided.")

    target = Path(target_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    job_config = _get_job_config(params)
    query_job = bq_client.query(query, job_config=job_config) if job_config else bq_client.query(query)
    iterator = query_job.result(page_size=batch_size)

    def _batch_gen():
        for page in iterator.pages:
            arrow_table = page.to_arrow()
            if arrow_table is not None and arrow_table.num_rows > 0:
                yield from arrow_table.to_batches()

    return stream_batches_to_parquet(
        batch_source=_batch_gen(),
        target_path=target,
        allow_empty=allow_empty,
        log_progress=log_progress,
        strip_strings=strip_strings,
    )


def bigquery_to_polars(
    *,
    query: str,
    client: Any = None,
    params: Any = None,
    project: str | None = None,
    credentials: Any = None,
    strip_strings: bool = False,
    **client_kwargs: Any,
) -> Any:
    """Execute BigQuery query and return an in-memory Polars DataFrame."""
    import polars as pl
    import pyarrow as pa

    bq_client = client or connect(project=project, credentials=credentials, **client_kwargs)
    job_config = _get_job_config(params)
    query_job = bq_client.query(query, job_config=job_config) if job_config else bq_client.query(query)
    arrow_table = query_job.to_arrow()
    if strip_strings and arrow_table is not None:
        batches = [strip_record_batch(b) for b in arrow_table.to_batches()]
        arrow_table = pa.Table.from_batches(batches, schema=arrow_table.schema) if batches else arrow_table
    return pl.from_arrow(arrow_table)


def bigquery_to_pandas(
    *,
    query: str,
    client: Any = None,
    params: Any = None,
    project: str | None = None,
    credentials: Any = None,
    **client_kwargs: Any,
) -> Any:
    """Execute BigQuery query and return an in-memory Pandas DataFrame."""
    bq_client = client or connect(project=project, credentials=credentials, **client_kwargs)
    job_config = _get_job_config(params)
    query_job = bq_client.query(query, job_config=job_config) if job_config else bq_client.query(query)
    return query_job.to_dataframe()
