from typing import Any, Dict, List

import pyarrow as pa
import pandas as pd

import dask.dataframe as dd
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer

from clickhouse_driver import Client


def _make_arrow_record_batch(batch: List[Any], columns: List[str]) -> pa.RecordBatch:
    df = pd.DataFrame(batch, columns=columns)
    return pa.RecordBatch.from_pandas(df)


def _fetch_batch(batch: pa.RecordBatch) -> pd.DataFrame:
    return batch.to_pandas()


@delayed
def _fetch_query_batches(
        query: str, connection_kwargs: Dict[str, Any], chunk_size: int, **kwargs
) -> List[pa.RecordBatch]:
    client = Client(**connection_kwargs)
    settings = dict(max_block_size=chunk_size)
    batch_iter = client.execute_iter(query, with_column_types=True, settings=settings, chunk_size=chunk_size, **kwargs)
    first_batch = next(batch_iter)
    schema = first_batch.pop(0)
    columns = [t[0] for t in schema]
    batches = [_make_arrow_record_batch(first_batch, columns)]
    batches += [_make_arrow_record_batch(batch, columns) for batch in batch_iter]
    return batches


def read_clickhouse(query: str, connection_kwargs: Dict[str, Any], partitions_size=10000, **kwargs) -> dd.DataFrame:
    label = "read-clickhouse-"
    output_name = label + tokenize(
        query,
        connection_kwargs,
    )

    batches: List[pa.RecordBatch] = _fetch_query_batches(
        query,
        connection_kwargs, chunk_size=partitions_size,
        **kwargs
    ).compute()
    meta: pd.DataFrame = batches[0].to_pandas()

    if not batches:
        graph = {(output_name, 0): meta}
        divisions = (None, None)
    else:
        layer = DataFrameIOLayer(
            name=output_name,
            columns=meta.columns,
            inputs=batches,
            io_func=_fetch_batch,
            label=label,
        )
        divisions = tuple([None] * (len(batches) + 1))
        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)
