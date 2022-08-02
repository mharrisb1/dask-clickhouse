# Dask-Clickhouse

Dask Clickhouse connector.

⚠️ Highly experimental. API and functionality will change ⚠️

## Installation

```shell
pip install dask-clickhouse
```

## Usage

`dask-clickhouse` provides `read_from_table` and `write_to_table` methods for parallel IO to and from Clickhouse with Dask.

```python
from dask_clickhouse import read_from_table

ddf = read_from_table(
    database="...",
    table="...",
    connection_kwargs={"...": "..."}
)
```

```python
import dask.dataframe as dd
from dask_clickhouse import write_to_table

ddf = dd.DataFrame(...)

ddf = write_to_table(
    ddf=ddf,
    database="...",
    table="...",
    connection_kwargs={"...": "..."}
)
```