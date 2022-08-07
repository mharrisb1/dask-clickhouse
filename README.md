# Dask-Clickhouse

Dask Clickhouse connector.

## Installation

```shell
pip install dask-clickhouse
```

## Usage

`dask-clickhouse` provides `read_from_table` and `write_to_table` methods for parallel IO to and from Clickhouse with Dask.

```python
from dask_clickhouse import read_clickhouse

query = "SELECT * FROM example_table"

ddf = read_clickhouse(
    query=query,
    connection_kwargs={"...": "..."}
)
```
