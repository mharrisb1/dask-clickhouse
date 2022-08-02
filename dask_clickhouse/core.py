from typing import Any, Dict

import ibis
from ibis.backends.clickhouse import Backend

import pandas as pd
from dask import delayed
import dask.dataframe as dd


def _get_table_system_info(conn: Backend, database: str, table: str) -> pd.DataFrame:
    p = conn.table("system.parts")["database", "table", "partition", "rows"]
    t = conn.table("system.tables")["database", "name", "partition_key", "sorting_key"]
    expr = p.left_join(t, [p.database == t.database, p.table == t.name])
    expr = expr[p.database, p.table, p.partition, t.partition_key, t.sorting_key, p.rows]
    expr = expr.filter([expr.database == database, expr.table == table])
    expr = expr.groupby([expr.database, expr.table, expr.partition, expr.partition_key, expr.sorting_key]).aggregate(
        expr.rows.sum().name("rows")
    )
    results = expr.execute()
    return results


@delayed
def _get_part(
    database: str,
    table: str,
    partition: Any,
    partition_key: str,
    connection_kwargs: Dict[str, Any],
) -> pd.DataFrame:
    conn = ibis.clickhouse.connect(**connection_kwargs)
    expr = conn.sql(f"select * from {database}.{table} where {partition_key} = {partition}")
    conn.close()
    return expr.execute()


@delayed
def _get_page(
    database: str,
    table: str,
    sorting_key: str,
    limit: int,
    offset: int,
    connection_kwargs: Dict[str, Any],
) -> pd.DataFrame:
    conn = ibis.clickhouse.connect(**connection_kwargs)
    expr = conn.sql(f"select * from {database}.{table} order by {sorting_key} limit {limit} offset {offset}")
    conn.close()
    return expr.execute(limit=limit)


def read_from_table(database: str, table: str, connection_kwargs: Dict[str, Any], verbose: bool = False) -> dd.DataFrame:
    ibis.options.verbose = verbose
    conn = ibis.clickhouse.connect(**connection_kwargs)
    part_df = _get_table_system_info(conn, database, table)
    meta = conn.table(f"{database}.{table}").limit(0).execute()
    if len(part_df) > 1:
        parts = []
        for _, row in part_df.iterrows():
            df = _get_part(row.database, row.table, row.partition, row.partition_key, connection_kwargs)
            parts.append(df)
    else:
        parts = []
        limit = 50000
        for offset in range(0, part_df.rows.iloc[0], limit):
            df = _get_page(database, table, part_df.sorting_key.iloc[0], limit, offset, connection_kwargs)
            parts.append(df)

    return dd.from_delayed(dfs=parts, meta=meta)


def write_to_table(
    ddf: dd.DataFrame,
    database: str,
    table: str,
    connection_kwargs: Dict[str, Any],
    method: str = "append",
    verbose: bool = False,
) -> int:
    conn = ibis.clickhouse.connect(**connection_kwargs)
    ibis.options.verbose = verbose
    if method not in ("append", "overwrite"):
        raise ValueError("Method must be one of ('append', 'overwrite')")

    if method == "overwrite":
        conn.raw_sql(f"truncate table {database}.{table}")

    table = conn.table(f"{database}.{table}")

    rows_inserted = 0

    for part in ddf.partitions:
        r = table.insert(part.compute())
        rows_inserted += r

    return rows_inserted
