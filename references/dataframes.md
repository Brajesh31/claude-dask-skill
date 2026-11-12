# Dask DataFrames

Dask DataFrames split tabular data into partitions and provide a pandas-like interface for datasets that can be processed in parallel.

## Read data

```python
import dask.dataframe as dd

df = dd.read_parquet("warehouse/events/")
# or: dd.read_csv("raw/events-*.csv", blocksize="128 MiB")
```

Inspect `df.npartitions`, `df.dtypes`, and `df.head()` before building a large pipeline. `head()` computes only a small sample.

## Filter and transform

```python
recent = df[df["event_date"] >= "2025-01-01"]
summary = recent.groupby("country").amount.sum()
result = summary.compute()
```

Filter early and project only columns required downstream. Avoid Python row-wise `apply` when vectorized operations are available.

## Joins and groupbys

Large joins and groupbys may require shuffles. Filter both inputs first, select only needed columns, and check join-key quality. If one side is small enough, a broadcast-style strategy may reduce network movement, but benchmark it.

## Indexing and divisions

Known, sorted index divisions can make index-based joins and range queries more efficient. Setting an index can be expensive because it may shuffle and sort the data. Do it when the resulting layout will be reused enough to repay the cost.

## Custom functions

Use `map_partitions` for partition-level logic. Specify `meta` so Dask can infer output columns and dtypes reliably:

```python
def normalize_partition(partition):
    result = partition.copy()
    result["email"] = result["email"].str.lower().str.strip()
    return result

clean = df.map_partitions(
    normalize_partition,
    meta=df._meta,
)
```

## Outputs and memory

Write partitioned Parquet for scalable output. Avoid converting a large Dask DataFrame to pandas. Compute only final aggregates or bounded samples that fit in client memory.

## Differences from pandas

Not every pandas API is implemented identically, and operations may have different performance characteristics. Test semantics, null handling, ordering, and dtypes on a representative dataset.
