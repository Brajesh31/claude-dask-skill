# Dask Bags

Dask Bags process large, semi-structured collections of Python objects. They are useful for JSON records, logs, text lines, and data that does not yet have a stable tabular schema.

## Create a bag

```python
import dask.bag as db

records = db.read_text("data/events-*.jsonl").map(json.loads)
```

A bag is lazy. Operations such as `map`, `filter`, `flatten`, and `foldby` build a graph rather than immediately reading and processing all records.

## Transform and aggregate

```python
valid = records.filter(lambda row: row.get("event_type") is not None)
counts = valid.map(lambda row: (row["event_type"], 1)).foldby(
    key=lambda pair: pair[0],
    binop=lambda total, pair: total + pair[1],
    initial=0,
    combine=lambda left, right: left + right,
    combine_initial=0,
)
result = counts.compute()
```

For large datasets, prefer `foldby` over converting the entire bag into a Python dictionary on the client.

## Partitioning and output

Dask Bags are partitioned sequences. Operations are usually applied partition by partition. Write results to partitioned output rather than collecting all records in one process.

## When to move to DataFrames

Use a Dask DataFrame when records share a reasonably consistent schema and you need joins, grouping, filtering, or column-based operations. Bag is a better fit for irregular records or early ingestion stages.

## Performance and correctness

- Keep per-record functions deterministic and side-effect free.
- Avoid large objects captured in closures.
- Normalize inconsistent records before converting to a DataFrame.
- Use explicit schemas and validation when records originate outside your system.
- Avoid `compute()` on a result that could exceed client memory.
