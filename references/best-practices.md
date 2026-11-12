# Dask Best Practices

## Design the workload before the cluster

Identify input size, data format, transformation cost, memory limits, output volume, and acceptable latency. A larger cluster does not fix an inefficient algorithm or a badly sized task graph.

## Chunk and partition sizing

- Use chunks large enough to keep task scheduling overhead reasonable.
- Keep several active chunks per worker, but leave memory headroom for temporary objects.
- Avoid millions of tiny tasks.
- Align partitions or chunks with common joins, groupbys, and reductions.
- Repartition only when measurement shows a meaningful benefit.

## Laziness and computation boundaries

Dask collections are lazy. Build a graph, then compute or persist at a deliberate boundary. Avoid repeated `compute()` calls inside loops because they serialize work and prevent graph-level optimization.

## Memory

- Prefer streaming and partitioned output over collecting all results on the client.
- Use `persist()` only for intermediate data reused often enough to justify memory use.
- Inspect worker memory and spilling.
- Avoid object-dtype data where a compact numeric or categorical representation is possible.
- Release references to completed intermediates.

## Data movement

Joins, shuffles, index changes, and rechunking can move large amounts of data. Reduce data before shuffling, avoid unnecessary wide joins, and consider whether the operation belongs in a database or a specialized engine.

## Reliability

- Use idempotent tasks where possible.
- Make writes atomic or partition-safe.
- Bound retries and timeouts.
- Log task inputs and outputs without exposing secrets or sensitive records.
- Test failure and worker-restart behavior.

## Debugging

Begin with a small representative sample. Use the synchronous scheduler to simplify debugging, inspect task graphs when needed, and profile a production-sized run before tuning. Record Dask, Python, and dependency versions.

## Common anti-patterns

- Calling `compute()` on the full dataset.
- Creating very small partitions.
- Using distributed execution for tiny in-memory tasks.
- Repeatedly converting between pandas, NumPy, Bags, and DataFrames.
- Ignoring memory pressure and network transfer.
- Assuming all pandas or NumPy operations have identical Dask semantics.
