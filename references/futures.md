# Dask Futures and Distributed Tasks

The Futures API provides explicit control over task submission and asynchronous results. It is useful when the workflow is dynamic or does not map naturally to a single collection graph.

## Connect to a cluster

```python
from dask.distributed import Client

client = Client("tcp://scheduler:8786")
print(client.dashboard_link)
```

For local development, use `Client()` to start a local cluster. Close the client when the work is finished.

## Submit and gather

```python
future_a = client.submit(parse_file, "a.json")
future_b = client.submit(parse_file, "b.json")
results = client.gather([future_a, future_b])
```

Use `client.map` for a common function over many inputs. Prefer collections when the workload is a standard array, dataframe, or bag operation.

## Dependencies

A Future passed to another submitted task establishes a dependency:

```python
source = client.submit(load_data, path)
processed = client.submit(transform, source)
summary = client.submit(summarize, processed)
result = summary.result()
```

Do not call `.result()` too early if it would block the client while independent tasks could still be scheduled.

## Resilience and cancellation

Handle task exceptions explicitly. Use `future.cancel()` or `client.cancel()` when work is no longer needed. Design tasks to be idempotent because distributed systems can retry work after worker failures.

## Data locality and worker memory

Large values passed between tasks can create network traffic. Prefer passing small references or partitioned data. Use worker restrictions only when data locality or specialized hardware requires them, since hard restrictions reduce scheduling flexibility.

## Observability

Use the dashboard, task stream, worker memory plots, and logs to diagnose bottlenecks. Do not rely only on client-side elapsed time.
