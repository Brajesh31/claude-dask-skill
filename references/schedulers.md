# Dask Schedulers

Dask can execute task graphs with different schedulers. Choose based on the workload, libraries involved, and deployment model.

## Common choices

| Scheduler | Best fit | Trade-offs |
|---|---|---|
| Synchronous | Debugging and small graphs | No parallelism |
| Threaded | NumPy, pandas, and native code that releases the GIL | Python-heavy code may not scale across threads |
| Multiprocessing | CPU-heavy Python code that benefits from separate processes | Serialization and process startup costs |
| Distributed | Multi-process, multi-machine, dashboard, Futures, and cluster execution | Requires resource and cluster management |

## Select intentionally

Use a local synchronous scheduler when diagnosing exceptions and task dependencies. Use threads for numerical libraries that release the GIL. Use processes for Python CPU-bound work when serialization costs are acceptable. Use Distributed when you need explicit worker management, the dashboard, Futures, or a remote cluster.

## Configure a local distributed cluster

```python
from dask.distributed import Client, LocalCluster

cluster = LocalCluster(
    n_workers=4,
    threads_per_worker=1,
    memory_limit="4 GiB",
)
client = Client(cluster)
```

Tune workers, threads, and memory to actual machine resources. Do not allocate all memory to Dask because Python, the operating system, and temporary objects also require headroom.

## Avoid oversubscription

Numerical libraries may start their own thread pools. Combining many Dask workers with many BLAS threads can oversubscribe CPUs. Measure and configure thread counts consistently.

## Cluster operations

Set worker resource limits, authentication, network access, and data access deliberately. Protect the dashboard and scheduler endpoints. Use the dashboard to watch task throughput, transfer volume, memory, and worker utilization.

## Debugging and validation

Start with fewer workers and a small dataset. Compare results with a trusted local implementation. Increase parallelism only after correctness and memory behavior are understood.
