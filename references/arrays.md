# Dask Arrays

Dask Arrays provide blocked, parallel operations over NumPy-like arrays. A Dask Array is split into chunks, and operations build a task graph that is computed later.

## When to use them

Use Dask Arrays when data is too large for memory, operations can be performed on independent blocks, or work can be distributed across cores or machines. Use NumPy directly when the data comfortably fits in memory and parallel scheduling would add more overhead than value.

## Read and inspect

```python
import dask.array as da

x = da.from_array(numpy_array, chunks=(10_000, 1_000))
print(x.shape, x.dtype, x.chunks)
```

Choose chunks large enough to amortize task overhead but small enough that several chunks fit in worker memory. Chunk shape should align with the operations performed most often.

## Common operations

```python
import dask.array as da

x = da.random.random((100_000, 100), chunks=(10_000, 100))
row_means = x.mean(axis=1)
result = row_means.compute()
```

Most familiar NumPy operations are available, but some operations require communication or rechunking. Check the resulting chunk structure after reshape, transpose, slicing, or reductions.

## Chunking guidance

- Start with chunks around tens to hundreds of megabytes, then benchmark.
- Avoid thousands of tiny chunks.
- Avoid chunks that are larger than worker memory.
- Keep chunks aligned across arrays that participate in elementwise operations.
- Rechunk only when the downstream computation benefits enough to justify data movement.

## Reductions and persistence

Reductions such as sum, mean, and standard deviation are naturally parallelizable. Call `compute()` only when the final result fits in client memory. Use `persist()` when an intermediate is expensive and reused several times, subject to available cluster memory.

## Common pitfalls

- Calling `numpy.asarray` or `compute()` on a huge array materializes it locally.
- Rechunking can trigger substantial network transfer.
- Random chunk sizes can cause incompatible block layouts.
- A task graph with excessive tiny tasks can become scheduler-bound.

Profile representative workloads before changing chunk geometry.
