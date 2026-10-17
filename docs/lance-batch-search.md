<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lance batch vector search

`vector_search` accepts independent queries through `query_vectors`, a non-empty
JSON matrix. Each row is one query against a `FixedSizeList<T, D>` vector column.
The output adds a non-null INT `query_index` starting at zero. Duplicate query
vectors retain separate indices. Each group returns at most `top_k` rows after
skipping its own `offset` rows.

```sql
SELECT query_index, id, _distance
FROM vector_search(
    "table" = "lance_catalog.db.items",
    "column" = "embedding",
    "query_vectors" = "[[1,2],[3,4],[1,2]]",
    "top_k" = "3",
    "offset" = "1",
    "nprobes" = "4"
)
ORDER BY query_index, _distance;
```

The TVF does not promise row ordering. At a tied Top-K boundary the selected rows
may differ, as with single-query search. For each query, candidate generation,
filter timing, index options and merging otherwise match an independent
`query_vector` request over the same snapshot.

## Filtering and execution

Each split retrieves `top_k + offset` candidates per query. A `WHERE` directly
on the batch TVF, optionally with a table alias, filters those candidates before
the local and global per-query merge. Rejected candidates are not replenished.
A derived-table or CTE consumer's `WHERE`, and JOIN `ON` or `WHERE` predicates,
filter the batch TVF's completed per-query offset/Top-K window instead. They do
not change which rows the window selects or replenish rejected rows.

For example, `FROM vector_search(...query_vectors...) AS v WHERE v.id > 0`
filters split candidates before the window. Wrapping the TVF as
`FROM (SELECT * FROM vector_search(...query_vectors...)) AS v WHERE v.id > 0`
completes the window before filtering. These schematic forms omit required
arguments and are not executed examples.

Analysis binds only a direct TVF WHERE below its grouped merge, matching
`GroupTopN(TVF)` or `Alias(GroupTopN(TVF))` while derived tables still retain a
Project and CTE consumers and joins retain their own nodes. Later rewrites do
not push predicates through GroupTopN.

A direct TVF WHERE that captures a column outside the TVF output is rejected
at analysis time. The guard checks both ordinary input slots and subquery
correlation slots. Use a derived table to filter the completed search results
when an outer column is needed. EXISTS, IN, and scalar subqueries correlated
only to the TVF's columns remain allowed.

The TVF `filter` property defaults to postfiltering; `prefilter=true` moves that
filter before candidate search.

The global grouped merge gathers all local candidates and applies offset per
query. Local partition-sort passthrough cannot bypass the final merge. Two-stage
reading keeps `query_index`, `_distance` and predicate columns in the first stage,
then fetches remaining projected columns by row ID.

Batch search preserves the single-query defaults. Shared IVF execution requires
an eligible non-HNSW index split, explicit `nprobes` and no `refine_factor`.
HNSW, omitted `nprobes`, refinement, and other ineligible index plans fall back
to per-query execution inside Lance. Flat splits use batched flat search.

`EXPLAIN` reports `lanceBatchQueries`, grouping keys, per-group limit and offset.
Plan and memo displays use a streaming matrix summary. Inputs of at most
`64 * 1024` characters can display `<N x D matrix>`; longer text or invalid shapes
show `<query matrix>`. This threshold only controls display. It does not limit
executable input or modify the original properties.

The batch TVF estimates `N * (top_k + offset)` candidate rows and `N` distinct
`query_index` values. Logical and global GroupTopN estimates are bounded by
`NDV * top_k` and the input-row bound after offset. Local GroupTopN retains its
child estimate because multiple drivers can each produce a window. Known empty
inputs remain empty, and prepared executions derive statistics from newly bound
parameters. These estimates are not actual result counts or a bound across splits.

Profile reports `LanceBatchQueryCount` as a scanner information string containing
N; do not sum it across scanner instances. `LanceBatchSharedIndexSplits`,
`LanceBatchPerQueryIndexSplits`, and `LanceBatchFlatSplits` count completed splits
and can be summed across instances. They are best-effort, based on Lance execution
metrics delivered at end of stream; cancellation, errors or early close can omit
them. Existing `LanceANNBatchExecTime` and prefilter, partition-load and take
counters show execution costs. Partition-load counters count load events, not
unique partitions across a query. Operator timers can overlap and must not be
summed to derive wall-clock query latency.

## Input and protocol limits

- Supported element types: Float16, Float32, Float64, UInt8 and Int8.
- Between 1 and 128 queries, all with the column's dimension and finite,
  representable numeric elements; encoded query bytes must not exceed 64 MiB.
- `S * N * (top_k + offset) * R` must not exceed 100,000, where `S` is the
  final scheduling split count and `R` is the explicitly supplied positive
  `refine_factor`, or `1` when unset. FE checks this after producing the final
  scheduling splits. FE analysis and each BE scanner also check the single-split
  budget. An empty snapshot has no splits and produces no candidates.
- `query_vector` and `query_vectors` are mutually exclusive.
- Batch search rejects multi-vector columns, distance bounds and datasets
  containing a `query_index` column. Single-query behavior is unchanged.
- Prepared statements support `query_vectors=?`. Each execution validates its
  bound matrix and budget; the property name determines the prepared schema.
- Batch requests use external-search schema version 2 and `num_queries`.
  Both group Top-N phases use the distinct `GROUP_TOPN_NODE` plan type while
  reusing the partition-sort payload and operators. Older backends reject this
  node even when they only merge results and do not receive a search request.
  Single and multi-vector queries remain version 1 and keep their existing plan types.

FE reads the matrix twice with a streaming JSON reader. The first pass validates
shape, dimensions, numeric tokens, query count and encoded bytes before allocating
the numeric buffer. The second pass validates representable values and encodes
them. Query-count and encoded-byte checks run as each row is reached, without
constructing a complete JSON tree.

The total candidate budget includes every final scheduling split and refinement.
It bounds candidate counts, not bytes, memory or internal concurrency. Local
passthrough can send all split candidates to the final grouped merge. Neither the
candidate budget nor the final `N * top_k` row limit bounds partition-sort bytes
or native working memory.

`query_parallelism` controls part of partition loading in Lance's per-query path.
The shared batch path does not read it. This option does not bound concurrency
across an entire batch or multiple scanners.

Batch scans use Lance's polling API. While a poll is pending, the reader checks
query cancellation and scanner stop state at 10 ms wait intervals, then closes the
scanner on the scanning thread. The waker context remains alive until close has
retired its callbacks. The first poll still builds the stream synchronously;
metadata access and stream construction cannot be interrupted by this wait loop.

Lance allocations remain outside Doris memory accounting. Closing a scanner drops
its stream but does not synchronously join every native task: already dispatched
CPU work can finish after close. An indexed query with `prefilter=true` and a
predicate without a scalar index, such as `id % 2 = 0`, can leave a detached
prefilter task scanning its full input after close. A larger batch can therefore
increase peak memory and cancellation latency. Fragment recovery or a
successful later query alone does not prove that all native tasks and allocations
have been released.

## Regression fixtures

`docker/thirdparties/docker-compose/iceberg/scripts/lance_build_batch_search.py`
uses pylance 13.0.0b18 to generate two independently trained index segments covering
eight fragments, an uncovered ninth fragment, and deleted rows. It creates IVF_FLAT,
IVF_HNSW_FLAT and IVF_HNSW_SQ variants. Strict row comparisons use FLAT distances;
SQ comparisons allow tied rows to differ and verify distances and row contents.
The catalog builder carries these fixtures forward because
its general writer remains pinned to pylance 7.0.0.
