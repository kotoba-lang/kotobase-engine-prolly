# kotobase-engine-prolly

An independent `kotobase-engine-contract/IEngine` implementation backed by
`arrangement` Prolly snapshots. It does not depend on `kotobase-peer`.

Logical Datom values are encoded into canonical EDN strings at the Arrangement
boundary and decoded on reads. This preserves keywords, numbers, booleans,
vectors, sets and maps while keeping the generic Arrangement primitive's
string-only physical contract.

Transactions use Arrangement `commit-changes!`, which coalesces assertions and
retractions into one deterministic Prolly mutation per index. Retractions are
real key deletion rather than tombstones: a deleted boundary re-chunks its leaf
and successor, while mixed additions/removals rebuild internal levels once.
The result is CID-identical to a full rebuild of the same logical graph.

The engine writes a small content-addressed manifest above each Arrangement
snapshot. The manifest persists transaction history, idempotency records and
epoch-to-snapshot roots, so `restore-state` works from object storage without
the process-local state.

`kotobase.engine.prolly.provider` adapts any JVM `kotobase-storage` backend to
the engine and publishes a manifest only through `IRefStore` compare-and-set.
A losing writer leaves harmless immutable blocks but cannot overwrite the
winner. S3-compatible providers must still pass `kotobase-storage`'s own CAS,
content-verification and concurrency suites before qualification.

The ClojureScript coordinator batches synchronously generated immutable blocks,
awaits the provider upload, and then performs CAS. Reopen fetches only the
manifest; reads use a direct async, range-pruned Prolly cursor with bounded
concurrency. The regression harness reads one entity from a 2,001-Datom,
40-block snapshot using 4 block requests total. The remaining performance
blocker is cold-writer prefetch: the current async mutation warms every leaf
summary before changing the affected paths.

Dependencies are fixed to published Git commit SHAs. West registration should
advance only to reviewed revisions; generated manifests are not edited with
invented SHAs.

```sh
clojure -M:test
clojure -M:lint
```
