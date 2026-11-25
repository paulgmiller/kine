# Experimental native CockroachDB driver

`cockroach://` uses native CockroachDB serializable transactions, MVCC history,
and sinkless SQL changefeeds. It targets **CockroachDB v26.2.x** and fresh
databases. It does not migrate the existing PostgreSQL-compatible Kine schema.

The maintained [design issue register](../../../docs/cockroach-native-issues.md)
records limitations, observed failures, workarounds, and remaining validation,
including problems that already have mitigations.

```sh
cockroach sql --url "$ADMIN_DATABASE_URL" \
  -e 'SET CLUSTER SETTING kv.rangefeed.enabled = true'
kine --endpoint 'cockroach://user:password@host:26257/kubernetes?sslmode=verify-full'
```

The connection URL otherwise accepts pgx PostgreSQL connection options. Existing
Kine backend TLS flags are supported. The configured database is created if it
is missing and the user has permission. The user must own the driver tables and
have permission to configure their GC zones and run changefeeds. No broker is
required. Watch connections are separate from the request connection pool.

## Transaction isolation: different from `postgres://`

**The native `cockroach://` driver requires `SERIALIZABLE`.** This is a deliberate
departure from running the existing PostgreSQL-compatible Kine driver against
CockroachDB. That driver's SQL-log append path explicitly uses
`READ COMMITTED`; its storage algorithm and polling/gap handling are different.
Its maintenance transactions can use stronger isolation. The native driver does
not reuse that append path or inherit its isolation assumptions.

Native mutation transactions explicitly select `SERIALIZABLE` and atomically
check the expected object revision, increment the resource counter, mutate the
object, and insert its revision anchor. Serializable ordering is a correctness
requirement for the counter/anchor mapping and contiguous watch delivery.
SQLSTATE `40001` retries the complete transaction with bounded backoff; ambiguous
commit/connection errors are returned rather than blindly replayed as new writes.

Current reads, lists, and counts use serializable read-only transactions;
historical reads use serializable read-only `AS OF SYSTEM TIME` snapshots. The
native driver's SQL connection default is also forced to `serializable`, including
changefeed connections, even if the URL or server default requests read committed.
Do not lower isolation to reduce contention: the supported boundary for write
independence is the resource counter, and changing isolation requires a new
correctness argument. Existing `postgres://` behavior is unchanged.

## Revision contract

This is a Kubernetes storage backend with **resource-scoped revisions**, not a
general etcd implementation. By default, the first component below `/registry/`
defines the resource domain. `/registry/pods/default/a` and
`/registry/pods/other/b` share a counter; deployments have an independent counter.
Each domain starts at revision 1. Cross-domain ranges and transactions are
rejected. Revisions must never be compared across resources.

| URL option | Default | Meaning |
| --- | --- | --- |
| `registry-prefix` | `/registry` | Prefix above resource storage prefixes; `/` supports root-level storage prefixes such as `/pods/` in the upstream test fixture. |
| `history-retention` | `1h` | Logical history window before revisions expire. |
| `gc-retention` | `2h` | CockroachDB table MVCC retention; must be at least twice the logical window. |

All instances sharing a database must use identical registry and retention
settings. Health, K3s `/bootstrap` records, non-path bookkeeping keys, and
Kubernetes compactor metadata use separate internal domains. K3s should use the
default `/registry` prefix so its Kubernetes resources remain independent.
The scoped server capability is optional; existing Kine drivers retain their
global revision semantics.
Registry and retention settings are fixed when the database is initialized;
changing them requires a coordinated administrative migration rather than just
changing one instance's URL. Databases containing the legacy `kine` SQL-log table
are rejected so switching endpoint schemes cannot silently hide existing objects.

## Range distribution

| Table/index | Ordered key | Placement implications |
| --- | --- | --- |
| `kine_crdb_objects` primary | `(domain, key)` | Objects of a resource are adjacent; namespace/name ordering supports prefix lists and continuation scans. A large resource can span multiple ranges. |
| `kine_crdb_anchors` primary | `(domain, revision)` | History metadata is grouped by resource, with sequential writes at that resource's right edge. |
| `kine_crdb_domains` primary | `domain` | One counter row per resource; all writes to that resource serialize on this row. |
| Object expiry index | `expires_at` | TTL workers scan due deadlines independently of resource ordering. |

CockroachDB distributes contiguous index key spans as ranges, splitting them as
data size and load warrant and placing their replicas and leaseholders according
to database policy. See its [distribution architecture](https://docs.cockroachlabs.com/docs/stable/architecture/distribution-layer)
and [load-based splitting](https://docs.cockroachlabs.com/docs/stable/load-based-splitting).

The driver currently adds **no hash buckets, explicit splits, scatter commands,
table partitions, locality rules, or dedicated node assignments**. A revision
domain is not a physical range boundary. Multiple resources' counters or object
spans can initially occupy the same range; separate counters avoid a shared SQL
row lock, but do not guarantee separate Raft groups or physical capacity. A hot
resource still has one counter bottleneck, and its sequential anchor writes can
hotspot their range. Namespaces do not introduce additional counters or explicit
range boundaries.

Inspect the actual layout with the following queries; repeat for anchors and
domains when evaluating contention. A multi-node load test is needed to establish
physical scaling; single-node correctness tests cannot establish distribution.

```sql
SHOW RANGES FROM TABLE kine_crdb_objects WITH DETAILS;
SHOW RANGES FROM TABLE kine_crdb_anchors WITH DETAILS;
SHOW RANGES FROM TABLE kine_crdb_domains WITH DETAILS;
```

## Snapshots, watches, and expiration

Each mutation atomically updates its resource counter and object and inserts a
small immutable revision anchor. The anchor's committed MVCC timestamp maps the
integer revision to an exact historical snapshot. Object history is retained
only by CockroachDB, not copied into a Kine event log. Empty or idle resources can
still be read at their current revision after their last mutation ages out.

Watches consume object changes and revision anchors with before-images and
correlate physical deletes with deletion revisions. The counter baseline is read
at the exact feed cursor. Complete object/anchor pairs are emitted in contiguous
resource-revision order, so a slow resolved frontier does not delay delivery.
Resolved frontiers establish recovery checkpoints and detect incomplete input.
Replays begin immediately before the requested revision's
commit timestamp and join buffered live events with deduplication. Buffers are
bounded; lagging or disconnected consumers are canceled and must resume/relist.
Sinkless feeds are not assumed to pin history indefinitely.
The object and anchor tables use `schema_locked=true` to avoid per-row descriptor
refresh delays; their schemas are driver-owned and fixed in this experimental
version. A future schema migration must explicitly unlock and coordinate changes.
Changefeed connections disable pgwire result buffering for prompt notifications.

CockroachDB GC reclaims MVCC history. The driver expires obsolete anchor metadata
and reports expired history as the standard etcd compacted error. Explicit etcd
compaction requests are acknowledged without triggering GC. Kubernetes' global
compactor bookkeeping advertises revision/value zero, so a resource counter
cannot invalidate unrelated caches. Leases use persisted expiration deadlines
and compare-and-swap deletes, including after restart; expiration is checked
every second in batches of up to 1,000 keys.

## Validation

```sh
KINE_COCKROACH_TEST_DSN='postgres://root@localhost:26257/defaultdb?sslmode=disable' \
  go test -race -count=1 -timeout=5m ./pkg/drivers/cockroach ./pkg/endpoint
./scripts/test cockroach-native
```

Integration tests create and drop isolated databases. The native script runs the
repo's upstream Kubernetes storage suite with both direct and split readers and
writers, followed by native integration tests and the existing K3s/load harness.
It uses separate test binaries so the normal global-revision suites are unchanged.
The upstream `increaseRV` helper writes the resource's `/pods` sentinel instead
of an unrelated global key. Six tests and the `TestWatch/WatchFromZero` subtest
that explicitly require imperative global compaction are excluded by name in
the script; native retention/TTL tests cover
the replacement contract. Assertions within the selected upstream tests remain
unchanged.

Metrics include mutation latency, transaction retries, resolved-frontier lag,
pending changefeed bytes, stream reconnects, and slow-watch cancellation under
the `kine_cockroach_` prefix. This driver remains opt-in and experimental.
