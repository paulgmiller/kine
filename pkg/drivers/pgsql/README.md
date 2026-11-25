## Endpoint Format

```
postgres://username:password@hostname:port/database-name
```

If you only supply `postgres://` as the endpoint, kine will attempt to do the following:
* Connect to localhost using `postgres` as the username and password
* Create a database named `kubernetes`

## CockroachDB isolation note

When used with CockroachDB, this PostgreSQL-compatible driver retains Kine's
generic SQL-log algorithm. Its append transaction explicitly uses **READ
COMMITTED**; maintenance operations may use stronger isolation. The separate,
experimental [`cockroach://` native driver](../cockroach/README.md) requires and
enforces **SERIALIZABLE** transactions for its per-resource counters, native MVCC
snapshots, and changefeed ordering. Do not transfer isolation tuning assumptions
between these two drivers.
