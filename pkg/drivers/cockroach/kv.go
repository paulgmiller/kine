package cockroach

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/k3s-io/kine/pkg/server"
)

var hlcPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

func translate(err error) error {
	if err == nil {
		return nil
	}
	// SQLSTATE 72000 is snapshot_too_old; older releases also surface the
	// GC threshold as an XX000 error. Do not translate unrelated SQL errors.
	var pe *pgconn.PgError
	if errors.As(err, &pe) && (pe.Code == "72000" || strings.Contains(pe.Message, "must be after replica GC threshold") || strings.Contains(pe.Message, "must be after GC threshold")) {
		return server.ErrCompacted
	}
	return err
}

func (b *backend) retry(ctx context.Context, fn func(pgx.Tx) error) error {
	for attempt := 0; ; attempt++ {
		tx, err := b.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			err = tx.Commit(ctx)
		}
		_ = tx.Rollback(ctx)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "40001" || attempt >= 20 {
			return translate(err)
		}
		b.metrics.WithLabelValues("transaction_retry").Inc()
		delay := time.Duration(1<<min(attempt, 7)) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

type scanner interface{ Scan(...any) error }

const columns = `key,value,create_revision,mod_revision,version,lease`

func scanKV(row scanner) (*server.KeyValue, error) {
	kv := &server.KeyValue{}
	var key []byte
	err := row.Scan(&key, &kv.Value, &kv.CreateRevision, &kv.ModRevision, &kv.Version, &kv.Lease)
	kv.Key = string(key)
	return kv, err
}

func (b *backend) mutate(ctx context.Context, key string, value []byte, expected, lease int64, op string) (rev int64, kv *server.KeyValue, ok bool, err error) {
	start := time.Now()
	defer func() { b.latency.WithLabelValues(op).Observe(time.Since(start).Seconds()) }()
	domain, err := b.resource(key, "")
	if err != nil {
		return 0, nil, false, err
	}
	if err = b.ensure(ctx, domain); err != nil {
		return 0, nil, false, err
	}
	if value == nil {
		value = []byte{}
	}
	err = b.retry(ctx, func(tx pgx.Tx) error {
		ok, kv = false, nil
		if err := tx.QueryRow(ctx, `SELECT revision FROM kine_crdb_domains WHERE domain=$1 FOR UPDATE`, domain).Scan(&rev); err != nil {
			return err
		}
		old, err := scanKV(tx.QueryRow(ctx, `SELECT `+columns+` FROM kine_crdb_objects WHERE domain=$1 AND key=$2`, domain, []byte(key)))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		exists := err == nil
		if op == "create" && exists {
			return server.ErrKeyExists
		}
		if op != "create" && (!exists || (expected != 0 && old.ModRevision != expected) || (op == "update" && old.ModRevision != expected)) {
			if exists {
				kv = old
			}
			return nil
		}
		if rev == int64(^uint64(0)>>1) {
			return errors.New("resource revision exhausted")
		}
		rev++
		if _, err := tx.Exec(ctx, `UPDATE kine_crdb_domains SET revision=$2 WHERE domain=$1`, domain, rev); err != nil {
			return err
		}
		if op == "delete" {
			kv = old
			_, err = tx.Exec(ctx, `DELETE FROM kine_crdb_objects WHERE domain=$1 AND key=$2`, domain, []byte(key))
		} else {
			create, version := rev, int64(1)
			if exists {
				create, version = old.CreateRevision, old.Version+1
			}
			_, err = tx.Exec(ctx, `UPSERT INTO kine_crdb_objects(domain,key,value,create_revision,mod_revision,version,lease,expires_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,CASE WHEN $7::INT8>0 THEN now()+$7*INTERVAL '1 second' ELSE NULL END)`, domain, []byte(key), value, create, rev, version, lease)
			kv = &server.KeyValue{Key: key, Value: value, CreateRevision: create, ModRevision: rev, Version: version, Lease: lease}
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO kine_crdb_anchors(domain,revision,key,operation) VALUES ($1,$2,$3,$4)`, domain, rev, []byte(key), op)
		ok = err == nil
		return err
	})
	if err != nil {
		return rev, nil, false, err
	}
	if ok {
		b.metrics.WithLabelValues(op).Inc()
	}
	return rev, kv, ok, nil
}

func (b *backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	rev, _, _, err := b.mutate(ctx, key, value, 0, lease, "create")
	return rev, err
}
func (b *backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	return b.mutate(ctx, key, value, revision, lease, "update")
}
func (b *backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	return b.mutate(ctx, key, nil, revision, 0, "delete")
}

// anchorTime must be called after the writing transaction has committed. The
// hidden MVCC timestamp inside that transaction may still be provisional.
func (b *backend) anchorTime(ctx context.Context, revision int64) (string, error) {
	var timestamp string
	err := b.pool.QueryRow(ctx, `SELECT crdb_internal_mvcc_timestamp::STRING FROM kine_crdb_anchors WHERE domain=$1 AND revision=$2`, b.domain, revision).Scan(&timestamp)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", server.ErrCompacted
	}
	if err != nil {
		return "", translate(err)
	}
	if !hlcPattern.MatchString(timestamp) {
		return "", errors.New("invalid database HLC timestamp")
	}
	return timestamp, nil
}

// snapshot returns a transaction pinned to exactly one resource revision. For
// an idle resource the current state can be read now, even if its last mutation
// predates the GC window. Later mutations map back to the exact anchor time.
func (b *backend) snapshot(ctx context.Context, requested int64) (pgx.Tx, int64, error) {
	tx, err := b.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, err
	}
	var current, compact int64
	err = tx.QueryRow(ctx, `SELECT revision,compact_revision FROM kine_crdb_domains WHERE domain=$1`, b.domain).Scan(&current, &compact)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, 0, err
	}
	if requested > current {
		_ = tx.Rollback(ctx)
		return nil, current, server.ErrFutureRev
	}
	if requested <= 0 || requested == current {
		return tx, current, nil
	}
	_ = tx.Rollback(ctx)
	if requested <= compact {
		return nil, current, server.ErrCompacted
	}
	timestamp, err := b.anchorTime(ctx, requested)
	if err != nil {
		return nil, current, err
	}
	// Timestamp interpolation is restricted to a decimal emitted by CockroachDB.
	tx, err = b.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, current, err
	}
	if _, err = tx.Exec(ctx, `SET TRANSACTION AS OF SYSTEM TIME `+timestamp); err != nil {
		_ = tx.Rollback(ctx)
		return nil, current, translate(err)
	}
	return tx, current, nil
}

func (b *backend) readScope(ctx context.Context, key, end string) (*backend, error) {
	d, err := b.resource(key, end)
	if err != nil {
		return nil, err
	}
	if err := b.ensure(ctx, d); err != nil {
		return nil, err
	}
	return &backend{store: b.store, domain: d}, nil
}

func (b *backend) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	rev, kvs, err := b.List(ctx, key, "", 1, revision, keysOnly)
	if err != nil || len(kvs) == 0 {
		return rev, nil, err
	}
	return rev, kvs[0], nil
}

func rangeSQL(key, end string) (string, []any) {
	if end == "" {
		return "key=$2", []any{[]byte(key)}
	}
	return "key>=$2 AND key<$3", []any{[]byte(key), []byte(end)}
}

func (b *backend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	b, err := b.readScope(ctx, key, end)
	if err != nil {
		return 0, nil, err
	}
	tx, current, err := b.snapshot(ctx, revision)
	if err != nil {
		return current, nil, err
	}
	defer tx.Rollback(ctx)
	where, args := rangeSQL(key, end)
	args = append([]any{b.domain}, args...)
	cols := columns
	if keysOnly {
		cols = `key,''::BYTES,create_revision,mod_revision,version,lease`
	}
	query := `SELECT ` + cols + ` FROM kine_crdb_objects WHERE domain=$1 AND ` + where + ` ORDER BY key`
	if limit > 0 {
		args = append(args, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return current, nil, translate(err)
	}
	defer rows.Close()
	var kvs []*server.KeyValue
	for rows.Next() {
		kv, err := scanKV(rows)
		if err != nil {
			return current, nil, err
		}
		kvs = append(kvs, kv)
	}
	return current, kvs, translate(rows.Err())
}

func (b *backend) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	b, err := b.readScope(ctx, key, end)
	if err != nil {
		return 0, 0, err
	}
	tx, current, err := b.snapshot(ctx, revision)
	if err != nil {
		return current, 0, err
	}
	defer tx.Rollback(ctx)
	where, args := rangeSQL(key, end)
	var count int64
	err = tx.QueryRow(ctx, `SELECT count(*) FROM kine_crdb_objects WHERE domain=$1 AND `+where, append([]any{b.domain}, args...)...).Scan(&count)
	return current, count, translate(err)
}

func (b *backend) ListStream(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) server.ListResult {
	kvc, errc := make(chan *server.KeyValue), make(chan error, 1)
	fail := func(rev int64, err error) server.ListResult {
		errc <- translate(err)
		close(kvc)
		close(errc)
		return server.ListResult{KVc: kvc, Errorc: errc, CurrentRevision: rev}
	}
	b, err := b.readScope(ctx, key, end)
	if err != nil {
		return fail(0, err)
	}
	tx, rev, err := b.snapshot(ctx, revision)
	if err != nil {
		return fail(rev, err)
	}
	where, args := rangeSQL(key, end)
	args = append([]any{b.domain}, args...)
	cols := columns
	if keysOnly {
		cols = `key,''::BYTES,create_revision,mod_revision,version,lease`
	}
	query := `SELECT ` + cols + ` FROM kine_crdb_objects WHERE domain=$1 AND ` + where + ` ORDER BY key`
	if limit > 0 {
		args = append(args, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fail(rev, err)
	}
	go func() {
		defer tx.Rollback(ctx)
		defer rows.Close()
		defer close(kvc)
		defer close(errc)
		for rows.Next() {
			kv, err := scanKV(rows)
			if err != nil {
				errc <- err
				return
			}
			select {
			case kvc <- kv:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
		if err := rows.Err(); err != nil {
			errc <- translate(err)
		}
	}()
	return server.ListResult{KVc: kvc, Errorc: errc, CurrentRevision: rev}
}
