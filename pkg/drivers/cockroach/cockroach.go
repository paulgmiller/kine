// Package cockroach implements resource-scoped Kubernetes storage using native
// CockroachDB transactions, MVCC history and sinkless changefeeds.
package cockroach

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

func init() { drivers.Register("cockroach", New) }

type store struct {
	pool       *pgxpool.Pool
	connConfig *pgx.ConnConfig
	wg         *sync.WaitGroup
	ctx        context.Context
	registry   string
	history    time.Duration
	gc         time.Duration
	mu         sync.Mutex
	subs       map[*subscription]struct{}
	published  map[string]int64
	available  bool
	metrics    *prometheus.CounterVec
	instance   string
	latency    *prometheus.HistogramVec
	watchLag   prometheus.Gauge
	buffered   prometheus.Gauge
	domains    sync.Map
}

type backend struct {
	*store
	domain string
}

var _ server.Backend = (*backend)(nil)
var _ server.ScopedBackend = (*backend)(nil)

// New opens the explicitly selected, experimental CockroachDB-native driver.
// Like the SQL drivers, it creates a missing database when privileges permit.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	u, err := url.Parse("postgres://" + cfg.DataSourceName)
	if err != nil {
		return false, nil, errors.New("invalid cockroach connection URL")
	}
	q := u.Query()
	registry := q.Get("registry-prefix")
	if registry == "" {
		registry = "/registry"
	}
	if !strings.HasPrefix(registry, "/") {
		return false, nil, errors.New("registry-prefix must be an absolute prefix")
	}
	history, gc := time.Hour, 2*time.Hour
	for key, dst := range map[string]*time.Duration{"history-retention": &history, "gc-retention": &gc} {
		if v := q.Get(key); v != "" {
			*dst, err = time.ParseDuration(v)
			if err != nil || *dst < time.Second {
				return false, nil, fmt.Errorf("%s must be a duration of at least 1s", key)
			}
		}
		q.Del(key)
	}
	if gc < 2*history {
		return false, nil, errors.New("gc-retention must be at least twice history-retention")
	}
	q.Del("registry-prefix")
	u.RawQuery = q.Encode()
	pc, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return false, nil, errors.New("invalid cockroach connection configuration")
	}
	// Native counter/anchor ordering requires serializable isolation even
	// when an operator or connection URL selects a weaker SQL default.
	pc.ConnConfig.RuntimeParams["default_transaction_isolation"] = "serializable"
	tls, err := cfg.BackendTLSConfig.ClientConfig()
	if err != nil {
		return false, nil, err
	}
	if tls != nil {
		if tls.ServerName == "" {
			tls.ServerName = pc.ConnConfig.Host
		}
		pc.ConnConfig.TLSConfig = tls
		pc.ConnConfig.Fallbacks = nil
	}
	if cfg.ConnectionPoolConfig.MaxOpen > 0 {
		pc.MaxConns = int32(cfg.ConnectionPoolConfig.MaxOpen)
	}
	pc.MaxConnLifetime = cfg.ConnectionPoolConfig.MaxLifetime
	if cfg.ConnectionPoolConfig.MaxIdleTime > 0 {
		pc.MaxConnIdleTime = cfg.ConnectionPoolConfig.MaxIdleTime
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return false, nil, err
	}
	s := &store{pool: pool, connConfig: pc.ConnConfig.Copy(), wg: wg, ctx: ctx,
		registry: strings.TrimRight(registry, "/"), history: history, gc: gc, subs: make(map[*subscription]struct{}), published: make(map[string]int64), instance: "kine-native-" + uuid.NewString()}
	s.metrics = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kine_cockroach_events_total", Help: "Native CockroachDB driver operations and stream events."}, []string{"operation"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "kine_cockroach_operation_seconds", Help: "Native CockroachDB mutation latency including transaction retries.", Buckets: prometheus.DefBuckets}, []string{"operation"})
	s.watchLag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "kine_cockroach_watch_lag_seconds", Help: "Age of the last resolved live changefeed frontier."})
	s.buffered = prometheus.NewGauge(prometheus.GaugeOpts{Name: "kine_cockroach_pending_bytes", Help: "Live changefeed bytes waiting for complete, contiguous resource revisions."})
	if err := s.setup(ctx); err != nil {
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || (pe.Code != "3D000" && !(pe.Code == "42602" && pe.Message == "no database or schema specified")) {
			pool.Close()
			return false, nil, err
		}
		adminConfig := pc.ConnConfig.Copy()
		adminConfig.Database = "defaultdb"
		admin, openErr := pgx.ConnectConfig(ctx, adminConfig)
		if openErr != nil {
			pool.Close()
			return false, nil, openErr
		}
		_, createErr := admin.Exec(ctx, `CREATE DATABASE IF NOT EXISTS `+pgx.Identifier{pc.ConnConfig.Database}.Sanitize())
		_ = admin.Close(ctx)
		if createErr != nil {
			pool.Close()
			return false, nil, createErr
		}
		if err := s.setup(ctx); err != nil {
			pool.Close()
			return false, nil, err
		}
	}
	if cfg.MetricsRegisterer != nil {
		var registered []prometheus.Collector
		for _, collector := range []prometheus.Collector{s.metrics, s.latency, s.watchLag, s.buffered} {
			if err := cfg.MetricsRegisterer.Register(collector); err != nil {
				for _, c := range registered {
					cfg.MetricsRegisterer.Unregister(c)
				}
				pool.Close()
				return false, nil, err
			}
			registered = append(registered, collector)
		}
	}
	wg.Go(func() { <-ctx.Done(); pool.Close() })
	return true, &backend{store: s}, nil
}

func (s *store) setup(ctx context.Context) error {
	var version string
	if err := s.pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return err
	}
	if !strings.Contains(version, "CockroachDB") || !strings.Contains(version, "v26.2.") {
		return errors.New("experimental cockroach driver requires CockroachDB v26.2.x")
	}
	var enabled bool
	if err := s.pool.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		return errors.New("cockroach driver requires kv.rangefeed.enabled=true")
	}
	var legacy bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('kine') IS NOT NULL`).Scan(&legacy); err != nil {
		return err
	}
	if legacy {
		return errors.New("native cockroach driver cannot open an existing kine SQL-log database; migration is not supported")
	}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS kine_crdb_settings (id INT PRIMARY KEY CHECK (id=1), registry STRING NOT NULL, history_seconds FLOAT8 NOT NULL, gc_seconds FLOAT8 NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS kine_crdb_domains (domain STRING PRIMARY KEY, revision INT8 NOT NULL, compact_revision INT8 NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS kine_crdb_objects (domain STRING NOT NULL, key BYTES NOT NULL, value BYTES NOT NULL, create_revision INT8 NOT NULL, mod_revision INT8 NOT NULL, version INT8 NOT NULL, lease INT8 NOT NULL, expires_at TIMESTAMPTZ, PRIMARY KEY (domain,key))`,
		`CREATE INDEX IF NOT EXISTS kine_crdb_expiry ON kine_crdb_objects (expires_at) WHERE expires_at IS NOT NULL`,
		`CREATE TABLE IF NOT EXISTS kine_crdb_anchors (domain STRING NOT NULL, revision INT8 NOT NULL, key BYTES NOT NULL, operation STRING NOT NULL, PRIMARY KEY (domain,revision))`,
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO kine_crdb_settings VALUES (1,$1,$2,$3) ON CONFLICT DO NOTHING`, s.registry, s.history.Seconds(), s.gc.Seconds()); err != nil {
		return err
	}
	var registry string
	var history, gc float64
	if err := s.pool.QueryRow(ctx, `SELECT registry,history_seconds,gc_seconds FROM kine_crdb_settings WHERE id=1`).Scan(&registry, &history, &gc); err != nil {
		return err
	}
	if registry != s.registry || history != s.history.Seconds() || gc != s.gc.Seconds() {
		return errors.New("cockroach instances sharing a database must use identical registry and retention settings")
	}
	for _, table := range []string{"kine_crdb_domains", "kine_crdb_objects", "kine_crdb_anchors"} {
		stmt := fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = %d", table, int64(s.gc/time.Second))
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	// These are driver-owned, fixed-schema tables. Without this guarantee
	// CDC waits for a descriptor refresh (one second by default) before
	// emitting fresh rows. No cluster-wide poll setting needs to change.
	for _, table := range []string{"kine_crdb_objects", "kine_crdb_anchors"} {
		if _, err := s.pool.Exec(ctx, `ALTER TABLE `+table+` SET (schema_locked=true)`); err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) Start(ctx context.Context) error {
	if b.domain != "" {
		return nil
	}
	var cursor string
	if err := b.pool.QueryRow(ctx, "SELECT cluster_logical_timestamp()::STRING").Scan(&cursor); err != nil {
		return err
	}
	ready := make(chan error, 1)
	b.wg.Go(func() { b.live(ctx, cursor, ready) })
	_, err := b.Create(ctx, server.HealthKey, []byte(server.HealthVal), 0)
	if errors.Is(err, server.ErrKeyExists) {
		err = nil
	}
	if err != nil {
		return err
	}
	select {
	case err := <-ready:
		if err != nil {
			return fmt.Errorf("starting native changefeed: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return errors.New("native changefeed did not reach an initial resolved frontier within 30s")
	}
	b.wg.Go(func() { b.maintenance(ctx) })
	return nil
}

// resource identifies the actual storage prefix, not API group/version names.
// Aliases sharing a Kubernetes storage prefix consequently share a counter.
func (b *backend) resource(key, end string) (string, error) {
	// K3s stores bootstrap records outside the Kubernetes registry. This
	// reserved prefix has its own counter and never shares resource revisions.
	if strings.HasPrefix(key, "/bootstrap") {
		if (end != "" && (end <= key || end > "/bootstraq")) || (b.domain != "" && b.domain != "@bootstrap") {
			return "", server.ErrNotSupported
		}
		return "@bootstrap", nil
	}
	// Non-path bookkeeping keys occupy one separate internal domain. This
	// also supports prefix queries on such keys without inventing revisions
	// comparable to any Kubernetes resource.
	if key != "" && !strings.HasPrefix(key, "/") && key != "\x00" && key != "compact_rev_key" && key != "compact_rev_key_apiserver" {
		if (end != "" && (end <= key || strings.HasPrefix(end, "/"))) || (b.domain != "" && b.domain != "@internal") {
			return "", server.ErrNotSupported
		}
		return "@internal", nil
	}
	if key == server.HealthKey {
		if end != "" || (b.domain != "" && b.domain != "@health") {
			return "", server.ErrNotSupported
		}
		return "@health", nil
	}
	if key == "compact_rev_key" || key == "compact_rev_key_apiserver" {
		if end != "" || (b.domain != "" && b.domain != "@compact") {
			return "", server.ErrNotSupported
		}
		return "@compact", nil
	}
	prefix := b.registry + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", server.ErrNotSupported
	}
	resource, _, _ := strings.Cut(strings.TrimPrefix(key, prefix), "/")
	if resource == "" || strings.ContainsAny(resource, "\x00\x01") {
		return "", server.ErrNotSupported
	}
	domain := prefix + resource
	if end != "" {
		// The exclusive upper bound of domain+"/" is domain+"0".
		upper := domain + "0"
		if end == "\x00" || end > upper || end <= key {
			return "", server.ErrNotSupported
		}
	}
	if b.domain != "" && b.domain != domain {
		return "", server.ErrNotSupported
	}
	return domain, nil
}

func (b *backend) Scope(ctx context.Context, key, end string) (server.Backend, string, error) {
	domain, err := b.resource(key, end)
	if err != nil {
		return nil, "", err
	}
	if err := b.ensure(ctx, domain); err != nil {
		return nil, "", err
	}
	return &backend{store: b.store, domain: domain}, domain, nil
}

func (b *backend) ensure(ctx context.Context, domain string) error {
	if _, ok := b.domains.Load(domain); ok {
		return nil
	}
	// The anchor and domain must be created together, including for an empty
	// resource first encountered by a list or watch.
	err := b.retry(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO kine_crdb_domains(domain,revision) VALUES ($1,1) ON CONFLICT DO NOTHING`, domain)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO kine_crdb_anchors(domain,revision,key,operation) VALUES ($1,1,''::BYTES,'init')`, domain)
		return err
	})
	if err == nil {
		b.domains.Store(domain, struct{}{})
	}
	return err
}

func (b *backend) CurrentRevision(ctx context.Context) (int64, error) {
	if b.domain == "" {
		return 0, nil
	} // no synthetic global revision
	var rev int64
	err := b.pool.QueryRow(ctx, `SELECT revision FROM kine_crdb_domains WHERE domain=$1`, b.domain).Scan(&rev)
	return rev, err
}

func (b *backend) Compact(ctx context.Context, _ int64) (int64, error) { return b.CurrentRevision(ctx) }

func (b *backend) DbSize(ctx context.Context) (int64, error) {
	var size int64
	err := b.pool.QueryRow(ctx, `SELECT COALESCE(sum(range_size),0)::INT8 FROM [SHOW RANGES FROM TABLE kine_crdb_objects WITH DETAILS]`).Scan(&size)
	return size, err
}

func (b *backend) maintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastPrune := time.Now()
	pruneInterval := max(time.Second, min(30*time.Second, b.history/2))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := b.expire(ctx); err != nil && ctx.Err() == nil {
			logrus.Warnf("cockroach TTL: %v", err)
		}
		if time.Since(lastPrune) >= pruneInterval {
			if err := b.prune(ctx); err != nil && ctx.Err() == nil {
				logrus.Warnf("cockroach retention: %v", err)
			}
			lastPrune = time.Now()
		}
	}
}

func (b *backend) expire(ctx context.Context) error {
	rows, err := b.pool.Query(ctx, `SELECT key,mod_revision FROM kine_crdb_objects WHERE expires_at <= now() LIMIT 1000`)
	if err != nil {
		return err
	}
	type item struct {
		key string
		rev int64
	}
	var items []item
	for rows.Next() {
		var key []byte
		var rev int64
		if err := rows.Scan(&key, &rev); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item{string(key), rev})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, i := range items {
		if _, _, _, err := b.Delete(ctx, i.key, i.rev); err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) prune(ctx context.Context) error {
	// Use the database clock and actual committed MVCC timestamps. Keep the
	// boundary anchor for inclusive replay from the next retained revision.
	rows, err := b.pool.Query(ctx, `SELECT domain,max(revision) FROM kine_crdb_anchors
	 WHERE crdb_internal_mvcc_timestamp < ((extract(epoch FROM now())-$1)*1e9)::DECIMAL GROUP BY domain`, b.history.Seconds())
	if err != nil {
		return err
	}
	floors := map[string]int64{}
	for rows.Next() {
		var domain string
		var rev int64
		if err := rows.Scan(&domain, &rev); err != nil {
			rows.Close()
			return err
		}
		floors[domain] = rev
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	// A maintenance transaction must never lock counters for two resources.
	// Waiting on one resource must not hold other resources' counter locks.
	for domain, floor := range floors {
		if err := b.retry(ctx, func(tx pgx.Tx) error {
			var compact int64
			if err := tx.QueryRow(ctx, `UPDATE kine_crdb_domains SET compact_revision=greatest(compact_revision,$2) WHERE domain=$1 RETURNING compact_revision`, domain, floor).Scan(&compact); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `DELETE FROM kine_crdb_anchors WHERE domain=$1 AND revision<$2`, domain, compact)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
