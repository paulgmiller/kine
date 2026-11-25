package cockroach

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

func testBackend(t *testing.T, params string) (*backend, func() *backend) {
	t.Helper()
	dsn := os.Getenv("KINE_COCKROACH_TEST_DSN")
	if dsn == "" {
		t.Skip("set KINE_COCKROACH_TEST_DSN to a CockroachDB v26.2 database URL")
	}
	u, err := url.Parse(strings.Replace(dsn, "cockroach://", "postgres://", 1))
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(t.Context(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("kine_native_%d", time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	if params != "" {
		u.RawQuery += "&" + params
	}
	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		_, err := admin.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{name}.Sanitize()+` CASCADE`)
		if err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	open := func() *backend {
		_, b, err := New(ctx, wg, &drivers.Config{DataSourceName: strings.TrimPrefix(u.String(), "postgres://")})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return b.(*backend)
	}
	return open(), open
}

func scope(t *testing.T, b *backend, resource string) *backend {
	t.Helper()
	s, _, err := b.Scope(t.Context(), "/registry/"+resource+"/", "/registry/"+resource+"0")
	if err != nil {
		t.Fatal(err)
	}
	return s.(*backend)
}

func TestNativeMVCCAndIndependentCounters(t *testing.T) {
	b, open := testBackend(t, "default_transaction_isolation=read+committed")
	peer := open()
	ctx := t.Context()
	var isolation string
	if err := b.pool.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&isolation); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(isolation, "serializable") {
		t.Fatalf("native isolation was weakened: %q", isolation)
	}
	key := "/registry/pods/default/a"
	rev, err := b.Create(ctx, key, []byte("first"), 0)
	if err != nil || rev != 2 {
		t.Fatalf("create: rev=%d err=%v", rev, err)
	}
	if _, err := b.Create(ctx, key, []byte("duplicate"), 0); !errors.Is(err, server.ErrKeyExists) {
		t.Fatal(err)
	}
	r, kv, ok, err := peer.Update(ctx, key, []byte("second"), rev, 0)
	if err != nil || !ok || r != 3 || kv.Version != 2 {
		t.Fatalf("update: %d %+v %v %v", r, kv, ok, err)
	}
	if failed, _, ok, err := b.Update(ctx, key, []byte("stale"), rev, 0); err != nil || ok || failed != r {
		t.Fatalf("CAS failure: %d %v %v", failed, ok, err)
	}
	_, old, err := b.Get(ctx, key, rev, false)
	if err != nil || old == nil || string(old.Value) != "first" {
		t.Fatalf("historical read: %+v %v", old, err)
	}
	oldTime, err := scope(t, b, "pods").anchorTime(ctx, rev)
	if err != nil {
		t.Fatal(err)
	}
	var objectTime string
	query := `SELECT crdb_internal_mvcc_timestamp::STRING FROM kine_crdb_objects AS OF SYSTEM TIME ` + oldTime + ` WHERE domain=$1 AND key=$2`
	if err := b.pool.QueryRow(ctx, query, "/registry/pods", []byte(key)).Scan(&objectTime); err != nil {
		t.Fatal(err)
	}
	if compareHLC(oldTime, objectTime) != 0 {
		t.Fatalf("anchor/object timestamps differ: %s %s", oldTime, objectTime)
	}
	other, err := b.Create(ctx, "/registry/deployments/default/a", []byte("independent"), 0)
	if err != nil || other != 2 {
		t.Fatalf("independent counter: %d %v", other, err)
	}
	ns, err := b.Create(ctx, "/registry/pods/other/a", []byte("same resource"), 0)
	if err != nil || ns != 4 {
		t.Fatalf("namespace counter: %d %v", ns, err)
	}
	if _, _, err := b.Scope(ctx, "/registry/pods/", "/registry/services0"); !errors.Is(err, server.ErrNotSupported) {
		t.Fatalf("cross-domain range accepted: %v", err)
	}
	if _, _, err := b.Get(ctx, key, 100, false); !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("future read: %v", err)
	}
	if _, err := b.DbSize(ctx); err != nil {
		t.Fatal(err)
	}
}

func nextEvent(t *testing.T, wr server.WatchResult) *server.Event {
	t.Helper()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case batch, ok := <-wr.Eventc:
			if !ok {
				select {
				case err := <-wr.Errorc:
					t.Fatalf("watch closed: %v", err)
				default:
					t.Fatal("watch closed")
				}
				return nil
			}
			if len(batch.Events) > 0 {
				return batch.Events[0]
			}
		case err := <-wr.Errorc:
			t.Fatalf("watch error: %v", err)
			return nil
		case <-timer.C:
			t.Fatal("timed out waiting for changefeed event")
			return nil
		}
	}
}

func TestNativeWatchReplayDeleteAndPeerWrites(t *testing.T) {
	b, open := testBackend(t, "")
	peer := open()
	pods := scope(t, b, "pods")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	key := "/registry/pods/default/a"
	rev, err := peer.Create(ctx, key, []byte("first"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := peer.Update(ctx, key, []byte("second"), rev, 0); err != nil {
		t.Fatal(err)
	}
	deleted, _, _, err := peer.Delete(ctx, key, 0)
	if err != nil {
		t.Fatal(err)
	}
	wr := pods.Watch(ctx, rev)
	for _, kind := range []string{"create", "update", "delete"} {
		e := nextEvent(t, wr)
		if e.KV.ModRevision != rev {
			t.Fatalf("%s revision: got %d want %d", kind, e.KV.ModRevision, rev)
		}
		if kind == "delete" && (!e.Delete || e.KV.ModRevision != deleted || string(e.PrevKV.Value) != "second") {
			t.Fatalf("delete event: %+v", e)
		}
		rev++
	}
	recreated, err := peer.Create(ctx, key, []byte("third"), 0)
	if err != nil {
		t.Fatal(err)
	}
	e := nextEvent(t, wr)
	if !e.Create || e.KV.CreateRevision != recreated || string(e.KV.Value) != "third" {
		t.Fatalf("live recreate: %+v", e)
	}
}

func TestNativeRetentionAndTTL(t *testing.T) {
	b, _ := testBackend(t, "history-retention=2s&gc-retention=4s")
	key := "/registry/events/default/a"
	rev, err := b.Create(t.Context(), key, []byte("leased"), 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, kv, err := b.Get(t.Context(), key, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if kv == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, kv, err := b.Get(t.Context(), key, 0, false)
	if err != nil || kv != nil {
		t.Fatalf("TTL did not expire: %+v %v", kv, err)
	}
	time.Sleep(2200 * time.Millisecond)
	if err := b.prune(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, _, err = b.Get(t.Context(), key, rev, false)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expired revision: %v", err)
	}
	// Even after the last mutation ages out, reading the current resource
	// revision must remain possible for list pagination and watch startup.
	domain := scope(t, b, "events")
	current, err := domain.CurrentRevision(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.List(t.Context(), "/registry/events/", "/registry/events0", 0, current, false); err != nil {
		t.Fatalf("idle resource: %v", err)
	}
}

func TestDecoderRequiresCompletePairAndCorrelatesDelete(t *testing.T) {
	d := newDecoder()
	d.delivered["pods"] = 3
	var batches []server.EventBatch
	emit := func(_ string, b server.EventBatch) error { batches = append(batches, b); return nil }
	accept := func(topic string, e envelope) {
		t.Helper()
		data, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.accept(topic, data, "", emit); err != nil {
			t.Fatal(err)
		}
	}
	before := &cdcRow{Domain: "pods", Key: `\x61`, Value: `\x76`, CreateRevision: 2, ModRevision: 3, Version: 2}
	accept("kine_crdb_objects", envelope{Before: before, Updated: "100.0000000001"})
	if len(batches) != 0 {
		t.Fatal("emitted before anchor arrived")
	}
	accept("kine_crdb_anchors", envelope{After: &cdcRow{Domain: "pods", Key: `\x61`, Revision: 4, Operation: "delete"}, Updated: "100.0000000001"})
	accept("", envelope{Resolved: "100.0000000000"})
	accept("", envelope{Resolved: "100.0000000001"})
	if len(batches) != 1 || batches[0].Events[0].KV.ModRevision != 4 || batches[0].Events[0].PrevKV.ModRevision != 3 {
		t.Fatalf("bad delete: %+v", batches)
	}
	// Delivery retries can duplicate only one key in the transaction. A
	// resolved frontier after an orphan duplicate must not poison the feed.
	accept("kine_crdb_objects", envelope{Before: before, Updated: "100.0000000001"})
	accept("", envelope{Resolved: "100.0000000002"})
	if len(d.pending) != 0 || len(batches) != 1 {
		t.Fatal("one-sided duplicate retained as an incomplete transaction")
	}
	// A recovery can re-emit an entire transaction; it must not deliver twice.
	accept("kine_crdb_objects", envelope{Before: before, Updated: "100.0000000001"})
	accept("kine_crdb_anchors", envelope{After: &cdcRow{Domain: "pods", Revision: 4, Operation: "delete"}, Updated: "100.0000000001"})
	accept("", envelope{Resolved: "100.0000000001"})
	if len(batches) != 1 {
		t.Fatal("duplicate delivery")
	}
}

func TestDecoderOrdersIndependentResourcesWithoutWaitingForResolved(t *testing.T) {
	d := newDecoder()
	d.delivered["pods"] = 1
	d.delivered["services"] = 1
	var got []string
	emit := func(domain string, b server.EventBatch) error {
		got = append(got, fmt.Sprintf("%s/%d", domain, b.CurrentRev))
		return nil
	}
	accept := func(topic string, e envelope) {
		t.Helper()
		value, _ := json.Marshal(e)
		if err := d.accept(topic, value, "", emit); err != nil {
			t.Fatal(err)
		}
	}
	pair := func(domain string, rev int64, ts string) {
		accept("kine_crdb_anchors", envelope{After: &cdcRow{Domain: domain, Revision: rev, Key: `\x61`, Operation: "create"}, Updated: ts})
		accept("kine_crdb_objects", envelope{After: &cdcRow{Domain: domain, Key: `\x61`, Value: `\x76`, CreateRevision: rev, ModRevision: rev, Version: 1}, Updated: ts})
	}
	pair("pods", 3, "100.0000000002")
	if len(got) != 0 {
		t.Fatal("skipped missing resource revision")
	}
	pair("services", 2, "100.0000000001")
	if len(got) != 1 || got[0] != "services/2" {
		t.Fatalf("independent resource blocked: %v", got)
	}
	pair("pods", 2, "100.0000000000")
	if fmt.Sprint(got) != "[services/2 pods/2 pods/3]" {
		t.Fatalf("out of order: %v", got)
	}
	if d.bytes != 0 || len(d.pending) != 0 {
		t.Fatal("completed transaction buffers leaked")
	}
}

func TestNativeResourceLocksAndConcurrentCAS(t *testing.T) {
	b, _ := testBackend(t, "")
	ctx := t.Context()
	key := "/registry/pods/default/a"
	rev, err := b.Create(ctx, key, []byte("initial"), 0)
	if err != nil {
		t.Fatal(err)
	}
	scope(t, b, "deployments")
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT revision FROM kine_crdb_domains WHERE domain='/registry/pods' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	independentCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = b.Create(independentCtx, "/registry/deployments/default/a", []byte("independent"), 0)
	cancel()
	if err != nil {
		t.Fatalf("pods counter lock blocked deployments: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 16)
	errors := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			_, _, ok, err := b.Update(ctx, key, []byte("winner"), rev, 0)
			results <- ok
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	var wins int
	for ok := range results {
		if ok {
			wins++
		}
	}
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	current, err := scope(t, b, "pods").CurrentRevision(ctx)
	if err != nil || wins != 1 || current != rev+1 {
		t.Fatalf("concurrent CAS: wins=%d current=%d err=%v", wins, current, err)
	}
}

func TestNativeFeedDisconnectAndResume(t *testing.T) {
	b, _ := testBackend(t, "")
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	pods := scope(t, b, "pods")
	key := "/registry/pods/default/a"
	wr := pods.Watch(ctx, 0)
	rev, err := b.Create(ctx, key, []byte("before disconnect"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if e := nextEvent(t, wr); e.KV.ModRevision != rev {
		t.Fatal(e)
	}
	_, err = b.pool.Exec(ctx, `CANCEL SESSIONS SELECT session_id FROM [SHOW CLUSTER SESSIONS] WHERE application_name=$1`, b.instance)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-wr.Errorc:
		if err == nil {
			t.Fatal("disconnect did not cancel watch")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	updated, _, _, err := b.Update(ctx, key, []byte("after disconnect"), rev, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Direct backend consumers, like etcd clients, retry watch creation while
	// the shared source is reconnecting. It must not accept a subscription
	// across a cursor reset before its new baseline is established.
	for {
		b.mu.Lock()
		available := b.available
		b.mu.Unlock()
		if available {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	resumed := pods.Watch(ctx, rev+1)
	if e := nextEvent(t, resumed); e.KV.ModRevision != updated || string(e.KV.Value) != "after disconnect" {
		t.Fatalf("resume lost event: %+v", e)
	}
}

func TestNativeFeedReturnsConsumerErrorWithoutDrainingLiveStream(t *testing.T) {
	b, _ := testBackend(t, "")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scope(t, b, "pods")
	var cursor string
	if err := b.pool.QueryRow(ctx, `SELECT cluster_logical_timestamp()::STRING`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	want := errors.New("test consumer rejected event")
	done := make(chan error, 1)
	go func() {
		done <- b.feed(ctx, cursor, "", "/registry/pods", func(string, server.EventBatch) error { return want }, nil)
	}()
	if _, err := b.Create(ctx, "/registry/pods/default/a", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("consumer error was lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer error blocked while draining an unbounded changefeed")
	}
}

func TestNativeRejectsLegacySQLLogDatabase(t *testing.T) {
	b, _ := testBackend(t, "")
	if _, err := b.pool.Exec(t.Context(), `CREATE TABLE kine (id INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	config := b.pool.Config().ConnConfig.ConnString()
	wg := &sync.WaitGroup{}
	_, _, err := New(t.Context(), wg, &drivers.Config{DataSourceName: strings.TrimPrefix(config, "postgres://")})
	if err == nil || !strings.Contains(err.Error(), "migration is not supported") {
		t.Fatalf("legacy database was not rejected: %v", err)
	}
	wg.Wait()
}
