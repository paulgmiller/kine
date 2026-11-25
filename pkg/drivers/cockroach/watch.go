package cockroach

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const maxPendingBytes = 64 << 20

var errReplayComplete = errors.New("resource replay reached its snapshot revision")

type subscription struct {
	domain  string
	batches chan server.EventBatch
	errors  chan error
}

type cdcRow struct {
	Domain         string `json:"domain"`
	Key            string `json:"key"`
	Value          string `json:"value"`
	Revision       int64  `json:"revision"`
	Operation      string `json:"operation"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`
	Lease          int64  `json:"lease"`
}

type envelope struct {
	After    *cdcRow `json:"after"`
	Before   *cdcRow `json:"before"`
	Updated  string  `json:"updated"`
	Resolved string  `json:"resolved"`
}

func bytesValue(s string) ([]byte, error) {
	if !strings.HasPrefix(s, `\x`) {
		return nil, errors.New("unexpected changefeed BYTES encoding")
	}
	return hex.DecodeString(s[2:])
}

func (r *cdcRow) kv() (*server.KeyValue, error) {
	key, err := bytesValue(r.Key)
	if err != nil {
		return nil, err
	}
	value, err := bytesValue(r.Value)
	if err != nil {
		return nil, err
	}
	return &server.KeyValue{Key: string(key), Value: value, CreateRevision: r.CreateRevision, ModRevision: r.ModRevision, Version: r.Version, Lease: r.Lease}, nil
}

type pending struct {
	size      int
	timestamp string
	domain    string
	anchor    *cdcRow
	object    *envelope
}

type decoder struct {
	pending    map[string]*pending
	delivered  map[string]int64
	timestamps map[string]string
	bytes      int
	revisions  map[string]map[int64]*pending
}

func newDecoder() *decoder {
	return &decoder{pending: make(map[string]*pending), delivered: make(map[string]int64), timestamps: make(map[string]string), revisions: make(map[string]map[int64]*pending)}
}

func compareHLC(a, b string) int {
	x, _ := new(big.Rat).SetString(a)
	y, _ := new(big.Rat).SetString(b)
	return x.Cmp(y)
}

// rewindHLC starts before the requested revision itself, rather than at its
// predecessor anchor (which can be older than GC for an otherwise idle resource).
func rewindHLC(timestamp string) (string, error) {
	if !hlcPattern.MatchString(timestamp) {
		return "", errors.New("invalid HLC timestamp")
	}
	wall, _, _ := strings.Cut(timestamp, ".")
	n, ok := new(big.Int).SetString(wall, 10)
	if !ok || n.Sign() <= 0 {
		return "", errors.New("invalid HLC wall time")
	}
	return n.Sub(n, big.NewInt(1)).String() + ".0000000000", nil
}

func (d *decoder) accept(topic string, value []byte, domain string, emit func(string, server.EventBatch) error) error {
	var e envelope
	if err := json.Unmarshal(value, &e); err != nil {
		return err
	}
	if e.Resolved != "" {
		if !hlcPattern.MatchString(e.Resolved) {
			return errors.New("invalid resolved HLC")
		}
		return d.flush(e.Resolved, emit)
	}
	row := e.After
	if row == nil {
		row = e.Before
	}
	if row == nil || (domain != "" && row.Domain != domain) {
		return nil
	}
	// Anchor removal is metadata GC, never a Kubernetes delete event.
	anchor := strings.HasSuffix(topic, "kine_crdb_anchors")
	if anchor && e.After == nil {
		return nil
	}
	if !hlcPattern.MatchString(e.Updated) {
		return errors.New("changefeed omitted exact update timestamp")
	}
	// At-least-once delivery is per key, not per transaction: recovery may
	// duplicate just the object or just the anchor. A complete contiguous
	// transaction certifies all older changes in this resource, so discard
	// either half independently instead of waiting for a duplicate partner.
	if last := d.timestamps[row.Domain]; last != "" && compareHLC(e.Updated, last) <= 0 {
		return nil
	}
	key := row.Domain + "\x00" + e.Updated
	p := d.pending[key]
	if p == nil {
		p = &pending{domain: row.Domain, timestamp: e.Updated}
		d.pending[key] = p
	}
	if anchor {
		p.anchor = row
		if d.revisions[row.Domain] == nil {
			d.revisions[row.Domain] = make(map[int64]*pending)
		}
		d.revisions[row.Domain][row.Revision] = p
	} else {
		p.object = &e
	}
	d.bytes += len(value)
	p.size += len(value)
	if d.bytes > maxPendingBytes {
		return server.ErrGRPCUnhealthy
	}
	logrus.Tracef("COCKROACH CDC domain=%s topic=%s revision=%d modRevision=%d delivered=%d timestamp=%s anchor=%v object=%v", p.domain, topic, row.Revision, row.ModRevision, d.delivered[p.domain], p.timestamp, p.anchor != nil, p.object != nil)
	// Every mutation writes exactly one object and one immutable anchor.
	// Once the pair is present, contiguous counter revisions establish order
	// without waiting for a (potentially seconds-late) resolved timestamp.
	if p.anchor != nil && p.anchor.Revision <= d.delivered[p.domain] && (p.object != nil || p.anchor.Operation == "init") {
		d.remove(p)
		return nil
	}
	return d.drain(p.domain, emit)
}

func (d *decoder) remove(p *pending) {
	delete(d.pending, p.domain+"\x00"+p.timestamp)
	if p.anchor != nil {
		delete(d.revisions[p.domain], p.anchor.Revision)
	}
	d.bytes -= p.size
}

func (d *decoder) drain(domain string, emit func(string, server.EventBatch) error) error {
	for {
		p := d.revisions[domain][d.delivered[domain]+1]
		if p == nil || (p.anchor.Operation != "init" && p.object == nil) {
			return nil
		}
		batch, err := p.batch()
		if err != nil {
			return err
		}
		if err := emit(domain, batch); err != nil {
			return err
		}
		d.delivered[domain] = p.anchor.Revision
		d.timestamps[domain] = p.timestamp
		d.remove(p)
	}
}

func (p *pending) batch() (server.EventBatch, error) {
	a := p.anchor
	batch := server.EventBatch{CurrentRev: a.Revision}
	if a.Operation == "init" {
		return batch, nil
	}
	if a.Operation != "create" && a.Operation != "update" && a.Operation != "delete" {
		return batch, errors.New("unknown anchor operation")
	}
	event := &server.Event{Create: a.Operation == "create", Delete: a.Operation == "delete"}
	if p.object.Before != nil {
		var err error
		event.PrevKV, err = p.object.Before.kv()
		if err != nil {
			return batch, err
		}
	}
	if event.Delete {
		if event.PrevKV == nil {
			return batch, errors.New("delete changefeed omitted before image")
		}
		kv := *event.PrevKV
		kv.ModRevision = a.Revision
		event.KV = &kv
	} else {
		if p.object.After == nil {
			return batch, errors.New("put changefeed omitted after image")
		}
		var err error
		event.KV, err = p.object.After.kv()
		if err != nil {
			return batch, err
		}
		if event.KV.ModRevision != a.Revision {
			return batch, errors.New("object/anchor revision mismatch")
		}
	}
	key, err := bytesValue(a.Key)
	if err != nil || len(key) == 0 || !bytes.Equal(key, []byte(event.KV.Key)) {
		return batch, errors.New("object/anchor key mismatch")
	}
	batch.Events = []*server.Event{event}
	return batch, nil
}

func (d *decoder) flush(resolved string, emit func(string, server.EventBatch) error) error {
	var ready []*pending
	for _, p := range d.pending {
		if compareHLC(p.timestamp, resolved) <= 0 {
			ready = append(ready, p)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return compareHLC(ready[i].timestamp, ready[j].timestamp) < 0 })
	for _, p := range ready {
		if p.anchor == nil {
			return errors.New("resolved object change has no revision anchor")
		}
		if p.anchor.Revision > d.delivered[p.domain] {
			if err := d.drain(p.domain, emit); err != nil {
				return err
			}
			if p.anchor.Revision > d.delivered[p.domain] {
				return errors.New("resolved frontier contains an incomplete resource revision")
			}
		}
		if _, exists := d.pending[p.domain+"\x00"+p.timestamp]; exists {
			d.remove(p)
		}
	}
	if len(d.pending) == 0 {
		d.bytes = 0
	}
	return nil
}

// feed uses a dedicated SQL connection; long-lived streams never exhaust the
// request pool. resolved frontiers certify complete object/anchor transactions.
func (s *store) feed(ctx context.Context, cursor, end, domain string, emit func(string, server.EventBatch) error, checkpoint func(string)) error {
	if !hlcPattern.MatchString(cursor) || (end != "" && !hlcPattern.MatchString(end)) {
		return errors.New("invalid changefeed cursor")
	}
	config := s.connConfig.Copy()
	// Streaming notifications must not wait for the default 512 KiB pgwire
	// results buffer to fill. This is a connection-startup setting, scoped
	// to changefeed connections rather than to the request pool or cluster.
	config.RuntimeParams["results_buffer_size"] = "0"
	if end == "" {
		config.RuntimeParams["application_name"] = s.instance
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	d := newDecoder()
	// Establish the revision baseline at the exact changefeed cursor. A
	// SQL read of the latest counters here would skip concurrent mutations.
	baselineQuery := `SELECT domain,revision FROM kine_crdb_domains AS OF SYSTEM TIME ` + cursor
	if domain != "" {
		baselineQuery += ` WHERE domain=$1`
	}
	var args []any
	if domain != "" {
		args = []any{domain}
	}
	baseline, err := conn.Query(ctx, baselineQuery, args...)
	if err != nil {
		return translate(err)
	}
	for baseline.Next() {
		var domain string
		var rev int64
		if err := baseline.Scan(&domain, &rev); err != nil {
			baseline.Close()
			return err
		}
		d.delivered[domain] = rev
		d.timestamps[domain] = cursor
	}
	baseline.Close()
	if err := baseline.Err(); err != nil {
		return translate(err)
	}
	if end == "" && domain == "" {
		s.mu.Lock()
		for domain, rev := range d.delivered {
			s.published[domain] = max(s.published[domain], rev)
		}
		s.available = true
		s.mu.Unlock()
	}
	query := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE kine_crdb_objects, kine_crdb_anchors WITH cursor='%s', updated, resolved='100ms', min_checkpoint_frequency='100ms', diff, format='json', envelope='wrapped'`, cursor)
	if end != "" {
		query += fmt.Sprintf(", end_time='%s'", end)
	}
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return translate(err)
	}
	// pgx Rows.Close drains the command. A live changefeed never finishes,
	// so draining on decoder/consumer failure would hide the error forever.
	// This connection is dedicated to this feed: abort it before closing rows.
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
		rows.Close()
	}()
	for rows.Next() {
		var topic *string
		var key, value []byte
		if err := rows.Scan(&topic, &key, &value); err != nil {
			return err
		}
		t := ""
		if topic != nil {
			t = *topic
		}
		if err := d.accept(t, value, domain, emit); err != nil {
			return err
		}
		if end == "" {
			s.buffered.Set(float64(d.bytes))
		}
		var e envelope
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		if e.Resolved != "" && checkpoint != nil {
			checkpoint(e.Resolved)
		}
	}
	if err := rows.Err(); err != nil {
		return translate(err)
	}
	if len(d.pending) != 0 {
		return errors.New("changefeed ended with incomplete resource revisions")
	}
	return nil
}

func (s *store) publish(domain string, batch server.EventBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published[domain] = max(s.published[domain], batch.CurrentRev)
	for sub := range s.subs {
		if sub.domain != domain {
			continue
		}
		select {
		case sub.batches <- batch:
		default:
			sub.errors <- server.ErrGRPCUnhealthy
			close(sub.batches)
			delete(s.subs, sub)
			s.metrics.WithLabelValues("slow_watch").Inc()
		}
	}
	s.metrics.WithLabelValues("watch_revision").Inc()
	return nil
}

func (s *store) live(ctx context.Context, cursor string, ready chan<- error) {
	started := false
	for ctx.Err() == nil {
		err := s.feed(ctx, cursor, "", "", s.publish, func(ts string) {
			cursor = ts
			if !started {
				ready <- nil
				started = true
			}
			wall, _, _ := strings.Cut(ts, ".")
			if nanos, err := strconv.ParseInt(wall, 10, 64); err == nil {
				s.watchLag.Set(max(0, time.Since(time.Unix(0, nanos)).Seconds()))
			}
		})
		if ctx.Err() != nil {
			return
		}
		if !started {
			if err == nil {
				err = server.ErrGRPCUnhealthy
			}
			ready <- err
			return
		}
		if err == nil {
			err = server.ErrGRPCUnhealthy
		}
		logrus.Warnf("cockroach changefeed reconnecting: %v", err)
		s.metrics.WithLabelValues("feed_reconnect").Inc()
		s.mu.Lock()
		s.available = false
		for sub := range s.subs {
			sub.errors <- err
			close(sub.batches)
			delete(s.subs, sub)
		}
		s.mu.Unlock()
		// If MVCC history was lost, all clients were explicitly canceled and
		// must relist. Restart the live feed at a fresh database timestamp.
		if errors.Is(err, server.ErrCompacted) {
			if e := s.pool.QueryRow(ctx, "SELECT cluster_logical_timestamp()::STRING").Scan(&cursor); e != nil {
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (b *backend) Watch(ctx context.Context, revision int64) server.WatchResult {
	result, errc := make(chan server.EventBatch, 32), make(chan error, 1)
	wr := server.WatchResult{Eventc: result, Errorc: errc}
	fail := func(err error) server.WatchResult {
		if errors.Is(err, server.ErrCompacted) {
			wr.CompactRevision = max(int64(1), revision)
		}
		errc <- err
		close(result)
		close(errc)
		return wr
	}
	if b.domain == "" {
		return fail(server.ErrNotSupported)
	}
	sub := &subscription{domain: b.domain, batches: make(chan server.EventBatch, 32), errors: make(chan error, 1)}
	b.mu.Lock()
	if !b.available {
		b.mu.Unlock()
		return fail(server.ErrGRPCUnhealthy)
	}
	published := b.published[b.domain]
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	unsubscribe := func() { b.mu.Lock(); delete(b.subs, sub); b.mu.Unlock() }
	tx, current, err := b.snapshot(ctx, 0)
	if err != nil {
		unsubscribe()
		return fail(err)
	}
	var end string
	var compact int64
	err = tx.QueryRow(ctx, `SELECT cluster_logical_timestamp()::STRING,compact_revision FROM kine_crdb_domains WHERE domain=$1`, b.domain).Scan(&end, &compact)
	_ = tx.Rollback(ctx)
	wr.CurrentRevision = current
	if err != nil {
		unsubscribe()
		return fail(err)
	}
	if revision == 0 {
		revision = current + 1
	}
	if revision <= compact {
		unsubscribe()
		return fail(server.ErrCompacted)
	}
	var cursor string
	// If the shared feed had not published the requested revision when we
	// subscribed, every needed change will arrive through this subscription.
	// Avoid opening a historical feed merely because a concurrent write
	// became visible to the SQL snapshot before its changefeed notification.
	if revision <= current && revision <= published {
		timestamp, e := b.anchorTime(ctx, revision)
		if e != nil {
			unsubscribe()
			return fail(e)
		}
		cursor, err = rewindHLC(timestamp)
		if err != nil {
			unsubscribe()
			return fail(err)
		}
	}
	go func() {
		defer unsubscribe()
		defer close(result)
		defer close(errc)
		last := revision - 1
		emit := func(_ string, batch server.EventBatch) error {
			if batch.CurrentRev <= last {
				return nil
			}
			select {
			case result <- batch:
				last = batch.CurrentRev
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-b.ctx.Done():
				return b.ctx.Err()
			case <-time.After(5 * time.Second):
				return server.ErrGRPCUnhealthy
			}
		}
		if cursor != "" {
			replayCtx, cancelReplay := context.WithCancel(ctx)
			replayEmit := func(domain string, batch server.EventBatch) error {
				if err := emit(domain, batch); err != nil {
					return err
				}
				// The snapshot counter tells us exactly which resource
				// revisions precede the handoff. Once all are delivered,
				// waiting for an unrelated timestamp frontier delays live
				// events and can starve an otherwise caught-up watcher.
				if batch.CurrentRev >= current {
					cancelReplay()
					return errReplayComplete
				}
				return nil
			}
			err := b.feed(replayCtx, cursor, end, b.domain, replayEmit, nil)
			cancelReplay()
			if err != nil && !errors.Is(err, errReplayComplete) {
				errc <- err
				return
			}
		}
		// A snapshot at current revision plus its completed replay is safe
		// progress, including when no events matched the requested start.
		baseline := min(current, last)
		if cursor != "" {
			baseline = current
		}
		if baseline >= last {
			select {
			case result <- server.EventBatch{CurrentRev: baseline}:
			case <-ctx.Done():
				return
			case <-b.ctx.Done():
				return
			}
			last = max(last, baseline)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-b.ctx.Done():
				return
			case err := <-sub.errors:
				errc <- err
				return
			case batch, ok := <-sub.batches:
				if !ok {
					select {
					case err := <-sub.errors:
						errc <- err
					default:
						errc <- server.ErrGRPCUnhealthy
					}
					return
				}
				if err := emit(b.domain, batch); err != nil {
					errc <- err
					return
				}
			}
		}
	}()
	return wr
}
