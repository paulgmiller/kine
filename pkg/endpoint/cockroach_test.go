package endpoint_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/k3s-io/kine/pkg/endpoint"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func nativeClient(t *testing.T) (*clientv3.Client, *clientv3.Client) {
	t.Helper()
	dsn := os.Getenv("KINE_COCKROACH_TEST_DSN")
	if dsn == "" {
		t.Skip("set KINE_COCKROACH_TEST_DSN for native bridge integration tests")
	}
	u, err := url.Parse(strings.Replace(dsn, "postgres://", "cockroach://", 1))
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("kine_bridge_%d", time.Now().UnixNano())
	u.Path = "/" + db
	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	var clients []*clientv3.Client
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
		cancel()
		wg.Wait()
		u.Scheme = "postgres"
		u.Path = "/defaultdb"
		conn, err := pgx.Connect(context.Background(), u.String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{db}.Sanitize()+` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	for range 2 {
		e, err := endpoint.Listen(ctx, endpoint.Config{Endpoint: u.String(), Listener: "tcp://127.0.0.1:0", WaitGroup: wg, NotifyInterval: 100 * time.Millisecond, EmulatedETCDVersion: "3.7.3"})
		if err != nil {
			t.Fatal(err)
		}
		c, err := clientv3.New(clientv3.Config{Endpoints: e.Endpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
	}
	return clients[0], clients[1]
}

func TestCockroachScopedWatchProgressAndCompaction(t *testing.T) {
	c, peer := nativeClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	key := "/registry/pods/default/a"
	first, err := peer.Put(ctx, key, "first")
	if err != nil {
		t.Fatal(err)
	}
	watch := c.Watch(ctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithRev(first.Header.Revision+1), clientv3.WithProgressNotify())
	other := c.Watch(ctx, "/registry/deployments/", clientv3.WithPrefix(), clientv3.WithProgressNotify())
	for i := range 10 {
		if _, err := peer.Put(ctx, fmt.Sprintf("/registry/deployments/default/d%d", i), "value"); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise the apiserver's exact compactor transaction. Its header and
	// stored value must not introduce a global revision into resource caches.
	compact, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Version("compact_rev_key"), "=", 0)).Then(clientv3.OpPut("compact_rev_key", "999")).Else(clientv3.OpGet("compact_rev_key")).Commit()
	if err != nil || !compact.Succeeded || compact.Header.Revision != 0 {
		t.Fatalf("compactor response: %+v %v", compact, err)
	}
	metadata, err := c.Get(ctx, "compact_rev_key")
	if err != nil || metadata.Header.Revision != 0 || len(metadata.Kvs) != 1 || string(metadata.Kvs[0].Value) != "0" {
		t.Fatalf("compactor metadata: %+v %v", metadata, err)
	}
	if _, err := c.Compact(ctx, 999); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, key, clientv3.WithRev(first.Header.Revision)); err != nil {
		t.Fatalf("explicit compaction discarded resource history: %v", err)
	}
	if err := c.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := peer.Put(ctx, key, "second")
	if err != nil {
		t.Fatal(err)
	}
	if second.Header.Revision != first.Header.Revision+1 {
		t.Fatalf("unrelated resource advanced pod revision: %d -> %d", first.Header.Revision, second.Header.Revision)
	}
	gotPod, gotOther := false, false
	for !gotPod || !gotOther {
		select {
		case wr := <-watch:
			if err := wr.Err(); err != nil {
				t.Fatal(err)
			}
			if wr.Header.Revision > second.Header.Revision {
				t.Fatalf("cross-domain progress: %d", wr.Header.Revision)
			}
			for _, e := range wr.Events {
				if string(e.Kv.Value) == "second" {
					gotPod = true
				}
			}
		case wr := <-other:
			if err := wr.Err(); err != nil {
				t.Fatal(err)
			}
			if wr.Header.Revision > second.Header.Revision {
				gotOther = true
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	_, err = c.Get(ctx, "/registry/", clientv3.WithPrefix())
	if err == nil {
		t.Fatal("cross-resource range accepted")
	}
	_, err = c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", second.Header.Revision)).Then(clientv3.OpPut("/registry/deployments/default/bad", "bad")).Commit()
	if err == nil {
		t.Fatal("cross-resource transaction accepted")
	}
}

func TestCockroachListThenWatchClosesConcurrentWriteGap(t *testing.T) {
	c, peer := nativeClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	prefix := "/registry/pods/default/"
	for _, key := range []string{"a", "b"} {
		if _, err := peer.Put(ctx, prefix+key, "initial"); err != nil {
			t.Fatal(err)
		}
	}
	list, err := c.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil || len(list.Kvs) != 2 {
		t.Fatalf("list: %+v %v", list, err)
	}
	r := list.Header.Revision
	// All three changes happen after the list but before creating the watch.
	if _, err := peer.Put(ctx, prefix+"a", "updated"); err != nil {
		t.Fatal(err)
	}
	deleted, err := peer.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(prefix+"b"), "=", list.Kvs[1].ModRevision)).Then(clientv3.OpDelete(prefix + "b")).Else(clientv3.OpGet(prefix + "b")).Commit()
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	if _, err := peer.Put(ctx, prefix+"c", "created"); err != nil {
		t.Fatal(err)
	}
	pinned, err := c.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(r))
	if err != nil || len(pinned.Kvs) != 2 || string(pinned.Kvs[0].Value) != "initial" || string(pinned.Kvs[1].Value) != "initial" {
		t.Fatalf("list snapshot changed: %+v %v", pinned, err)
	}
	watch := c.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(r+1))
	var got int
	for got < 4 {
		select {
		case wr, ok := <-watch:
			if !ok || wr.Err() != nil {
				t.Fatalf("watch failed: %v", wr.Err())
			}
			for _, e := range wr.Events {
				got++
				if e.Kv.ModRevision != r+int64(got) {
					t.Fatalf("missing or duplicate revision: got %d want %d", e.Kv.ModRevision, r+int64(got))
				}
				if got == 2 && e.Type != clientv3.EventTypeDelete {
					t.Fatalf("lost delete: %+v", e)
				}
			}
			if got == 3 {
				if _, err := peer.Put(ctx, prefix+"c", "live"); err != nil {
					t.Fatal(err)
				}
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
