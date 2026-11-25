package server

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	notifyInterval time.Duration
	backend        Backend
	scheme         string
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	var err error
	l, err = l.forRange(ctx, r.Key, r.RangeEnd)
	if err != nil {
		return nil, err
	}
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// Compaction bookkeeping has no resource scope. Keep its value at zero
	// for scoped backends so it cannot invalidate unrelated resource caches.
	_, scoped := l.backend.(ScopedBackend)
	var err error
	l, err = l.forTxn(ctx, txn)
	if err != nil {
		return nil, err
	}
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put)
	}
	if rev, key, prevKV, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev, prevKV)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if ver, value, ok := isCompact(txn); ok {
		if scoped {
			value = []byte("0")
		}
		resp, err := l.compact(ctx, ver, value)
		if scoped && resp != nil {
			// Kubernetes feeds this header back into its next compaction
			// request. There is no global revision for it to compact.
			resp.Header.Revision = 0
		}
		return resp, err
	}
	return nil, ErrNotSupported
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
