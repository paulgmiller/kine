package server

import (
	"bytes"
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) forRange(ctx context.Context, key, end []byte) (*LimitedServer, error) {
	s, ok := l.backend.(ScopedBackend)
	if !ok {
		return l, nil
	}
	b, _, err := s.Scope(ctx, string(key), string(end))
	if err != nil {
		return nil, err
	}
	copy := *l
	copy.backend = b
	return &copy, nil
}

func (l *LimitedServer) forTxn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*LimitedServer, error) {
	s, ok := l.backend.(ScopedBackend)
	if !ok {
		return l, nil
	}
	var backend Backend
	var domain string
	var txnKey []byte
	check := func(key, end []byte) error {
		// LimitedServer supports single-key transactions. Do not let its
		// shape recognizers silently substitute a different mutation key.
		if len(end) != 0 || (backend != nil && !bytes.Equal(txnKey, key)) {
			return ErrNotSupported
		}
		b, d, err := s.Scope(ctx, string(key), string(end))
		if err != nil {
			return err
		}
		if backend != nil && domain != d {
			return ErrNotSupported
		}
		backend, domain = b, d
		txnKey = key
		return nil
	}
	for _, c := range txn.Compare {
		if err := check(c.Key, c.RangeEnd); err != nil {
			return nil, err
		}
	}
	for _, ops := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		for _, op := range ops {
			var err error
			switch {
			case op.GetRequestRange() != nil:
				r := op.GetRequestRange()
				err = check(r.Key, r.RangeEnd)
			case op.GetRequestPut() != nil:
				err = check(op.GetRequestPut().Key, nil)
			case op.GetRequestDeleteRange() != nil:
				r := op.GetRequestDeleteRange()
				err = check(r.Key, r.RangeEnd)
			default:
				return nil, ErrNotSupported
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if backend == nil {
		return nil, ErrNotSupported
	}
	copy := *l
	copy.backend = backend
	return &copy, nil
}
