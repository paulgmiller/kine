package sqllog

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func TestCompactReadsThroughTarget(t *testing.T) {
	tx := &compactReadTx{t: t}
	s := SQLLog{d: compactReadDialect{tx: tx}, ctx: t.Context(), compactTimeout: time.Second}
	if _, err := s.compact(0, 3, 4); err != nil {
		t.Fatal(err)
	}
	if tx.through != 3 {
		t.Fatalf("transaction compacted through revision %d, want 3", tx.through)
	}
}

type compactReadDialect struct {
	server.Dialect
	tx *compactReadTx
}

func (d compactReadDialect) BeginTx(context.Context, *sql.TxOptions) (server.Transaction, error) {
	return d.tx, nil
}

type compactReadTx struct {
	server.Transaction
	t       *testing.T
	through int64
}

func (tx *compactReadTx) CurrentRevision(context.Context) (int64, error) {
	tx.t.Error("compaction transaction read the database head")
	return 4, nil
}

func (tx *compactReadTx) Compact(_ context.Context, _, through int64) (int64, error) {
	tx.through = max(tx.through, through)
	return 0, nil
}

func (*compactReadTx) GetCompactRevision(context.Context) (int64, error) { return 0, nil }
func (*compactReadTx) SetCompactRevision(context.Context, int64) error   { return nil }
func (*compactReadTx) MustCommit()                                       {}
func (*compactReadTx) MustRollback()                                     {}
