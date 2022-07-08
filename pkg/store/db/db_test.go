package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestRecordPull(t *testing.T) {
	root := t.TempDir()
	dbfile := filepath.Join(root, "db")
	ms, err := NewMetaStore(dbfile)
	if err != nil {
		t.Fatalf("failed to create metastore: %s", err)
	}
	defer ms.Close()

	ctx, tx, err := ms.TransactionContext(context.Background(), true)
	if err != nil {
		t.Fatalf("failed to get transaction context: %s", err)
	}
	defer tx.Rollback()

	registry := "fakereg.io"
	d := digest.FromString("test")
	ok, err := HasPullRecord(ctx, registry, d)
	if err != nil {
		t.Fatalf("failed to check if record exists: %s", err)
	} else if ok {
		t.Fatalf("record should not exist")
	}

	if err = RecordPull(ctx, registry, d); err != nil {
		t.Fatalf("failed to record image pull: %s", err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %s", err)
	}

	// Open a new transaction and check if record exists
	ctx, tx, err = ms.TransactionContext(context.Background(), false)
	if err != nil {
		t.Fatalf("failed to get transaction context: %s", err)
	}
	defer tx.Rollback()

	ok, err = HasPullRecord(ctx, registry, d)
	if err != nil {
		t.Fatalf("failed to check if record exists: %s", err)
	} else if !ok {
		t.Fatalf("expected record to exist")
	}
}
