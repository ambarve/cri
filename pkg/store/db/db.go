package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/containerd/containerd/snapshots/storage"
	"github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
)

// Transactor, Metastore and corresponding function below are directly copied from
// containerd/containerd/snapshots/storage/metastore.go, We can't use the functions from
// containerd directly since the transactionKey is not exported. Also, this is a temporary
// fix so it will be removed soon anyway.

// Transactor is used to finalize an active transaction.
type Transactor interface {
	// Commit commits any changes made during the transaction. On error a
	// caller is expected to clean up any resources which would have relied
	// on data mutated as part of this transaction. Only writable
	// transactions can commit, non-writable must call Rollback.
	Commit() error

	// Rollback rolls back any changes made during the transaction. This
	// must be called on all non-writable transactions and aborted writable
	// transaction.
	Rollback() error
}

// MetaStore is used to store metadata related to a snapshot driver. The
// MetaStore is intended to store metadata related to name, state and
// parentage. Using the MetaStore is not required to implement a snapshot
// driver but can be used to handle the persistence and transactional
// complexities of a driver implementation.
type MetaStore struct {
	dbfile string

	dbL sync.Mutex
	db  *bolt.DB
}

// NewMetaStore returns a snapshot MetaStore for storage of metadata related to
// a snapshot driver backed by a bolt file database. This implementation is
// strongly consistent and does all metadata changes in a transaction to prevent
// against process crashes causing inconsistent metadata state.
func NewMetaStore(dbfile string) (*MetaStore, error) {
	return &MetaStore{
		dbfile: dbfile,
	}, nil
}

type transactionKey struct{}

// TransactionContext creates a new transaction context. The writable value
// should be set to true for transactions which are expected to mutate data.
func (ms *MetaStore) TransactionContext(ctx context.Context, writable bool) (context.Context, Transactor, error) {
	ms.dbL.Lock()
	if ms.db == nil {
		db, err := bolt.Open(ms.dbfile, 0600, nil)
		if err != nil {
			ms.dbL.Unlock()
			return ctx, nil, fmt.Errorf("failed to open database file: %w", err)
		}
		ms.db = db
	}
	ms.dbL.Unlock()

	tx, err := ms.db.Begin(writable)
	if err != nil {
		return ctx, nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	ctx = context.WithValue(ctx, transactionKey{}, tx)

	return ctx, tx, nil
}

// Close closes the metastore and any underlying database connections
func (ms *MetaStore) Close() error {
	ms.dbL.Lock()
	defer ms.dbL.Unlock()
	if ms.db == nil {
		return nil
	}
	return ms.db.Close()
}

var (
	bucketKeyStorageVersion = []byte("v1")
	bucketKeyImagePulls     = []byte("imagepulls")
)

func withImagePullsBucket(ctx context.Context, fn func(context.Context, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok {
		return storage.ErrNoTransaction
	}

	bkt := tx.Bucket(bucketKeyStorageVersion)
	if bkt == nil {
		return fmt.Errorf("bucket %s not found", bucketKeyStorageVersion)
	}

	ibkt := bkt.Bucket(bucketKeyImagePulls)
	if ibkt == nil {
		return fmt.Errorf("bucket %s not found", bucketKeyImagePulls)
	}
	return fn(ctx, ibkt)
}

func generatePullRecordKey(registry string, blob digest.Digest) string {
	return fmt.Sprintf("%s_%s", registry, blob.String())
}

func HasPullRecord(ctx context.Context, registry string, blob digest.Digest) (bool, error) {
	var hasPulledBefore bool
	err := withImagePullsBucket(ctx, func(ctx context.Context, pullsBucket *bolt.Bucket) error {
		if r := pullsBucket.Get([]byte(generatePullRecordKey(registry, blob))); r != nil {
			hasPulledBefore = true
		}
		return nil
	})
	return hasPulledBefore, err
}

func createImagePullBucket(ctx context.Context) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok {
		return storage.ErrNoTransaction
	}

	bkt, err := tx.CreateBucketIfNotExists(bucketKeyStorageVersion)
	if err != nil {
		return fmt.Errorf("failed to create bucket %s", bucketKeyStorageVersion)
	}

	_, err = bkt.CreateBucketIfNotExists(bucketKeyImagePulls)
	if err != nil {
		return fmt.Errorf("failed to create bucket %s", bucketKeyImagePulls)
	}
	return nil
}

func RecordPull(ctx context.Context, registry string, blob digest.Digest) error {
	err := createImagePullBucket(ctx)
	if err != nil {
		return err
	}
	err = withImagePullsBucket(ctx, func(ctx context.Context, pullsBucket *bolt.Bucket) error {
		if err := pullsBucket.Put([]byte(generatePullRecordKey(registry, blob)), nil); err != nil {
			return err
		}
		return nil
	})
	return err
}
