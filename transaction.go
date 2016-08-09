package skeleton

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrTxnConflict represents a conflict when writing a transaction.
	ErrTxnConflict = errors.New("an error occurred while committing the transaction")
)

// TransactionStatus represents the state of the transaction.
type TransactionStatus int64

// Status of the transaction.
const (
	StatusUnknown TransactionStatus = iota
	StatusPending
	StatusAborted
	StatusCommitted
)

// Txn represents a transaction.
type Txn struct {
	db     *DB
	time   time.Time
	status TransactionStatus
}

// NewTxn creates a new transaction.
func (db *DB) NewTxn() *Txn {
	return &Txn{
		db:     db,
		time:   time.Now(),
		status: StatusPending,
	}
}

// Txn creates a new transaction. If no error is returned, the transaction tries
// to be committed. If there's a conflict, the transaction will automatically be
// retried.
func (db *DB) Txn(f func(*Txn) error) error {
	for {
		t := db.NewTxn()
		if err := f(t); err != nil {
			return err
		}
		if err := t.Commit(); err != ErrTxnConflict {
			return err
		}
	}
}

func (t *Txn) finish(status TransactionStatus) error {
	if !atomic.CompareAndSwapInt64((*int64)(&t.status), int64(StatusPending), int64(status)) {
		return ErrTxnConflict
	}
	return nil
}

// Commit commits the transaction.
func (t *Txn) Commit() error {
	return t.finish(StatusCommitted)
}

// Close aborts the transaction.
func (t *Txn) Close() error {
	return t.finish(StatusAborted)
}

// Status returns the current status of the transaction.
func (t Txn) Status() TransactionStatus {
	return t.status
}

// Put writes a value into the database.
func (t *Txn) Put(k, v []byte) error {
	return t.db.put(t, k, v)
}

// Delete removes a value from the database.
func (t *Txn) Delete(k []byte) error {
	return t.db.delete(t, k)
}

// Get gets a value from the database.
func (t *Txn) Get(key []byte) ([]byte, bool) {
	return t.db.getAt(t, key, zeroTime)
}

// GetAt gets a value from the database at the specified time.
func (t *Txn) GetAt(key []byte, at time.Time) ([]byte, bool) {
	return t.db.getAt(t, key, at)
}
