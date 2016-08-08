package skeleton

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	// ErrTxnConflict represents a conflict when writing a transaction.
	ErrTxnConflict = errors.New("an error occurred while committing the transaction")
)

type txnStatus int64

// Status of the transaction.
const (
	StatusUnknown txnStatus = iota
	StatusPending
	StatusAborted
	StatusCommitted
)

type transaction struct {
	time   time.Time
	status txnStatus
}

func (t *transaction) commit() error {
	if !atomic.CompareAndSwapInt64((*int64)(&t.status), int64(StatusPending), int64(StatusCommitted)) {
		return ErrTxnConflict
	}
	return nil
}

// Txn represents a transaction.
type Txn struct {
	transaction *transaction
}

// Commit commits the transaction.
func (t *Txn) Commit() error {
	return t.transaction.commit()
}

// NewTxn creates a new transaction.
func (db *DB) NewTxn() *Txn {
	return &Txn{
		transaction: &transaction{
			time:   time.Now(),
			status: StatusPending,
		},
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
