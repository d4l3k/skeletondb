package skeleton

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func TestTransactionCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTxn()
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err == nil {
		t.Fatal(errors.New("txn.Commit() should have thrown an error"))
	}
	if status := txn.Status(); status != StatusCommitted {
		t.Fatal(errors.Errorf("txn.Status() = %v; not %v", status, StatusCommitted))
	}
}

func TestTransactionClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTxn()
	if err := txn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Close(); err == nil {
		t.Fatal(errors.New("txn.Close() should have thrown an error"))
	}
	if status := txn.Status(); status != StatusAborted {
		t.Fatal(errors.Errorf("txn.Status() = %v; not %v", status, StatusAborted))
	}
}

func TestTransactionPutCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 10

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTxn()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		txn.Put(k, k)
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := txn.Get(k)
		if !bytes.Equal(out, k) {
			t.Errorf("txn.Get(%q) = %q; not %q", k, out, k)
		}
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if out != nil {
			t.Errorf("db.Get(%q) = %q; not nil", k, out)
		}
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if !bytes.Equal(out, k) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, k)
		}
	}
}

// TestTransactionSerializability tests that when two transactions conflict, one
// is committed and the other is aborted.
func TestTransactionSerializability(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	k := intToKey(1)
	k2 := intToKey(2)

	// Create a transaction with read and write intents.
	txn := db.NewTxn()
	txn.Get(k)
	txn.Put(k2, k2)

	// Write conflict.
	txn2 := db.NewTxn()
	txn2.Put(k, k)
	if err := txn2.Commit(); err != ErrTxnConflict {
		t.Fatalf("err = %v; not %v", err, ErrTxnConflict)
	}

	// First transaction should be fine.
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestTransactionPendingConsolidate tests that consolidation doesn't
// consolidate pending transactions.
func TestTransactionPendingConsolidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 10

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	k := intToKey(-100)
	txn := db.NewTxn()
	txn.Put(k, k)

	// Force consolidation.
	db.consolidate(rootPage)

	if v, _ := db.Get(k); v != nil {
		t.Fatalf("db.Get(%q) = %q; not nil", k, v)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	if v, _ := db.Get(k); !bytes.Equal(v, k) {
		t.Fatalf("db.Get(%q) = %q; not %q", k, v, k)
	}
}

// TestTransactionPendingSplit tests that splits handle pending transactions
// correctly.
func TestTransactionPendingSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 200

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	k := intToKey(-100)
	txn := db.NewTxn()
	txn.Put(k, k)

	// Force consolidation and splitting
	db.consolidate(rootPage)
	db.split(rootPage)

	if v, _ := db.Get(k); v != nil {
		t.Fatalf("db.Get(%q) = %q; not nil", k, v)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	if v, _ := db.Get(k); !bytes.Equal(v, k) {
		t.Fatalf("db.Get(%q) = %q; not %q", k, v, k)
	}
}

// TestDBTxn tests that splits handle pending transactions
// correctly.
func TestDBTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 200

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	k := []byte("key")
	if err := db.Put(k, []byte{0}); err != nil {
		t.Fatal(err)
	}

	txn := db.NewTxn()
	if err := txn.Put(k, []byte{1}); err != nil {
		t.Fatal(err)
	}

	times := 0
	if err := db.Txn(func(t *Txn) error {
		if times >= 2 {
			if err := txn.Commit(); err != nil {
				return errors.Wrap(err, "txn.Commit() failed")
			}
		}
		a, _ := t.Get(k)
		t.Put(k, []byte{a[0] + 1})
		times++
		return nil
	}); err != nil {
		t.Fatalf("%+v", err)
	}

	if times != 3 {
		t.Fatalf("db.Txn ran %d times; not 3", times)
	}
	expected := []byte{2}
	if v, _ := db.Get(k); !bytes.Equal(v, expected) {
		t.Fatalf("db.Get(%q) = %q; not %q", k, v, expected)
	}
}
