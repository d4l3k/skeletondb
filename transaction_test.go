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
