package skeleton

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestConsolidateOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	k := []byte("key")

	for i := 0; i < 5; i++ {
		v := intToKey(i)
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	want := intToKey(4)
	if out, _ := db.Get(k); !bytes.Equal(out, want) {
		t.Fatalf("db.Get(%q) = %q; not %q", k, out, want)
	}
}
