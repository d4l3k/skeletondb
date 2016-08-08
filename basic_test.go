package skeleton

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/davecgh/go-spew/spew"
)

func intToKey(i int) []byte {
	return []byte("key" + strconv.Itoa(i))
}

// TestNewDBConfig makes sure a new DB with a nil config uses DefaultConfig.
// instead.
func TestNewDBConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	want := DefaultConfig
	if db.config != want {
		t.Errorf("db.config = %+v; not %+v", db.config, want)
	}
}

// TestBasicGet tests that get requests on an empty database work as expected.
func TestBasicGet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const count = 1000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, ok := db.Get(k)
		if ok {
			t.Errorf("%s: should not be ok", k)
		}
		if out != nil {
			t.Errorf("%s: should be nil, not %+v", k, out)
		}
	}
}

func TestBasicPut(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const count = 1000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Put(k, k)
	}
}

func TestBasicPutGet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const count = 10000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Put(k, k)
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if !bytes.Equal(out, k) {
			spew.Dump(db)
			t.Fatalf("expected db.Get(%q) = %q; not %q", k, out, k)
		}
	}
}

func TestConcurrentPutGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 100
	const readers = 100
	const writers = 100

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var done sync.WaitGroup

	for i := 0; i < readers; i++ {
		done.Add(1)
		go func() {
			for i := 0; i < count; i++ {
				k := intToKey(i)
				out, _ := db.Get(k)
				if out != nil && !bytes.Equal(out, k) {
					t.Errorf("expected db.Get(%q) = %q; not %q", k, out, k)
				}
			}
			done.Done()
		}()
	}
	for i := 0; i < writers; i++ {
		done.Add(1)
		go func() {
			for i := 0; i < count; i++ {
				k := intToKey(i)
				db.Put(k, k)
			}
			done.Done()
		}()
	}
	done.Wait()
	t.Log("done done")
}
