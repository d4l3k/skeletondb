package skeleton

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
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

// TestConsolidate tests that deltas will be correctly consolidated when there
// are too many of them.
func TestConsolidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 1000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write a bunch of data in so deltas are created.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Put(k, k)
	}
	// Consolidation only triggers on gets.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Get(k)
	}

	util.SucceedsSoon(t, func() error {
		for i := range *db.pages {
			id := pageID(i + 1)
			count := db.getDeltaCount(id)
			if count > db.config.MaxDeltaCount {
				return errors.Errorf("page %d: delta count = %d; not <= %d", id, count, db.config.MaxDeltaCount)
			}
		}
		return nil
	})
}

// TestSplit tests that nodes are split when they get too large.
func TestSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 1000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write a bunch of data in so deltas are created.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Put(k, k)
	}
	// Splits only triggers on gets.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Get(k)
	}

	util.SucceedsSoon(t, func() error {
		for i := range *db.pages {
			id := pageID(i + 1)
			page := db.getPage(id).getPage()
			// Since we're iterating over the entire page table, there might be nil
			// entries.
			if page == nil {
				continue
			}
			count := len(page.keys)
			if count > db.config.MaxKeysPerNode {
				return errors.Errorf("page %d: key count = %d; not <= %d", id, count, db.config.MaxKeysPerNode)
			}
		}
		return nil
	})
}
