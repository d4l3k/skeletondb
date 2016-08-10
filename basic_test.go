package skeleton

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func intToKey(i int) []byte {
	return intPrefix("key", i)
}
func intPrefix(prefix string, i int) []byte {
	return []byte(prefix + strconv.Itoa(i))
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

// TestNewDBBadConfig tests that passing an invalid config into NewDB throws an
// error.
func TestNewDBBadConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := &Config{}
	if err := c.Verify(); err == nil {
		t.Fatalf("expected %v.Verify() to throw an error", c)
	}
	if _, err := NewDB(c); err == nil {
		t.Fatalf("expected NewDB(%v) to throw an error", c)
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
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
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
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if !bytes.Equal(out, k) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, k)
		}
	}
}

func TestBasicPutDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 1000

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

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if !bytes.Equal(out, k) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, k)
		}
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out, _ := db.Get(k)
		if out != nil {
			t.Errorf("db.Get(%q) = %q; not nil", k, out)
		}
	}
}

func TestBasicGetAt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 1000

	db, err := NewDB(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		v := intPrefix("old-val", i)
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	initialWrite := time.Now()

	// Test read at old time.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		v := intPrefix("old-val", i)
		out, _ := db.GetAt(k, initialWrite)
		if !bytes.Equal(out, v) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, v)
		}
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		v := intPrefix("new-val", i)
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	// Test read at old time.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		v := intPrefix("old-val", i)
		out, _ := db.GetAt(k, initialWrite)
		if !bytes.Equal(out, v) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, v)
		}
	}

	// Test normal read.
	for i := 0; i < count; i++ {
		k := intToKey(i)
		v := intPrefix("new-val", i)
		out, _ := db.Get(k)
		if !bytes.Equal(out, v) {
			t.Errorf("db.Get(%q) = %q; not %q", k, out, v)
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
