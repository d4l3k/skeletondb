package skeleton

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

func intToKey(i int) []byte {
	return []byte("key" + strconv.Itoa(i))
}

func TestBasicPutGet(t *testing.T) {
	const count = 100000

	db := NewDB()

	for i := 0; i < count; i++ {
		k := intToKey(i)
		db.Put(k, k)
	}

	for i := 0; i < count; i++ {
		k := intToKey(i)
		out := db.Get(k)
		if !bytes.Equal(out, k) {
			t.Errorf("expected db.Get(%q) = %q; not %q", k, out, k)
		}
	}
}

func TestConcurrentPutGet(t *testing.T) {
	const count = 10000
	const readers = 100
	const writers = 100

	db := NewDB()

	var done sync.WaitGroup

	for i := 0; i < readers; i++ {
		done.Add(1)
		go func() {
			for i := 0; i < count; i++ {
				k := intToKey(i)
				out := db.Get(k)
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
