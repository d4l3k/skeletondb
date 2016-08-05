package skeleton

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	keysPerNode = 20
)

var (
	childrenPool = sync.Pool{
		New: func() interface{} {
			return []*node(nil)
		},
	}
	nodePool = sync.Pool{
		New: func() interface{} {
			return &node{}
		},
	}
)

func getChildrenSlice() []*node {
	return childrenPool.Get().([]*node)[0:0]
}
func getNode() *node {
	n := nodePool.Get().(*node)
	*n = node{}
	return n
}

// NewDB creates a new database.
func NewDB() *DB {
	db := &DB{
		root: &node{},
	}
	db.unsafe = (*unsafeDB)(unsafe.Pointer(db))
	return db
}

// DB is a skeletondb instance.
type DB struct {
	root *node
	// unsafe is DB but with the root as a uintptr. This is done to stop the root
	// from being garbase collected.
	unsafe *unsafeDB
}

// An unsafe version of DB.
type unsafeDB struct {
	root   unsafe.Pointer
	unsafe *unsafeDB
}

// Get gets a value from the database.
func (db *DB) Get(key []byte) []byte {
	for {
		old := db.unsafe.root
		n := db.root
		for {
			if bytes.Equal(n.key, key) {
				// Read contention, restart read.
				if old != db.unsafe.root {
					break
				}
				return n.value
			} else if len(n.children) == 0 {
				// Read contention, restart read.
				if old != db.unsafe.root {
					break
				}
				return nil
			} else {
				i := n.find(key)
				if i >= len(n.children) {
					i = len(n.children) - 1
				}
				n = n.children[i]
			}
		}
	}
}

// Put writes a value into the database.
func (db *DB) Put(key, value []byte) {
	for {
		old := db.unsafe.root
		root := *db.root
		freeList := getChildrenSlice()
		freeList = append(freeList, db.root)
		allocList := getChildrenSlice()
		allocList = append(allocList, &root)
		n := &root
		for {
			// make a new copy of the slice
			if bytes.Equal(n.key, key) {
				n.value = value
			} else {
				i := n.find(key)
				if len(n.children) < keysPerNode {
					n2 := getNode()
					allocList = append(allocList, n2)
					n2.key = key
					n2.value = value
					n2.time = time.Now()
					n.children = append(append(append(getChildrenSlice(), n.children[:i]...), n2), n.children[i:]...)
				} else {
					if i >= len(n.children) {
						i = len(n.children) - 1
					}
					freeList = append(freeList, n.children[i])
					n2 := getNode()
					allocList = append(allocList, n2)
					*n2 = *n.children[i]
					n.children = append(getChildrenSlice(), n.children...)
					n.children[i] = n2
					n = n2
					continue
				}
			}
			break
		}

		// Try and save the new root. If there's contention, retry.
		if db.saveNewRoot(old, &root) {
			// Put the old nodes back into the pools to minimize allocations.
			free(freeList)
			return
		}
		free(allocList)
	}
}

func free(nodes []*node) {
	go func() {
		for _, node := range nodes {
			childrenPool.Put(node.children)
			nodePool.Put(node)
		}
		childrenPool.Put(nodes)
	}()
}

// saveNewRoot saves the new root and returns whether it succeeded.
func (db *DB) saveNewRoot(old unsafe.Pointer, root *node) bool {
	return atomic.CompareAndSwapPointer(&db.unsafe.root, old, unsafe.Pointer(root))
}

type node struct {
	children   []*node
	key, value []byte
	time       time.Time
	tombstone  bool
}

// find finds the closest matching node
func (n *node) find(key []byte) int {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, len(n.children)
	for i < j {
		h := i + (j-i)/2 // avoid overflow when computing h
		// i ≤ h < j
		if bytes.Compare(key, n.children[h].key) == -1 {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}
