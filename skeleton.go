package skeleton

import (
	"bytes"
	"log"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	rootPage = pageID(1)
)

var zeroTime = time.Unix(0, 0)

// DB is a skeletondb instance.
type DB struct {
	pages            []*delta
	splitQueue       chan pageID
	consolidateQueue chan pageID
	closed           chan struct{}
	config           Config
}

// NewDB creates a new database.
func NewDB(c *Config) (*DB, error) {
	log.SetOutput(os.Stderr)
	if c == nil {
		c = &DefaultConfig
	}
	if err := c.Verify(); err != nil {
		return nil, err
	}
	db := &DB{
		splitQueue:       make(chan pageID, 10),
		consolidateQueue: make(chan pageID, 10),
		closed:           make(chan struct{}),
		pages: []*delta{
			{
				next: &delta{
					page: &page{id: 1},
				},
			},
		},
		config: *c,
	}
	go db.workerLoop()
	return db, nil
}

// Close closes the database and all workers.
func (db *DB) Close() {
	close(db.closed)
}

// Key represents a single key with potentially multiple values.
type key struct {
	key         []byte
	transaction *transaction
	values      []value
}

func (k key) clone() key {
	k.values = append([]value{}, k.values...)
	return k
}

// byKey implements sort.Interface for []*key first based on key and then by
// the timestamp.
type byKey []*key

func (a byKey) Len() int      { return len(a) }
func (a byKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool {
	compare := bytes.Compare(a[i].key, a[j].key)
	if compare == 0 {
		return a[i].values[0].time.After(a[j].values[0].time)
	}
	return compare < 0
}

func (k *key) getAt(at time.Time) ([]byte, bool) {
	if k.transaction != nil && k.transaction.status != StatusCommitted {
		return nil, false
	}
	for _, v := range k.values {
		if at != zeroTime && at.Before(v.time) {
			continue
		}
		if v.tombstone {
			break
		}
		return v.value, true
	}
	return nil, false
}

// Value represents a value and the previous versions.
type value struct {
	value     []byte
	time      time.Time
	tombstone bool
}

type pageID int64

type page struct {
	id    pageID
	key   []byte
	keys  []*key
	left  pageID
	right pageID
}

// delta represents a single change to be applied to a page.
type delta struct {
	next *delta
	key  *key
	page *page
}

type unsafeDelta struct {
	next unsafe.Pointer
}

// Get gets a value from the database.
func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.GetAt(key, zeroTime)
}

// GetAt gets a value from the database at the specified time.
func (db *DB) GetAt(key []byte, at time.Time) ([]byte, bool) {
	id := rootPage
	delta := db.getPage(id).next
	deltaCount := 0
	defer func() {
		// Check if the depth is too high, and if so, queue for consolidation.
		if deltaCount > db.config.MaxDeltaCount {
			db.consolidateQueue <- id
		}
	}()
	for delta != nil {
		if (delta.key == nil) == (delta.page == nil) {
			panic("invariant: exactly one of delta.key, delta.page must be set")
		}

		if delta.page != nil { // Check page for match.
			page := delta.page
			if page.key != nil { // Index node
				if bytes.Compare(page.key, key) <= 0 {
					id = page.right
				} else {
					id = page.left
				}
				delta = db.getPage(id).next
			} else { // Data node
				for _, entry := range page.keys {
					if bytes.Equal(entry.key, key) {
						return entry.getAt(at)
					}
				}
				break
			}
		} else if delta.key != nil { // Check delta for match.
			deltaCount++
			txn := delta.key.transaction
			// Skip uncommitted keys.
			if txn != nil && txn.status != StatusCommitted {
				delta = delta.next
				continue
			}
			if bytes.Equal(key, delta.key.key) {
				return delta.key.getAt(at)
			}
			delta = delta.next
		}
	}
	return nil, false
}

// Put writes a value into the database.
// TODO(d4l3k): Do conflict checks.
func (db *DB) Put(k, v []byte) {
	for {
		id := rootPage
		page := db.getPage(id)
		d := page.next

		// Find the matching page.  We only have to check if the first page is nil
		// since index nodes won't have any deltas on top of them.
		for d.page != nil {
			if d.page.key == nil {
				break
			}

			if bytes.Compare(d.page.key, k) <= 0 {
				id = d.page.right
			} else {
				id = d.page.left
			}
			d = db.getPage(id).next
		}

		insert := delta{
			key: &key{
				key: k,
				values: []value{
					{
						value: v,
						time:  time.Now(),
					},
				},
			},
			next: d,
		}
		if db.savePageNext(id, d, &insert) {
			break
		}
		//log.Printf("failed to save %+v %+v %+v %+v %+v", id, db.pages[id].next, page.next, d, &insert)
	}
}

func (db *DB) getPage(id pageID) *delta {
	return db.pages[id-1]
}

func (db *DB) savePageNext(id pageID, old, new *delta) bool {
	page := db.getPage(id)
	unsafePage := (*unsafeDelta)(unsafe.Pointer(page))
	return atomic.CompareAndSwapPointer(&unsafePage.next, unsafe.Pointer(old), unsafe.Pointer(new))
}
