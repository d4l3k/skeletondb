package skeleton

import (
	"bytes"
	"errors"
	"log"
	"os"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"
)

// DefaultConfig options.
var DefaultConfig = Config{
	MaxKeysPerNode: 100,
	MaxDeltaCount:  10,
	GCTime:         24 * time.Hour,
}

// Config holds configuration options for DB.
type Config struct {
	MaxKeysPerNode, MaxDeltaCount int
	// GCTime is the amount of time until data is garbage collected.
	GCTime time.Duration
}

// Verify returns an error if an invariant is violated.
func (c Config) Verify() error {
	if c.MaxKeysPerNode <= 0 {
		return errors.New("MaxKeysPerNode must be positive")
	}
	if c.MaxDeltaCount <= 0 {
		return errors.New("MaxDeltaCount must be positive")
	}
	return nil
}

// Errors...
var (
	ErrTxnConflict = errors.New("an error occurred while committing the transaction")
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

// workerLoop process the split and consolidate queues.
func (db *DB) workerLoop() {
	for {
		select {
		case <-db.closed:
			return
		case id := <-db.splitQueue:
			db.split(id)
		case id := <-db.consolidateQueue:
			db.consolidate(id)
		}
	}
}

// consolidate consolidates deltas for that page.
func (db *DB) consolidate(id pageID) {
	var newPage page
	for {
		root := db.getPage(id).next

		// Count deltas to ensure that we don't do unnecessary work.
		deltaCount := 0
		for d := root; d != nil; d = d.next {
			if d.key != nil && (d.key.transaction == nil || d.key.transaction.status == StatusCommitted) {
				deltaCount++
			}
		}
		if deltaCount <= db.config.MaxDeltaCount {
			return
		}
		log.Printf("consolidate %+v: start", id)

		// Build slice of deltas sorted by their keys.
		var page *page
		var keys []*key
		for d := root; d != nil; d = d.next {
			if d.key != nil && (d.key.transaction == nil || d.key.transaction.status == StatusCommitted) {
				keys = append(keys, d.key)
			}
			if d.page != nil {
				page = d.page
			}
		}
		sort.Sort(byKey(keys))

		if page.key != nil {
			panic("invariant: index node must not have deltas")
		}

		// TODO(d4l3k): Optimize memory allocations and copies.
		newPage = *page
		newPage.keys = make([]*key, 0, len(page.keys)+len(keys))
		var i, j int
		for i < len(page.keys) || j < len(keys) {

			if i < len(page.keys) && (j >= len(keys) || bytes.Compare(page.keys[i].key, keys[j].key) <= 0) {
				newKey := page.keys[i].clone()
				newPage.keys = append(newPage.keys, &newKey)
				i++
			} else if j < len(keys) {
				b := keys[j]
				var prevKey key
				if len(newPage.keys) > 0 {
					prevKey = *newPage.keys[len(newPage.keys)-1]
				}
				if bytes.Equal(b.key, prevKey.key) {
					prevKey.values = append(append([]value{}, b.values...), prevKey.values...)
				} else {
					newKey := b.clone()
					newPage.keys = append(newPage.keys, &newKey)
				}
				j++
			}
		}

		newRoot := &delta{page: &newPage}
		if db.savePageNext(id, root, newRoot) {
			log.Printf("consolidate %+v: finish", id)
			break
		}
		log.Printf("consolidate %+v: conflict, retrying", id)
	}

	// Schedule node for splitting if it's too large.
	if len(newPage.keys) > db.config.MaxKeysPerNode {
		select {
		case db.splitQueue <- newPage.id:
		default:
		}
	}
}

// split splits a page.
func (db *DB) split(id pageID) {
	log.Printf("splitting %+v", id)
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
