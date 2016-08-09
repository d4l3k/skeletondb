package skeleton

import (
	"bytes"
	"log"
	"sort"
)

// workerLoop process the various queues.
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
		deltaCount := root.deltaCount()
		if deltaCount <= db.config.MaxDeltaCount {
			return
		}
		log.Printf("consolidate %+v: start", id)

		// Build slice of deltas sorted by their keys.
		var page *page
		var keys []*key
		// Also keep track of deltas that can't be merged.
		var head, tail *delta

		for d := root; d != nil; d = d.next {
			if d.key != nil {
				// This does some subtle things with transactions.
				// - Merge committed transactions.
				// - Keep pending transactions as deltas for easier cleanup.
				// - Discard aborted transactions.
				// - Discard read intents.
				if d.key.txn == nil || d.key.txn.status == StatusCommitted {
					if !d.key.read {
						keys = append(keys, d.key)
					}
				} else if d.key.txn.status == StatusPending {
					d2 := d.clone()
					d2.next = nil
					if tail != nil {
						tail.next = d2
					}
					if head == nil {
						head = d2
					}
					tail = d2
				}
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
		for i, j := 0, 0; i < len(page.keys) || j < len(keys); {
			if i < len(page.keys) && (j >= len(keys) || bytes.Compare(page.keys[i].key, keys[j].key) <= 0) {
				newKey := page.keys[i].clone()
				newPage.keys = append(newPage.keys, &newKey)
				i++
			} else if j < len(keys) {
				b := keys[j]
				var prevKey *key
				if len(newPage.keys) > 0 {
					prevKey = newPage.keys[len(newPage.keys)-1]
				}
				if prevKey != nil && bytes.Equal(b.key, prevKey.key) {
					prevKey.values = append(append([]value{}, b.values...), prevKey.values...)
				} else {
					newKey := b.clone()
					newPage.keys = append(newPage.keys, &newKey)
				}
				j++
			}
		}

		newRoot := &delta{page: &newPage}
		if head == nil {
			head = newRoot
		} else {
			tail.next = newRoot
		}
		if db.savePageNext(id, root, head) {
			log.Printf("consolidate %+v: finish, merged %d, key count = %d", id, deltaCount, len(newPage.keys))
			break
		}
		log.Printf("consolidate %+v: conflict, retrying", id)
	}
	db.maybeQueueSplit(newPage)
}

func (db *DB) maybeQueueSplit(p page) {
	// Schedule node for splitting if it's too large.
	if len(p.keys) > db.config.MaxKeysPerNode {
		select {
		case db.splitQueue <- p.id:
		default:
		}
	}
}

// split splits a page.
func (db *DB) split(id pageID) {
	log.Printf("split %+v: scheduled", id)
	for {
		root := db.getPage(id).next
		p := root.getPage()
		// Count keys to ensure that we don't do unnecessary work.
		if len(p.keys) <= db.config.MaxKeysPerNode {
			return
		}
		log.Printf("split %+v: start, key count = %d", id, len(p.keys))

		mid := len(p.keys) / 2
		midKey := p.keys[mid].key
		left := page{
			id:   db.nextPageID(),
			keys: p.keys[:mid],
		}
		leftPage := &delta{
			page: &left,
		}
		right := page{
			id:   db.nextPageID(),
			keys: p.keys[mid:],
		}
		rightPage := &delta{
			page: &right,
		}
		newPage := page{
			id:    p.id,
			key:   midKey,
			left:  left.id,
			right: right.id,
		}

		// Move any existing deltas onto the new children.
		for d := root; d.next != nil; d = d.next {
			d2 := d.clone()
			if bytes.Compare(midKey, d2.key.key) <= 0 {
				d2.next = rightPage
				rightPage = d2
			} else {
				d2.next = leftPage
				leftPage = d2
			}
		}

		// These sets don't have to be atomic since these IDs haven't been used yet.
		db.getPage(left.id).next = leftPage
		db.getPage(right.id).next = rightPage

		newRoot := &delta{page: &newPage}
		if db.savePageNext(id, root, newRoot) {
			log.Printf("split %+v: finish", id)
			db.maybeQueueSplit(left)
			db.maybeQueueSplit(right)
			break
		}
		log.Printf("split %+v: conflict, retrying", id)
		db.pageIDPool <- left.id
		db.pageIDPool <- right.id
	}
}
