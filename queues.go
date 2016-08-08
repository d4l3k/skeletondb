package skeleton

import (
	"bytes"
	"log"
	"sort"
)

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
