package skeleton

import (
	"bytes"
	"unsafe"
)

// delta represents a single change to be applied to a page.
type delta struct {
	key  *key
	page *page
	next *delta
}

type unsafeDelta struct {
	_    *key
	_    *page
	next unsafe.Pointer
}

func (d delta) clone() *delta {
	return &d
}

// deltaCount returns the number of deltas on the current page.
func (d *delta) deltaCount() int {
	var count int
	for ; d.next != nil; d = d.next {
		if d.isPending() {
			continue
		}
		count++
	}
	return count
}

// hasPendingTxn returns whether the delta or it's children has a pending
// transaction on the specified key.
func (d *delta) hasPendingTxn(k []byte) *Txn {
	for ; d != nil; d = d.next {
		if d.key == nil || !bytes.Equal(d.key.key, k) {
			continue
		}
		if d.isPending() {
			return d.key.txn
		}
	}
	return nil
}

// isPending returns whether the current delta is part of a pending transaction.
func (d delta) isPending() bool {
	return d.key != nil && d.key.txn != nil && d.key.txn.status == StatusPending
}

// getPage walks the delta and returns the page from the last element.
func (d *delta) getPage() *page {
	for ; d.next != nil; d = d.next {
		if d.page != nil {
			panic("only last element should have a page")
		}
	}
	return d.page
}
