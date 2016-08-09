package skeleton

import "unsafe"

// delta represents a single change to be applied to a page.
type delta struct {
	next *delta
	key  *key
	page *page
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
// transaction.
func (d *delta) hasPendingTxn() bool {
	for ; d != nil; d = d.next {
		if d.isPending() {
			return true
		}
	}
	return false
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

type unsafeDelta struct {
	next unsafe.Pointer
}
