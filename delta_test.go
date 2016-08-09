package skeleton

import (
	"reflect"
	"testing"
)

func TestDeltaIsPending(t *testing.T) {
	testCases := []struct {
		d        *delta
		expected bool
	}{
		{
			&delta{},
			false,
		},
		{
			&delta{key: &key{}},
			false,
		},
		{
			&delta{
				key: &key{
					txn: &Txn{},
				},
			},
			false,
		},
		{
			&delta{
				key: &key{
					txn: &Txn{
						status: StatusPending,
					},
				},
			},
			true,
		},
		{
			&delta{
				key: &key{
					txn: &Txn{
						status: StatusCommitted,
					},
				},
			},
			false,
		},
	}

	for _, tc := range testCases {
		if out := tc.d.isPending(); out != tc.expected {
			t.Errorf("%v.isPending() = %v; not %v", tc.d, out, tc.expected)
		}
	}
}

func TestDeltaGetPage(t *testing.T) {
	testCases := []struct {
		d        *delta
		expected *page
	}{
		{
			&delta{},
			nil,
		},
		{
			&delta{page: &page{key: []byte("foo")}},
			&page{key: []byte("foo")},
		},
		{
			&delta{next: &delta{page: &page{key: []byte("foo")}}},
			&page{key: []byte("foo")},
		},
	}

	for _, tc := range testCases {
		if out := tc.d.getPage(); !reflect.DeepEqual(out, tc.expected) {
			t.Errorf("%v.getPage() = %v; not %v", tc.d, out, tc.expected)
		}
	}
}

func TestDeltaHasPendingTxn(t *testing.T) {
	testCases := []struct {
		d        *delta
		expected bool
	}{
		{
			&delta{},
			false,
		},
		{
			&delta{key: &key{}},
			false,
		},
		{
			&delta{
				key: &key{
					txn: &Txn{},
				},
			},
			false,
		},
		{
			&delta{
				key: &key{
					txn: &Txn{
						status: StatusPending,
					},
				},
			},
			true,
		},
		{
			&delta{
				next: &delta{
					key: &key{
						txn: &Txn{
							status: StatusCommitted,
						},
					},
				},
			},
			false,
		},
		{
			&delta{
				next: &delta{
					key: &key{
						txn: &Txn{
							status: StatusCommitted,
						},
					},
				},
			},
			false,
		},
	}

	for i, tc := range testCases {
		if out := tc.d.hasPendingTxn(); out != tc.expected {
			t.Errorf("%d: %v.hasPendingTxn() = %v; not %v", i, tc.d, out, tc.expected)
		}
	}
}
