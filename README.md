# skeletondb [![GoDoc](https://godoc.org/github.com/d4l3k/skeletondb?status.svg)](https://godoc.org/github.com/d4l3k/skeletondb) [![Build Status](https://travis-ci.org/d4l3k/skeletondb.svg?branch=master)](https://travis-ci.org/d4l3k/skeletondb)

SkeletonDB is a lock-less thread safe MVCC store.


## Implementation
Skeleton uses a
[bw-tree](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
which is a parallel data structure that only uses atomic operations for high
performance parallel operations.

## License

Licensed under the MIT license. See the LICENSE file for more information.
