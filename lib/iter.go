package huton

import (
	"bytes"
	"container/heap"
	"io"
)

// Iterator is an interface used to iterator over a collection.
type Iterator interface {
	// Next moves the iterator to the next k/v entry and will return ErrIteratorDone if it is finished.
	Next() (key, val []byte, err error)
}

type iterator struct {
	ss      *segmentStack
	cursors []*cursor
}

type cursor struct {
	idx int
	op  uint64
	sc  *segmentCursor
	k   []byte
	v   []byte
}

func (ss *segmentStack) iter() *iterator {
	ss.ensureSorted(0, len(ss.segments)-1)
	it := &iterator{
		ss:      ss,
		cursors: make([]*cursor, 0, len(ss.segments)+1),
	}
	for i := 0; i < len(ss.segments)-1; i++ {
		seg := ss.segments[i]
		c := seg.cursor()
		op, k, v := c.current()
		if op != 0 || k != nil || v != nil {
			it.cursors = append(it.cursors, &cursor{
				idx: i,
				sc:  c,
				op:  op,
				k:   k,
				v:   v,
			})
		}
	}
	heap.Init(it)
	return it
}

func (it *iterator) next() error {
	if len(it.cursors) <= 0 {
		return io.EOF
	}
	lastKey := it.cursors[0].k
	for len(it.cursors) > 0 {
		next := it.cursors[0]
		err := next.sc.next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			heap.Pop(it)
		} else {
			next.op, next.k, next.v = next.sc.current()
			if next.op == 0 {
				heap.Pop(it)
			} else if len(it.cursors) > 1 {
				heap.Fix(it, 0)
			}
		}
		if len(it.cursors) <= 0 {
			return io.EOF
		}
		if !iteratorBytesEqual(it.cursors[0].k, lastKey) {
			return nil
		}
	}
	return io.EOF
}

func (it *iterator) current() (uint64, []byte, []byte, error) {
	if len(it.cursors) <= 0 {
		return 0, nil, nil, io.EOF
	}
	c := it.cursors[0]
	if c.op == opDel {
		return 0, nil, nil, nil
	}
	return c.op, c.k, c.v, nil
}

func (it *iterator) Len() int {
	return len(it.cursors)
}

func (it *iterator) Less(i, j int) bool {
	c := bytes.Compare(it.cursors[i].k, it.cursors[j].k)
	if c < 0 {
		return true
	}
	if c > 0 {
		return false
	}
	return it.cursors[i].idx > it.cursors[j].idx
}

func (it *iterator) Swap(i, j int) {
	it.cursors[i], it.cursors[j] = it.cursors[j], it.cursors[i]
}

func (it *iterator) Push(x interface{}) {
	it.cursors = append(it.cursors, x.(*cursor))
}

func (it *iterator) Pop() interface{} {
	n := len(it.cursors)
	x := it.cursors[n-1]
	it.cursors = it.cursors[:n-1]
	return x
}

func iteratorBytesEqual(a, b []byte) bool {
	i := len(a)
	if i != len(b) {
		return false
	}
	for i > 0 { // Optimization to compare right-hand-side of keys first.
		i--
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
