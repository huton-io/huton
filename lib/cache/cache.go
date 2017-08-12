package cache

import (
	"errors"
	"sync"
)

var (
	ErrWrongBatchType = errors.New("wrong batch implementation type")
)

type Batch interface {
	Set(key, val []byte) error
	Del(key []byte) error
}

type Snapshot interface {
	Get(key []byte) []byte
}

type Cache interface {
	NewBatch(totalOps, totalBufSize int) Batch
	ExecuteBatch(batch Batch) error
	Snapshot() Snapshot
}

type cache struct {
	stack          *segmentStack
	mu             sync.Mutex
	stackDirtyCond *sync.Cond
}

func (c *cache) NewBatch(totalOps, totalBufSize int) Batch {
	return newSegment(totalOps, totalBufSize)
}

func (c *cache) ExecuteBatch(batch Batch) error {
	seg, ok := batch.(*segment)
	if !ok {
		return ErrWrongBatchType
	}
	if seg.isEmpty() {
		return nil
	}
	seg.Sort()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushToStack(seg)
	seg.markCommitted()
	return nil
}

func (c *cache) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &segmentStack{
		segments: make([]*segment, 0, len(c.stack.segments)),
	}
	s.segments = append(s.segments, c.stack.segments...)
	return s
}

func (c *cache) pushToStack(seg *segment) {
	if c.stack == nil {
		c.stack = &segmentStack{
			segments: make([]*segment, 0, 1),
		}
	}
	c.stack.segments = append(c.stack.segments, seg)
}

func NewCache() Cache {
	c := &cache{}
	c.stackDirtyCond = sync.NewCond(&c.mu)
	return c
}
