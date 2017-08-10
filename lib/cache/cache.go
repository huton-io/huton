package cache

import (
	"errors"
	"sync"
)

const (
	maxBatchBufSize = 0
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
	Get(key []byte) []byte
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
	for c.stack.segments != nil && len(c.stack.segments) >= maxBatchBufSize {
		c.stackDirtyCond.Wait()
	}
	c.pushToStack(seg)
	seg.markCommitted()
	return nil
}

func (c *cache) Get(key []byte) []byte {
	c.mu.Lock()
	stack := c.stack
	c.mu.Unlock()
	return stack.Get(key)
}

func (c *cache) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &segmentStack{
		segments: make([]*segment, 0, len(c.stack.segments)),
	}
}

func (c *cache) pushToStack(seg *segment) {
	if c.stack == nil {
		c.stack = &segmentStack{
			segments: make([]*segment, 0, 1),
		}
	}
	c.stack.segments = append(c.stack.segments, seg)
}
