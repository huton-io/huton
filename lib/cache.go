package huton

import (
	"errors"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/huton-io/huton/lib/proto"
)

var (
	// ErrWrongBatchType is an error used when the provided implementation of Batch is unexpected.
	ErrWrongBatchType = errors.New("wrong batch implementation type")
)

// Batch is an interface for bulk operations on a cache. Batches can be continuously updated until
// they are executed, after which they become immutable.
type Batch interface {
	// Set updates key with val. An error is returned if the value cannot be set.
	Set(key, val []byte) error
	// Del deletes key from the cache. An error is returned if the key-value pair cannot be deleted.
	Del(key []byte) error
}

// Snapshot is a read-only view of the cache. Operations performed on the cache the snapshot is taken will
// not be reflected in the snapshot.
type Snapshot interface {
	// Get retrieves the value for key from the snapshot.
	Get(key []byte) []byte
}

// Cache is an in-memory key-value store.
type Cache interface {
	// Name returns the name of the cache.
	Name() string
	// NewBatch creates and returns a new Batch supporting totalOps operations and a buffer size of totalBufSize.
	// All mutations on a Cache must go through a Batch, even if it's only one.
	NewBatch(totalOps, totalBufSize int) Batch
	// ExecuteBatch executes all operations in a Batch in sequence. Execution is replicated on all nodes of the cluster asynchronously.
	// An error is returned if the issuance of replication for the batch could not be completed.
	ExecuteBatch(batch Batch) error
	// Snapshot creates a Snapshot of the cache. All reads from the cache must be performed from a Snapshot(). This ensures that the data
	// isn't mutated in the middle of a read.
	Snapshot() Snapshot
	// Compact performs compaction on the cache. This operation is immediate and blocking. It should not be used unless you really know what you are doing.
	// Instead, a Compactor should be used to manage compaction in the background.
	Compact() error
}

type cache struct {
	name     string
	instance *instance
	stack    *segmentStack
	mu       sync.Mutex
}

func (c *cache) Name() string {
	return c.name
}

func (c *cache) NewBatch(totalOps, totalBufSize int) Batch {
	return newSegment(totalOps, totalBufSize)
}

func (c *cache) ExecuteBatch(batch Batch) error {
	seg, ok := batch.(*segment)
	if !ok {
		return ErrWrongBatchType
	}
	cacheOp, err := proto.Marshal(&huton_proto.CacheBatch{
		CacheName: &c.name,
		Buf:       seg.buf,
		Meta:      seg.meta,
	})
	if err != nil {
		return err
	}
	t := typeCacheExecute
	cmd := &huton_proto.Command{
		Type: &t,
		Body: cacheOp,
	}
	return c.instance.apply(cmd)
}

func (c *cache) executeSegment(seg *segment) error {
	if seg.isEmpty() {
		return nil
	}
	seg.readyDeferredSort()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushToStack(seg)
	return nil
}

func (c *cache) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &segmentStack{}
	if c.stack != nil {
		s.segments = make([]*segment, 0, len(c.stack.segments))
		s.segments = append(s.segments, c.stack.segments...)
	}
	return s
}

func (c *cache) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stack == nil {
		c.instance.logger.Printf("[INFO]: Nothing to compact for cache %s. Skipping compaction for this cache.", c.name)
		return nil
	}
	c.instance.logger.Println("[INFO]: Starting compaction")
	it := c.stack.iter()
	seg := newSegment(0, 0)
	for op, k, v, err := it.current(); err != io.EOF; err = it.next() {
		if err != nil {
			c.instance.logger.Println("[ERR]: Failed during compaction iteration.")
			return err
		}
		if k != nil && v != nil {
			if err := seg.mutate(op, k, v); err != nil {
				c.instance.logger.Println("[ERR]: Failed mutation during compaction.")
				return err
			}
		}
	}
	c.stack = &segmentStack{
		segments: []*segment{seg},
	}
	c.instance.logger.Println("[INFO]: Compaction completed successfully.")
	return nil
}

func (c *cache) pushToStack(seg *segment) {
	if c.stack == nil {
		c.stack = &segmentStack{
			segments: make([]*segment, 0, 1),
		}
	}
	c.stack.segments = append(c.stack.segments, seg)
}

func newCache(name string, inst *instance) *cache {
	c := &cache{
		name:     name,
		instance: inst,
	}
	return c
}
