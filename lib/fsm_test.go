package huton

import (
	"bytes"
	"fmt"
	"testing"
)

type mockSnapshotSink struct {
	contents *bytes.Buffer
}

func (m *mockSnapshotSink) Write(p []byte) (int, error) {
	return m.contents.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "test"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func (m *mockSnapshotSink) Read(p []byte) (int, error) {
	return m.contents.Read(p)
}

func newMockSnapshotSink(b []byte) *mockSnapshotSink {
	return &mockSnapshotSink{
		contents: bytes.NewBuffer(b),
	}
}

func TestFSMSnapshot(t *testing.T) {
	sink := newMockSnapshotSink([]byte{})
	i := &Instance{
		caches:    make(map[string]*Cache),
		compactor: func(*Cache, chan<- error, <-chan struct{}) {},
	}
	i2 := &Instance{
		caches:    make(map[string]*Cache),
		compactor: func(*Cache, chan<- error, <-chan struct{}) {},
	}
	primeCache("1", i, t)
	primeCache("2", i, t)
	ss, err := i.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	err = ss.Persist(sink)
	if err != nil {
		t.Fatalf("Failed to persist Snapshot: %v", err)
	}
	err = i2.Restore(sink)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}
	compareInstances(i, i2, t)
}

func primeCache(name string, i *Instance, t *testing.T) {
	c := i.Cache(name)
	batch := c.NewBatch(2, 1000).(*segment)
	batch.Set([]byte("test1"), []byte("test1Val"))
	batch.Set([]byte("test12"), []byte("test2Val"))
	err := c.executeSegment(batch)
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}
}

func compareInstances(i, i2 *Instance, t *testing.T) {
	for name, c := range i.caches {
		fmt.Println(name)
		c2, ok := i2.caches[name]
		if !ok {
			t.Fatalf("Cache does not exist in second instance.")
		}
		compareCaches(c, c2, t)
	}
}

func compareCaches(c, c2 *Cache, t *testing.T) {
	if c.name != c2.name {
		t.Fatalf("Cache name mismatch: %s and %s", c.name, c2.name)
	}
	if len(c.stack.segments) != len(c2.stack.segments) {
		t.Fatalf("Cache stacks size mismatch: %d and %d", len(c.stack.segments), len(c2.stack.segments))
	}
	for i, seg := range c.stack.segments {
		seg2 := c2.stack.segments[i]
		if len(seg.buf) != len(seg2.buf) {
			t.Fatalf("Segment buffer size mismatch: %d and %d", len(seg.buf), len(seg2.buf))
		}
		if len(seg.meta) != len(seg2.meta) {
			t.Fatalf("Segment meta size mismatch: %d and %d", len(seg.meta), len(seg2.meta))
		}
		for i, b := range seg.buf {
			if b != seg2.buf[i] {
				t.Fatalf("Segment bufs not equal")
			}
		}
		for i, m := range seg.meta {
			if m != seg2.meta[i] {
				t.Fatalf("Segment meta not equal")
			}
		}
	}
}
