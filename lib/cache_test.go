package huton

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestCacheSet(t *testing.T) {
	c := newCache("test", nil)
	b := c.NewBatch(2, 1000).(*segment)
	err := b.Set([]byte("test"), []byte("testVal"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = b.Set([]byte("test2"), []byte("testVal2"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = c.executeSegment(b)
	if err != nil {
		t.Errorf("Err while executing batch: %s", err)
	}
	val := c.Snapshot().Get([]byte("test"))
	if string(val) != "testVal" {
		t.Errorf("value %s not equal to 'testVal'", string(val))
	}
	val = c.Snapshot().Get([]byte("test2"))
	if string(val) != "testVal2" {
		t.Errorf("value %s not equal to 'testVal2'", string(val))
	}
}

func TestCacheDel(t *testing.T) {
	c := newCache("test", nil)
	b := c.NewBatch(2, 1000).(*segment)
	err := b.Set([]byte("test"), []byte("testVal"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = b.Set([]byte("test2"), []byte("testVal2"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = c.executeSegment(b)
	if err != nil {
		t.Errorf("Err while executing batch: %s", err)
	}
	b = c.NewBatch(1, 1000).(*segment)
	err = b.Del([]byte("test"))
	if err != nil {
		t.Errorf("Err while deleting key: %s", err)
	}
	err = c.executeSegment(b)
	if err != nil {
		t.Errorf("Err while executing batch: %s", err)
	}
	val := c.Snapshot().Get([]byte("test2"))
	if string(val) != "testVal2" {
		t.Errorf("value %s not equal to 'testVal2'", string(val))
	}
	val = c.Snapshot().Get([]byte("test"))
	if val != nil {
		t.Errorf("Expected nil val but got %v", val)
	}
}

func TestCacheCompact(t *testing.T) {
	instance := &instance{logger: log.New(ioutil.Discard, "", 0)}
	c := newCache("test", instance)
	b := c.NewBatch(2, 1000).(*segment)
	err := b.Set([]byte("test"), []byte("testVal"))
	if err != nil {
		t.Errorf("Failed to set val: %s", err)
	}
	err = b.Set([]byte("test2"), []byte("testVal2"))
	if err != nil {
		t.Errorf("Failed to set val: %s", err)
	}
	err = c.executeSegment(b)
	if err != nil {
		t.Errorf("failed to execute batch: %s", err)
	}
	b = c.NewBatch(2, 1000).(*segment)
	err = b.Set([]byte("test3"), []byte("testVal3"))
	if err != nil {
		t.Errorf("Failed to set val: %s", err)
	}
	err = b.Set([]byte("test4"), []byte("testVal4"))
	if err != nil {
		t.Errorf("Failed to set val: %s", err)
	}
	err = c.executeSegment(b)
	if err != nil {
		t.Errorf("failed to execute batch: %s", err)
	}
	if len(c.stack.segments) != 2 {
		t.Errorf("unexpected stack size %d != 2", len(c.stack.segments))
	}
	err = c.Compact()
	if err != nil {
		t.Errorf("compaction failed: %s", err)
	}
	if len(c.stack.segments) != 1 {
		t.Errorf("unexpected stack size %d != 1", len(c.stack.segments))
	}
}
