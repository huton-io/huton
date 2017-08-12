package cache

import (
	"testing"
)

func TestCacheSet(t *testing.T) {
	c := NewCache()
	b := c.NewBatch(2, 1000)
	err := b.Set([]byte("test"), []byte("testVal"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = b.Set([]byte("test2"), []byte("testVal2"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = c.ExecuteBatch(b)
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
	c := NewCache()
	b := c.NewBatch(2, 1000)
	err := b.Set([]byte("test"), []byte("testVal"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = b.Set([]byte("test2"), []byte("testVal2"))
	if err != nil {
		t.Errorf("Err while setting kvp: %s", err)
	}
	err = c.ExecuteBatch(b)
	if err != nil {
		t.Errorf("Err while executing batch: %s", err)
	}
	b = c.NewBatch(1, 1000)
	err = b.Del([]byte("test"))
	if err != nil {
		t.Errorf("Err while deleting key: %s", err)
	}
	err = c.ExecuteBatch(b)
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
