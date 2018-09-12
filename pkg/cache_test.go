package huton

import (
	"testing"
)

func TestCacheSet(t *testing.T) {
	c, _ := newCache("test", nil)
	err := c.executeSet([]byte("key"), []byte("val"))
	if err != nil {
		t.Errorf("Got error setting cache value: %s", err)
	}
}

func TestCacheDel(t *testing.T) {
	c, _ := newCache("test", nil)
	err := c.executeDelete([]byte("key"))
	if err != nil {
		t.Errorf("Error deleting key from cache: %s", err)
	}
}
