package huton

import (
	"github.com/boltdb/bolt"
	"path/filepath"
	"sync"
	"time"
)

type Cache struct {
	mu       sync.RWMutex
	db       *bolt.DB
	name     string
	instance *instance
}

func newCache(baseDir string, name string, instance *instance) (*Cache, error) {
	db, err := bolt.Open(filepath.Join(baseDir, "caches", name+".db"), 0644, &bolt.Options{
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Cache{
		db:       db,
		name:     name,
		instance: instance,
	}, nil
}

func (c *Cache) Get(key []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var b []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		b = bucket.Get(key)
		return nil
	})
	return b, err
}

func (c *Cache) Set(key, value []byte) error {
	return nil
}

func (c *Cache) Delete(key []byte) error {
	return nil
}
