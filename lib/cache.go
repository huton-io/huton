package huton

import (
	"github.com/boltdb/bolt"
	"path/filepath"
	"sync"
	"time"
)

type Cache struct {
	mu   sync.RWMutex
	db   *bolt.DB
	name string
}

func NewCache(baseDir string, name string) (*Cache, error) {
	db, err := bolt.Open(filepath.Join(baseDir, "caches", name+".db"), 0644, &bolt.Options{
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Cache{
		db:   db,
		name: name,
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.name))
		if err != nil {
			return err
		}
		return bucket.Put(key, value)
	})
}

func (c *Cache) Delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		return bucket.Delete(key)
	})
}
