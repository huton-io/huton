package huton

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/jonbonazza/huton/lib/proto"
	"sync"
)

type Cache struct {
	mu       sync.RWMutex
	db       *bolt.DB
	name     string
	instance *instance
}

func newCache(cachesDB *bolt.DB, name string, instance *instance) (*Cache, error) {
	err := cachesDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(name))
		if err == bolt.ErrBucketExists {
			err = nil
		}
		return err
	})
	return &Cache{
		db:       cachesDB,
		name:     name,
		instance: instance,
	}, err
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
	putCmd := &huton_proto.CachePutCommand{
		CacheName: &c.name,
		Key:       key,
		Value:     value,
	}
	b, err := proto.Marshal(putCmd)
	if err != nil {
		return fmt.Errorf("Failed to marshal put command to protobuf when setting value in cache %s: %s", c.name, err)
	}
	t := typeCachePut
	cmd := &huton_proto.Command{
		Type: &t,
		Body: b,
	}
	return c.instance.apply(cmd)
}

func (c *Cache) set(key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		return bucket.Put(key, value)
	})
}

func (c *Cache) Delete(key []byte) error {
	delCmd := &huton_proto.CacheDeleteCommand{
		CacheName: &c.name,
		Key:       key,
	}
	b, err := proto.Marshal(delCmd)
	if err != nil {
		return fmt.Errorf("Failed to marshal del command to protobuf when deleting value from cache %s: %s", c.name, err)
	}
	t := typeCacheDelete
	cmd := &huton_proto.Command{
		Type: &t,
		Body: b,
	}
	return c.instance.apply(cmd)
}

func (c *Cache) delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		return bucket.Delete(key)
	})
}
