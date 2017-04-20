package huton

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/jonbonazza/huton/lib/proto"
)

// Cache is an interface for a cache, providing basic caching operations
type Cache interface {
	// Get retrieves the entry with the given key from the cache, if one exists and calls f, passing in the value.
	Get(key []byte, f func(val []byte)) error
	// Set puts a key-value-pair into the cache. If an entry with key already exists, it is overwritten.
	Set(key, value []byte) error
	// Delete deletes an entry with the given key from the cache, if one exists.
	Delete(key []byte) error
}

type cache struct {
	db       *bolt.DB
	name     string
	instance *instance
}

func newCache(cachesDB *bolt.DB, name string, instance *instance) (Cache, error) {
	err := cachesDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(name))
		if err == bolt.ErrBucketExists {
			err = nil
		}
		return err
	})
	return &cache{
		db:       cachesDB,
		name:     name,
		instance: instance,
	}, err
}

func (c *cache) Get(key []byte, f func(val []byte)) error {
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		b := bucket.Get(key)
		f(b)
		return nil
	})
	return err
}

func (c *cache) Set(key, value []byte) error {
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

func (c *cache) set(key, value []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		return bucket.Put(key, value)
	})
}

func (c *cache) Delete(key []byte) error {
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

func (c *cache) delete(key []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.name))
		return bucket.Delete(key)
	})
}
