package huton

import "sync"

type Cache struct {
	mu    sync.RWMutex
	cache map[string][]byte
}

func NewCache() *Cache {
	return &Cache{
		cache: make(map[string][]byte),
	}
}

func (c *Cache) Get(key []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache[string(key)], nil
}

func (c *Cache) Set(key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[string(key)] = value
	return nil
}

func (c *Cache) Delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, string(key))
	return nil
}
