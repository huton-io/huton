package huton

import (
	"github.com/hashicorp/serf/serf"
	"sync"
)

type Instance interface {
	Cache(name string) *Cache
}

type instance struct {
	serf             *serf.Serf
	serfEventChannel chan serf.Event
	cacheMu          sync.Mutex
	config           *Config
	caches           map[string]*Cache
}

func NewInstance(config *Config) (Instance, error) {
	i := &instance{
		serfEventChannel: make(chan serf.Event),
		config:           config,
		caches:           make(map[string]*Cache),
	}
	err := i.setupSerf()
	return i, err
}

func (i *instance) Cache(name string) *Cache {
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c
	}
	return NewCache()
}
