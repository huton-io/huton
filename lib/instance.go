package huton

import (
	"fmt"
	"github.com/hashicorp/serf/serf"
	"sync"
)

type Instance interface {
	Cache(name string) *Cache
	Members() []string
	Close() error
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

func (i *instance) Members() []string {
	me := i.serf.LocalMember()
	members := i.serf.Members()
	peers := make([]string, 0, len(members))
	for _, member := range members {
		if member.Addr.String() != me.Addr.String() || member.Port != me.Port {
			peers = append(peers, fmt.Sprintf("%s:%d", member.Addr.String(), member.Port))
		}
	}
	return peers
}

func (i instance) Close() error {
	if err := i.serf.Leave(); err != nil {
		return err
	}
	return i.serf.Shutdown()
}
