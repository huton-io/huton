package huton

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"sync"
)

type Config struct {
	Serf  SerfConfig
	Raft  RaftConfig
	Peers []string
}

func DefaultConfig() Config {
	return Config{
		Serf: SerfConfig{
			BindAddr: "0.0.0.0",
			BindPort: 5557,
		},
		Raft: RaftConfig{
			BindAddr:            "0.0.0.0",
			BindPort:            5567,
			RetainSnapshotCount: 2,
			MaxPool:             3,
			Timeout:             "10s",
		},
	}
}

type Instance interface {
	Cache(name string) *Cache
	Members() []string
	Close() error
}

type instance struct {
	serf             *serf.Serf
	serfEventChannel chan serf.Event
	shutdownCh       chan struct{}
	cacheMu          sync.Mutex
	config           *Config
	caches           map[string]*Cache
	raft             *raft.Raft
}

func NewInstance(config *Config) (Instance, error) {
	i := &instance{
		serfEventChannel: make(chan serf.Event),
		config:           config,
		caches:           make(map[string]*Cache),
	}
	if err := i.setupRaft(); err != nil {
		return nil, err
	}
	if err := i.setupSerf(); err != nil {
		return nil, err
	}
	go i.handleEvents()
	return i, nil
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
	i.shutdownCh <- struct{}{}
	close(i.shutdownCh)
	if err := i.serf.Leave(); err != nil {
		return err
	}
	if err := i.serf.Shutdown(); err != nil {
		return err
	}
	if err := i.raft.Shutdown().Error(); err != nil {
		return err
	}
	return nil
}

func (i *instance) handleEvents() {
	for {
		select {
		case e := <-i.serfEventChannel:
			i.handleSerfEvent(e)
		case <-i.shutdownCh:
			return
		}
	}
}
