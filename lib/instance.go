package huton

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"sync"
)

type Config struct {
	Serf    SerfConfig
	Raft    RaftConfig
	Peers   []string
	BaseDir string
	Name    string
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
	Cache(name string) (*Cache, error)
	Peers() []*Peer
	Local() *Peer
	Close() error
}

type instance struct {
	id                  string
	serf                *serf.Serf
	raft                *raft.Raft
	raftJSONPeers       *raft.JSONPeers
	raftBoltStore       *raftboltdb.BoltStore
	serfEventChannel    chan serf.Event
	shutdownCh          chan struct{}
	peersMu             sync.Mutex
	peers               map[string]*Peer
	cacheMu             sync.Mutex
	config              *Config
	caches              map[string]*Cache
}

func NewInstance(config *Config) (Instance, error) {
	addr, err := config.Serf.Addr()
	if err != nil {
		return nil, err
	}
	i := &instance{
		id:               fmt.Sprintf("%s:%d", addr.IP.String(), addr.Port),
		serfEventChannel: make(chan serf.Event),
		shutdownCh:       make(chan struct{}),
		peers:            make(map[string]*Peer),
		config:           config,
		caches:           make(map[string]*Cache),
	}
	if err := i.setupSerf(addr); err != nil {
		return nil, err
	}
	if err := i.setupRaft(); err != nil {
		return nil, err
	}
	go i.handleEvents()
	return i, nil
}

func (i *instance) Cache(name string) (*Cache, error) {
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c, nil
	}
	return newCache(i.config.BaseDir, name, i)
}

func (i *instance) Close() error {
	if i.serf != nil {
		if err := i.serf.Leave(); err != nil {
			return err
		}
	}
	if i.raft != nil {
		i.raftBoltStore.Close()
		if err := i.raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	close(i.shutdownCh)
	return nil
}

func (i *instance) handleEvents() {
	for {
		select {
		case e := <-i.serfEventChannel:
			i.handleSerfEvent(e)
		case <-i.serf.ShutdownCh():
			i.Close()
		case <-i.shutdownCh:
			return
		}
	}
}

func (i *instance) isLeader() bool {
	return i.raft.State() == raft.Leader
}
