package huton

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"net"
	"os"
	"strconv"
	"sync"
)

type Config struct {
	Serf    *serf.Config
	Raft    *RaftConfig
	Peers   []string
	BaseDir string
}

func DefaultConfig() *Config {
	c := &Config{
		Serf: serf.DefaultConfig(),
		Raft: &RaftConfig{
			Config:              raft.DefaultConfig(),
			RetainSnapshotCount: 2,
			MaxPool:             3,
			TransportTimeout:    "10s",
			Writer:              os.Stdout,
		},
		Peers: make([]string, 0),
	}
	c.Raft.ShutdownOnRemove = false
	return c
}

type Instance interface {
	Cache(name string) (*Cache, error)
	Peers() []*Peer
	Local() *Peer
	Shutdown() error
}

type instance struct {
	id               string
	serf             *serf.Serf
	raft             *raft.Raft
	raftJSONPeers    *raft.JSONPeers
	raftBoltStore    *raftboltdb.BoltStore
	raftTransport    *raft.NetworkTransport
	serfEventChannel chan serf.Event
	shutdownCh       chan struct{}
	peersMu          sync.Mutex
	peers            map[string]*Peer
	cacheMu          sync.Mutex
	config           *Config
	caches           map[string]*Cache
}

func NewInstance(config *Config) (Instance, error) {
	host := net.JoinHostPort(config.Serf.MemberlistConfig.BindAddr, strconv.Itoa(config.Serf.MemberlistConfig.BindPort))
	i := &instance{
		id:               host,
		serfEventChannel: make(chan serf.Event),
		shutdownCh:       make(chan struct{}),
		peers:            make(map[string]*Peer),
		config:           config,
		caches:           make(map[string]*Cache),
	}
	raftAddr := &net.TCPAddr{
		IP:   net.ParseIP(config.Serf.MemberlistConfig.BindAddr),
		Port: config.Serf.MemberlistConfig.BindPort + 1,
	}
	if err := i.setupSerf(config.Serf, raftAddr); err != nil {
		i.Shutdown()
		return nil, err
	}
	i.peersMu.Lock()
	members := i.serf.Members()
	for _, member := range members {
		p, err := newPeer(member)
		if err != nil {
			i.Shutdown()
			return nil, err
		}
		i.peers[p.RaftAddr.String()] = p
	}
	i.peersMu.Unlock()
	if err := i.setupRaft(); err != nil {
		i.Shutdown()
		return nil, err
	}
	// Wait for initial leader state
	for {
		if i.raft.Leader() != "" {
			break
		}
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

func (i *instance) Shutdown() error {
	if i.serf != nil {
		if err := i.serf.Leave(); err != nil {
			return err
		}
	}
	if i.raftBoltStore != nil {
		if err := i.raftBoltStore.Close(); err != nil {
			return err
		}
	}
	if i.raftTransport != nil {
		if err := i.raftTransport.Close(); err != nil {
			return err
		}
	}
	if i.raft != nil {
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
			i.Shutdown()
		case <-i.shutdownCh:
			return
		}
	}
}

func (i *instance) isLeader() bool {
	if i.raft == nil {
		return false
	}
	return i.raft.State() == raft.Leader
}
