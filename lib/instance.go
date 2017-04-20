package huton

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	Serf           *serf.Config
	Raft           *RaftConfig
	Peers          []string
	BaseDir        string
	CacheDBTimeout string
}

func DefaultConfig() *Config {
	c := &Config{
		Serf: serf.DefaultConfig(),
		Raft: &RaftConfig{
			Config:              raft.DefaultConfig(),
			RetainSnapshotCount: 2,
			MaxPool:             3,
			TransportTimeout:    "10s",
		},
		Peers:          make([]string, 0),
		CacheDBTimeout: "10s",
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
	cachesDB         *bolt.DB
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
	if err := i.setupCachesDB(); err != nil {
		return i, err
	}
	raftAddr := &net.TCPAddr{
		IP:   net.ParseIP(config.Serf.MemberlistConfig.BindAddr),
		Port: config.Serf.MemberlistConfig.BindPort + 1,
	}
	if err := i.setupSerf(config.Serf, raftAddr); err != nil {
		i.Shutdown()
		return i, err
	}
	i.peersMu.Lock()
	members := i.serf.Members()
	for _, member := range members {
		p, err := newPeer(member)
		if err != nil {
			i.Shutdown()
			return i, err
		}
		i.peers[p.RaftAddr.String()] = p
	}
	i.peersMu.Unlock()
	if err := i.setupRaft(); err != nil {
		i.Shutdown()
		return i, err
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

func (i *instance) setupCachesDB() error {
	timeout, err := time.ParseDuration(i.config.CacheDBTimeout)
	if err != nil {
		return err
	}
	cachesDBFile := filepath.Join(i.config.BaseDir, i.config.Serf.NodeName, "caches.db")
	cachesDB, err := bolt.Open(cachesDBFile, 0644, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return fmt.Errorf("Failed to open caches DB file %s: %s", cachesDBFile, err)
	}
	i.cachesDB = cachesDB
	return nil
}

func (i *instance) Cache(name string) (*Cache, error) {
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c, nil
	}
	timeout, err := time.ParseDuration(i.config.CacheDBTimeout)
	if err != nil {
		return nil, fmt.Errorf("Failed to partion duration %s: %s", i.config.CacheDBTimeout, err)
	}
	return newCache(i.cachesDB, name, i, timeout)
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
	if i.cachesDB != nil {
		if err := i.cachesDB.Close(); err != nil {
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
