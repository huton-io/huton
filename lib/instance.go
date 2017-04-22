package huton

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Config provides configuration to a Huton instance. It is composed of Serf and Raft configs, as well as some
// huton-specific configurations. A few of the 3rd party configurations are ignored, such as Serf.EventCh, but most
// all of it is used.
type Config struct {
	Serf           *serf.Config
	Raft           *RaftConfig
	Peers          []string
	BaseDir        string
	CacheDBTimeout string
}

// DefaultConfig creates a Config instance initialized with default values.
func DefaultConfig() *Config {
	c := &Config{
		Serf: serf.DefaultConfig(),
		Raft: &RaftConfig{
			Config:              raft.DefaultConfig(),
			RetainSnapshotCount: 2,
			MaxPool:             3,
			TransportTimeout:    "10s",
			ApplicationTimeout:  "10s",
		},
		Peers:          make([]string, 0),
		CacheDBTimeout: "10s",
	}
	c.Raft.ShutdownOnRemove = false
	return c
}

// Instance is an interface for the Huton instance.
type Instance interface {
	// Cache returns the off-heap cache with the given name. If the cache doesn't exist, it is automatically created.
	Cache(name string) (Cache, error)
	// Peers returns the current list of cluster peers. The list includes the local peer.
	Peers() []*Peer
	// Local returns the local peer.
	Local() *Peer
	// Shutdown gracefully shuts down the instance.
	Shutdown() error
}

type instance struct {
	id               string
	serf             *serf.Serf
	raft             *raft.Raft
	raftJSONPeers    *raft.JSONPeers
	raftBoltStore    *raftboltdb.BoltStore
	raftTransport    *raft.NetworkTransport
	cachesDBFilePath string
	cachesDB         *bolt.DB
	rpcListener      net.Listener
	rpc              *grpc.Server
	serfEventChannel chan serf.Event
	shutdownCh       chan struct{}
	peersMu          sync.Mutex
	peers            map[string]*Peer
	cacheMu          sync.Mutex
	config           *Config
	caches           map[string]*cache
}

func (i *instance) Cache(name string) (Cache, error) {
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c, nil
	}
	return newCache(i.cachesDB, name, i)
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
	if i.rpcListener != nil {
		if err := i.rpcListener.Close(); err != nil {
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

// NewInstance creates a new Huton instance and initailizes it and all of its subcomponents, such as Serf, Raft, and GRPC server, with the provided configuration.
// If this function returns successfully, the instance should be considered started and ready for use.
func NewInstance(config *Config) (Instance, error) {
	host := net.JoinHostPort(config.Serf.MemberlistConfig.BindAddr, strconv.Itoa(config.Serf.MemberlistConfig.BindPort))
	i := &instance{
		id:               host,
		serfEventChannel: make(chan serf.Event),
		shutdownCh:       make(chan struct{}),
		peers:            make(map[string]*Peer),
		config:           config,
		caches:           make(map[string]*cache),
		cachesDBFilePath: filepath.Join(config.BaseDir, config.Serf.NodeName, "caches.db"),
	}
	if err := i.setupCachesDB(); err != nil {
		return i, err
	}
	ip := net.ParseIP(config.Serf.MemberlistConfig.BindAddr)
	raftAddr := &net.TCPAddr{
		IP:   ip,
		Port: config.Serf.MemberlistConfig.BindPort + 1,
	}
	rpcAddr := &net.TCPAddr{
		IP:   ip,
		Port: config.Serf.MemberlistConfig.BindPort + 2,
	}
	if err := i.setupSerf(config.Serf, raftAddr, rpcAddr); err != nil {
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
	if err := i.setupRPC(); err != nil {
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
	cachesDB, err := bolt.Open(i.cachesDBFilePath, 0644, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return fmt.Errorf("Failed to open caches DB file %s: %s", i.cachesDBFilePath, err)
	}
	i.cachesDB = cachesDB
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
