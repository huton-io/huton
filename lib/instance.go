package huton

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/juju/errors"
	"google.golang.org/grpc"
)

var (
	// ErrNoName is an error used when and instance name is not provided
	ErrNoName = errors.New("no instance name provided")
)

// Config provides configuration to a Huton instance. It is composed of Serf and Raft configs, as well as some
// huton-specific configurations. A few of the 3rd party configurations are ignored, such as Serf.EventCh, but most
// all of it is used.
type Config struct {
	BindAddr                string
	BindPort                int
	Peers                   []string
	BaseDir                 string
	CacheDBTimeout          string
	RaftApplicationRetries  int
	RaftApplicationTimeout  string
	RaftRetainSnapshotCount int
	RaftMaxPool             int
	RaftTransportTimeout    string
	LogOutput               io.Writer
	SerfEventChannel        chan serf.Event
}

// DefaultConfig creates a Config instance initialized with default values.
func DefaultConfig() *Config {
	c := &Config{
		BindAddr:                "127.0.0.1",
		BindPort:                8100,
		RaftRetainSnapshotCount: 2,
		RaftMaxPool:             3,
		RaftTransportTimeout:    "10s",
		RaftApplicationTimeout:  "10s",
		Peers:            make([]string, 0),
		CacheDBTimeout:   "10s",
		SerfEventChannel: make(chan serf.Event),
		LogOutput:        os.Stdout,
	}
	return c
}

// Instance is an interface for the Huton instance.
type Instance interface {
	// Bucket returns the bucket in the off-heap database with the given name.
	// If the bucket doesn't exist, it is automatically created.
	Bucket(name string) (Bucket, error)
	// Peers returns the current list of cluster peers. The list includes the local peer.
	Peers() []*Peer
	// Local returns the local peer.
	Local() *Peer
	// Shutdown gracefully shuts down the instance.
	Shutdown() error
}

type instance struct {
	name             string
	serf             *serf.Serf
	raft             *raft.Raft
	raftJSONPeers    *raft.JSONPeers
	raftBoltStore    *raftboltdb.BoltStore
	raftTransport    *raft.NetworkTransport
	dbFilePath       string
	db               *bolt.DB
	rpcListener      net.Listener
	rpc              *grpc.Server
	serfEventChannel chan serf.Event
	shutdownCh       chan struct{}
	peersMu          sync.Mutex
	peers            map[string]*Peer
	dbMu             sync.Mutex
	config           *Config
	buckets          map[string]*bucket
	logger           *log.Logger
}

func (i *instance) Bucket(name string) (Bucket, error) {
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	if c, ok := i.buckets[name]; ok {
		return c, nil
	}
	return newBucket(i.db, name, i)
}

func (i *instance) Shutdown() error {
	i.logger.Println("Shutting down instance...")
	if i.serf != nil {
		if err := i.serf.Leave(); err != nil {
			return fmt.Errorf("Failed to leave Serf cluster: %s", err)
		}
	}
	if i.raftBoltStore != nil {
		if err := i.raftBoltStore.Close(); err != nil {
			return fmt.Errorf("Failed to close Raft store: %s", err)
		}
	}
	if i.raftTransport != nil {
		if err := i.raftTransport.Close(); err != nil {
			return fmt.Errorf("Failed to close Raft transport: %s", err)
		}
	}
	if i.raft != nil {
		if err := i.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("Failed to shut down Raft instance: %s", err)
		}
	}
	if i.rpcListener != nil {
		if err := i.rpcListener.Close(); err != nil {
			return fmt.Errorf("Failed to close RPC Listener: %s", err)
		}
	}
	if i.db != nil {
		if err := i.db.Close(); err != nil {
			return fmt.Errorf("Failed to close datastore: %s", err)
		}
	}
	close(i.shutdownCh)
	return nil
}

// NewInstance creates a new Huton instance and initializes it and all of its sub-components, such as Serf, Raft, and
// GRPC server, with the provided configuration.
//
// If this function returns successfully, the instance should be considered started and ready for use.
func NewInstance(name string, config *Config) (Instance, error) {
	if name == "" {
		return nil, ErrNoName
	}
	if config.LogOutput == nil {
		config.LogOutput = ioutil.Discard
	}
	i := &instance{
		name:       name,
		shutdownCh: make(chan struct{}),
		peers:      make(map[string]*Peer),
		config:     config,
		buckets:    make(map[string]*bucket),
		dbFilePath: filepath.Join(config.BaseDir, name, "store.db"),
		logger:     log.New(config.LogOutput, "huton", log.LstdFlags),
	}
	i.logger.Println("Initializing datastore...")
	if err := i.setupDB(); err != nil {
		return i, err
	}
	ip := net.ParseIP(config.BindAddr)
	raftAddr := &net.TCPAddr{
		IP:   ip,
		Port: config.BindPort + 1,
	}
	rpcAddr := &net.TCPAddr{
		IP:   ip,
		Port: config.BindPort + 2,
	}
	i.logger.Println("Initializing Serf cluster...")
	if err := i.setupSerf(raftAddr, rpcAddr); err != nil {
		i.Shutdown()
		return i, err
	}
	i.logger.Println("Configuring peers...")
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
	i.logger.Println("Initializing Raft cluster...")
	if err := i.setupRaft(); err != nil {
		i.Shutdown()
		return i, err
	}
	i.logger.Println("Initializing RPC server...")
	if err := i.setupRPC(); err != nil {
		i.Shutdown()
		return i, err
	}
	// Wait for initial leader state
	i.logger.Println("Waiting for initial leader state...")
	for {
		if i.raft.Leader() != "" {
			break
		}
	}
	go i.handleEvents()
	return i, nil
}

func (i *instance) setupDB() error {
	timeout, err := time.ParseDuration(i.config.CacheDBTimeout)
	if err != nil {
		return fmt.Errorf("Failed to parse duration: %s", err)
	}
	cachesDB, err := bolt.Open(i.dbFilePath, 0644, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return fmt.Errorf("Failed to open DB file %s: %s", i.dbFilePath, err)
	}
	i.db = cachesDB
	return nil
}

func (i *instance) handleEvents() {
	for {
		select {
		case e := <-i.config.SerfEventChannel:
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
