package huton

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

const (
	raftRemoveGracePeriod = 5 * time.Second
)

var (
	// ErrNoName is an error used when and instance name is not provided
	ErrNoName = errors.New("no instance name provided")
)

// Instance is an interface for the Huton instance.
type Instance struct {
	name             string
	config           *Config
	logger           *log.Logger
	serf             *serf.Serf
	raft             *raft.Raft
	raftBoltStore    *raftboltdb.BoltStore
	raftTransport    *raft.NetworkTransport
	rpc              *rpcServer
	serfEventChannel chan serf.Event
	shutdownCh       chan struct{}
	peersMu          sync.Mutex
	peers            map[string]*Peer
	dbMu             sync.Mutex
	caches           map[string]*Cache
	raftNotifyCh     chan bool
	readyCh          chan struct{}
	shutdownLock     sync.Mutex
	shutdown         bool
	errCh            chan error
}

// Cache returns the cache with the given name. If no cache exists with the
// provided name, a new, empty cache with that name will be created.
func (i *Instance) Cache(name string) (*Cache, error) {
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c, nil
	}
	dc, err := newCache(name, i)
	if err != nil {
		return nil, err
	}
	i.caches[name] = dc
	return dc, nil
}

// IsLeader returns true if this instance is the cluster leader.
func (i *Instance) IsLeader() bool {
	if i.raft == nil {
		return false
	}
	return i.raft.State() == raft.Leader
}

// Join joins and existing cluster.
func (i *Instance) Join(addrs []string) (int, error) {
	return i.serf.Join(addrs, true)
}

// Leave gracefully leaves the cluster.
func (i *Instance) Leave() error {
	numPeers, err := i.numPeers()
	if err != nil {
		return err
	}
	addr := i.raftTransport.LocalAddr()
	isLeader := i.IsLeader()
	if isLeader && numPeers > 1 {
		future := i.raft.RemoveServer(raft.ServerID(i.name), 0, 0)
		if err := future.Error(); err != nil {
			i.logger.Printf("[ERR] failed to remove ourself as raft peer: %v", err)
		}
	}
	if i.serf != nil {
		if err := i.serf.Leave(); err != nil {
			i.logger.Printf("[ERR] failed to leave serf cluster: %v", err)
		}
	}
	// If we were not leader, wait to be safely removed from the cluster. We
	// must wait to allow the raft replication to take place, otherwise an
	// immediate shutdown could cause a loss of quorum.
	if !isLeader {
		var left bool
		limit := time.Now().Add(raftRemoveGracePeriod)
		for !left && time.Now().Before(limit) {
			time.Sleep(50 * time.Millisecond)
			future := i.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				i.logger.Printf("[ERR] failed to get raft configuration: %v", err)
				break
			}
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == addr {
					left = false
					break
				}
			}
		}
		if !left {
			i.logger.Printf("[WARN] failed to leave raft configuration gracefully, timeout")
		}
	}
	return nil
}

// Shutdown forcefully shuts down the instance.
func (i *Instance) Shutdown() error {
	i.logger.Println("[INFO] Shutting down instance...")
	i.shutdownLock.Lock()
	defer i.shutdownLock.Unlock()
	if i.shutdown {
		return nil
	}
	i.shutdown = true
	close(i.shutdownCh)
	if i.serf != nil {
		i.serf.Shutdown()
	}
	if i.raft != nil {
		i.raftTransport.Close()
		if err := i.raft.Shutdown().Error(); err != nil {
			i.logger.Printf("[ERR] failed to shudown raft server: %v", err)
		}
		if i.raftBoltStore != nil {
			i.raftBoltStore.Close()
		}
	}
	if i.rpc != nil {
		i.rpc.Stop()
	}
	return nil
}

// WaitForReady blocks until the cluster becomes ready and leadership is established.
func (i *Instance) WaitForReady() {
	<-i.readyCh
}

func (i *Instance) numPeers() (int, error) {
	future := i.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	configuration := future.Configuration()
	return len(configuration.Servers), nil
}

// NewInstance creates a new Huton instance and initializes it and all of its sub-components, such as Serf, Raft, and
// GRPC server, with the provided configuration.
//
// If this function returns successfully, the instance should be considered started and ready for use.
func NewInstance(name string, config Config) (*Instance, error) {
	if name == "" {
		return nil, ErrNoName
	}
	i := &Instance{
		name:             name,
		config:           &config,
		shutdownCh:       make(chan struct{}, 1),
		peers:            make(map[string]*Peer),
		caches:           make(map[string]*Cache),
		serfEventChannel: make(chan serf.Event, 256),
		errCh:            make(chan error, 256),
		readyCh:          make(chan struct{}, 1),
	}
	i.ensureConfig()
	i.logger.Println("[INFO] Initializing RPC server...")
	var err error
	if i.rpc, err = newRPCServer(i); err != nil {
		i.Shutdown()
		return i, err
	}
	i.logger.Println("[INFO] Initializing Raft cluster...")
	if err := i.setupRaft(); err != nil {
		i.Shutdown()
		return i, err
	}
	ip := net.ParseIP(i.config.BindHost)
	raftAddr := &net.TCPAddr{
		IP:   ip,
		Port: i.config.BindPort + 1,
	}
	rpcAddr := &net.TCPAddr{
		IP:   ip,
		Port: i.config.BindPort + 2,
	}

	i.logger.Println("[INFO] Initializing Serf cluster...")
	if err := i.setupSerf(raftAddr, rpcAddr); err != nil {
		i.Shutdown()
		return i, err
	}
	go i.handleEvents()
	return i, nil
}

func (i *Instance) ensureConfig() {
	if i.config.BindHost == "" {
		i.config.BindHost = "127.0.0.1"
	}
	if i.config.LogOutput == nil {
		i.config.LogOutput = os.Stdout
	}
	if i.config.Expect < 3 {
		i.config.Expect = 3
	}
	i.logger = log.New(os.Stdout, "huton", log.LstdFlags)
}

func (i *Instance) handleEvents() {
	for {
		select {
		case e := <-i.serfEventChannel:
			i.handleSerfEvent(e)
		case <-i.raft.LeaderCh():
			close(i.readyCh)
		case <-i.serf.ShutdownCh():
			i.Shutdown()
		case err := <-i.errCh:
			i.logger.Printf("[ERR] %s", err)
		case <-i.shutdownCh:
			return
		}
	}
}
