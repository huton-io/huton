package huton

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/jonbonazza/huton/lib/proto"
	"google.golang.org/grpc"
)

const (
	raftRemoveGracePeriod = 5 * time.Second
)

var (
	// ErrNoName is an error used when and instance name is not provided
	ErrNoName = errors.New("no instance name provided")
)

// Instance is an interface for the Huton instance.
type Instance interface {
	Cache(name string) Cache
	// Peers returns the current list of cluster peers. The list includes the local peer.
	Peers() []*Peer
	// Local returns the local peer.
	Local() *Peer
	// IsLeader returns true if this instance is the cluster leader.
	IsLeader() bool
	// Join joins and existing cluster.
	Join(addrs []string) (int, error)
	// Leave gracefully leaves the cluster.
	Leave() error
	// Shutdown forcefully shuts down the instance.
	Shutdown() error
}

type instance struct {
	name                    string
	bindAddr                string
	bindPort                int
	baseDir                 string
	bootstrap               bool
	bootstrapExpect         int
	serf                    *serf.Serf
	raft                    *raft.Raft
	raftBoltStore           *raftboltdb.BoltStore
	raftTransport           *raft.NetworkTransport
	raftTransportTimeout    time.Duration
	raftApplicationRetries  int
	raftApplicationTimeout  time.Duration
	raftRetainSnapshotCount int
	tlsConfig               *tls.Config
	encryptionKey           []byte
	rpcListener             net.Listener
	rpc                     *grpc.Server
	serfEventChannel        chan serf.Event
	shutdownCh              chan struct{}
	peersMu                 sync.Mutex
	peers                   map[string]*Peer
	dbMu                    sync.Mutex
	caches                  map[string]*cache
	logger                  *log.Logger
	raftNotifyCh            chan bool
	shutdownLock            sync.Mutex
	shutdown                bool
}

func (i *instance) Cache(name string) Cache {
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	if c, ok := i.caches[name]; ok {
		return c
	}
	dc := newCache(name, i)
	i.caches[name] = dc
	return dc
}

func (i *instance) Join(addrs []string) (int, error) {
	return i.serf.Join(addrs, true)
}

func (i *instance) IsLeader() bool {
	if i.raft == nil {
		return false
	}
	return i.raft.State() == raft.Leader
}

func (i *instance) Shutdown() error {
	i.logger.Println("Shutting down instance...")
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
	if i.rpcListener != nil {
		if err := i.rpcListener.Close(); err != nil {
			return fmt.Errorf("Failed to close RPC Listener: %s", err)
		}
	}
	return nil
}

func (i *instance) Leave() error {
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
	i.issueLeaveIntent()
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

func (i *instance) numPeers() (int, error) {
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
func NewInstance(name string, opts ...Option) (Instance, error) {
	if name == "" {
		return nil, ErrNoName
	}
	i := &instance{
		name:                    name,
		bindAddr:                "127.0.0.1",
		bindPort:                8100,
		shutdownCh:              make(chan struct{}),
		peers:                   make(map[string]*Peer),
		caches:                  make(map[string]*cache),
		logger:                  log.New(os.Stdout, "huton", log.LstdFlags),
		raftApplicationTimeout:  10 * time.Second,
		raftTransportTimeout:    10 * time.Second,
		raftRetainSnapshotCount: 2,
		serfEventChannel:        make(chan serf.Event, 256),
	}
	for _, opt := range opts {
		opt(i)
	}
	i.logger.Println("Initializing RPC server...")
	if err := i.setupRPC(); err != nil {
		i.Shutdown()
		return i, err
	}
	i.logger.Println("Initializing Raft cluster...")
	if err := i.setupRaft(); err != nil {
		i.Shutdown()
		return i, err
	}
	ip := net.ParseIP(i.bindAddr)
	raftAddr := &net.TCPAddr{
		IP:   ip,
		Port: i.bindPort + 1,
	}
	rpcAddr := &net.TCPAddr{
		IP:   ip,
		Port: i.bindPort + 2,
	}

	i.logger.Println("Initializing Serf cluster...")
	if err := i.setupSerf(raftAddr, rpcAddr); err != nil {
		i.Shutdown()
		return i, err
	}
	go i.handleEvents()
	return i, nil
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

func (i *instance) issueLeaveIntent() {
	t := typeLeaveCluster
	cmd := &huton_proto.Command{
		Type: &t,
		Body: []byte(i.name),
	}
	i.apply(cmd)
}
