package huton

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"

	"fmt"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	raftLogCacheSize = 512
)

func (i *Instance) setupRaft() error {
	repConfig := i.config.Replication
	bindHost := i.config.BindHost
	if bindHost == "" {
		bindHost = "127.0.0.1"
	}
	addr := &net.TCPAddr{
		IP:   net.ParseIP(bindHost),
		Port: i.config.BindPort + 1,
	}
	var err error
	i.raftTransport, err = i.getRaftTransport(addr, repConfig)
	if err != nil {
		return err
	}
	basePath := filepath.Join(repConfig.BaseDir, i.name)
	if err := ensurePath(basePath, true); err != nil {
		return err
	}
	i.raftBoltStore, err = raftboltdb.NewBoltStore(filepath.Join(basePath, "raft.db"))
	if err != nil {
		return err
	}
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, i.raftBoltStore)
	if err != nil {
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(basePath, repConfig.RetainSnapshotCount, ioutil.Discard)
	if err != nil {
		return err
	}
	raftConfig := i.getRaftConfig()
	peersFile := filepath.Join(basePath, "peers.json")
	if err = i.maybeRecoverRaft(raftConfig, cacheStore, snapshotStore, peersFile); err != nil {
		return err
	}
	if err = i.maybePerformInitialBootstrap(raftConfig, cacheStore, snapshotStore); err != nil {
		return err
	}
	raftNotifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = raftNotifyCh
	i.raftNotifyCh = raftNotifyCh
	i.raft, err = raft.NewRaft(raftConfig, i, cacheStore, i.raftBoltStore, snapshotStore, i.raftTransport)
	return err
}

func (i *Instance) maybePerformInitialBootstrap(raftConfig *raft.Config, cacheStore *raft.LogCache, snapshotStore raft.SnapshotStore) error {
	if i.config.Bootstrap {
		hasState, err := raft.HasExistingState(cacheStore, i.raftBoltStore, snapshotStore)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftConfig.LocalID,
						Address: i.raftTransport.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(raftConfig, cacheStore, i.raftBoltStore, snapshotStore, i.raftTransport, configuration); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Instance) maybeRecoverRaft(
	raftConfig *raft.Config,
	cacheStore *raft.LogCache,
	snapshotStore raft.SnapshotStore,
	peersFile string) error {
	if _, err := os.Stat(peersFile); err == nil {
		configuration, err := raft.ReadConfigJSON(peersFile)
		if err != nil {
			return fmt.Errorf("recovery failed to parse peers.json: %v", err)
		}
		if err = raft.RecoverCluster(raftConfig, i, cacheStore, i.raftBoltStore, snapshotStore, i.raftTransport, configuration); err != nil {
			return fmt.Errorf("recovery failed: %v", err)
		}
		if err = os.Remove(peersFile); err != nil {
			return fmt.Errorf("recovery failed to delete peers.json, please delete manually: %v", err)
		}
	}
	return nil
}

func (i *Instance) getRaftTransport(addr net.Addr, repConfig RaftConfig) (*raft.NetworkTransport, error) {
	if repConfig.TLSConfig != nil {
		return newTLSTransport(addr.String(), addr, 3, repConfig.TransportTimeout, repConfig.TLSConfig, ioutil.Discard)
	}
	return raft.NewTCPTransport(addr.String(), addr, 3, repConfig.TransportTimeout, ioutil.Discard)
}

func (i *Instance) getRaftConfig() *raft.Config {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(i.name)
	raftConfig.ShutdownOnRemove = false
	return raftConfig
}

func (i *Instance) apply(op byte, cmd []byte) error {
	if i.raft == nil {
		return nil
	}
	b := make([]byte, 0, len(cmd)+1)
	b = append(b, op)
	b = append(b, cmd...)
	future := i.raft.Apply(b, i.config.Replication.ApplicationTimeout)
	return future.Error()
}
