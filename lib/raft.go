package huton

import (
	"io"
	"net"
	"os"
	"path/filepath"

	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/jonbonazza/huton/lib/proto"
)

const (
	raftLogCacheSize = 512
)

func (i *instance) setupRaft(logWriter io.Writer) error {
	addr := &net.TCPAddr{
		IP:   net.ParseIP(i.bindAddr),
		Port: i.bindPort + 1,
	}
	var err error
	i.raftTransport, err = raft.NewTCPTransport(addr.String(), addr, 3, i.raftTransportTimeout, logWriter)
	if err != nil {
		return err
	}
	basePath := filepath.Join(i.baseDir, i.name)
	if err := EnsurePath(basePath, true); err != nil {
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
	snapshotStore, err := raft.NewFileSnapshotStore(basePath, i.raftRetainSnapshotCount, logWriter)
	if err != nil {
		return err
	}
	raftConfig := i.getRaftConfig(logWriter)
	peersFile := filepath.Join(basePath, "peers.json")
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
	if i.bootstrap {
		hasState, err := raft.HasExistingState(cacheStore, i.raftBoltStore, snapshotStore)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					raft.Server{
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
	raftNotifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = raftNotifyCh
	i.raftNotifyCh = raftNotifyCh
	i.raft, err = raft.NewRaft(raftConfig, i, cacheStore, i.raftBoltStore, snapshotStore, i.raftTransport)
	return err
}

func (i *instance) getRaftConfig(logWriter io.Writer) *raft.Config {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(i.name)
	raftConfig.LogOutput = logWriter
	raftConfig.ShutdownOnRemove = false
	return raftConfig
}

func (i *instance) apply(cmd *huton_proto.Command) error {
	if i.raft == nil {
		return nil
	}
	// Only the leader can commit raft logs, so if we aren't the leader, we need to forward it to him.
	if !i.IsLeader() {
		leader := i.raft.Leader()
		i.peersMu.Lock()
		defer i.peersMu.Unlock()
		return i.sendCommand(i.peers[string(leader)], cmd)
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return i.raft.Apply(b, i.raftApplicationTimeout).Error()
}
