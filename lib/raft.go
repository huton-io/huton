package huton

import (
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/jonbonazza/huton/lib/proto"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	raftLogCacheSize = 512
)

func (i *instance) setupRaft() error {
	t, err := time.ParseDuration(i.config.RaftTransportTimeout)
	if err != nil {
		return err
	}
	addr := &net.TCPAddr{
		IP:   net.ParseIP(i.config.BindAddr),
		Port: i.config.BindPort + 1,
	}
	i.raftTransport, err = raft.NewTCPTransport(addr.String(), addr, i.config.RaftMaxPool, t, i.config.LogOutput)
	if err != nil {
		return err
	}
	basePath := filepath.Join(i.config.BaseDir, i.name)
	i.raftJSONPeers = raft.NewJSONPeers(basePath, i.raftTransport)
	var peers []string
	for _, peer := range i.peers {
		peers = append(peers, peer.RaftAddr.String())
	}
	if err := i.raftJSONPeers.SetPeers(peers); err != nil {
		return err
	}
	snapshots, err := raft.NewFileSnapshotStore(basePath, i.config.RaftRetainSnapshotCount, os.Stderr)
	if err != nil {
		return err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(basePath, "raft.db"))
	if err != nil {
		return err
	}
	i.raftBoltStore = logStore
	logCache, err := raft.NewLogCache(raftLogCacheSize, logStore)
	if err != nil {
		return err
	}
	raftConfig := i.getRaftConfig()
	i.raft, err = raft.NewRaft(raftConfig, i, logCache, logStore, snapshots, i.raftJSONPeers, i.raftTransport)
	return err
}

func (i *instance) getRaftConfig() *raft.Config {
	raftConfig := raft.DefaultConfig()
	raftConfig.LogOutput = i.config.LogOutput
	raftConfig.ShutdownOnRemove = false
	return raftConfig
}

func (i *instance) addRaftPeer(peer *Peer) error {
	if !i.isLeader() {
		return nil
	}
	return i.raft.AddPeer(peer.RaftAddr.String()).Error()
}

func (i *instance) removeRaftPeer(peer *Peer) error {
	if !i.isLeader() {
		return nil
	}
	return i.raft.RemovePeer(peer.RaftAddr.String()).Error()
}

func (i *instance) apply(cmd *huton_proto.Command) error {
	if i.raft == nil {
		return nil
	}
	// Only the leader can commit raft logs, so if we aren't the leader, we need to forward it to him.
	if !i.isLeader() {
		leader := i.raft.Leader()
		i.peersMu.Lock()
		defer i.peersMu.Unlock()
		return i.sendCommand(i.peers[leader], cmd)
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	timeout, err := time.ParseDuration(i.config.RaftApplicationTimeout)
	if err != nil {
		return err
	}
	return i.raft.Apply(b, timeout).Error()
}
