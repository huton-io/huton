package huton

import (
	"errors"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	raftLogCacheSize = 512
)

type RaftConfig struct {
	*raft.Config        `json:",inline" yaml:",inline"`
	ApplicationRetries  int    `json:"applicationRetries" yaml:"applicationRetries"`
	RetainSnapshotCount int    `json:"retainSnapshotCount" yaml:"retainSnapshotCount"`
	MaxPool             int    `json:"maxPool" yaml:"maxPool"`
	TransportTimeout    string `json:"transportTimeout" yaml:"transportTimeout"`
}

func (i *instance) setupRaft() error {
	t, err := time.ParseDuration(i.config.Raft.TransportTimeout)
	if err != nil {
		return err
	}
	addr := &net.TCPAddr{
		IP:   net.ParseIP(i.config.Serf.MemberlistConfig.BindAddr),
		Port: i.config.Serf.MemberlistConfig.BindPort + 1,
	}
	i.raftTransport, err = raft.NewTCPTransport(addr.String(), addr, i.config.Raft.MaxPool, t, i.config.Raft.LogOutput)
	if err != nil {
		return err
	}
	if i.id == "" {
		return errors.New("No instance id provided.")
	}
	basePath := filepath.Join(i.config.BaseDir, i.config.Serf.NodeName)
	i.raftJSONPeers = raft.NewJSONPeers(basePath, i.raftTransport)
	var peers []string
	for _, peer := range i.peers {
		peers = append(peers, peer.RaftAddr.String())
	}
	if err := i.raftJSONPeers.SetPeers(peers); err != nil {
		return err
	}
	snapshots, err := raft.NewFileSnapshotStore(basePath, i.config.Raft.RetainSnapshotCount, os.Stderr)
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
	i.raft, err = raft.NewRaft(i.config.Raft.Config, i, logCache, logStore, snapshots, i.raftJSONPeers, i.raftTransport)
	return err
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
