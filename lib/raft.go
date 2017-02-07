package huton

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
	"errors"
	"fmt"
)

type RaftConfig struct {
	BindAddr            string `json:"bindAddr" yaml:"bindAddr"`
	BindPort            int    `json:"bindPort" yaml:"bindPort"`
	RetainSnapshotCount int    `json:"retainSnapshotCount" yaml:"retainSnapshotCount"`
	MaxPool             int    `json:"maxPool" yaml:"maxPool"`
	Timeout             string `json:"timeout" yaml:"timeout"`
}

func (c *RaftConfig) Addr() (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort))
}

func (i *instance) setupRaft() error {
	addr, err := i.config.Raft.Addr()
	if err != nil {
		return err
	}
	config := raft.DefaultConfig()
	config.ShutdownOnRemove = false
	t, err := time.ParseDuration(i.config.Raft.Timeout)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(addr.String(), addr, i.config.Raft.MaxPool, t, os.Stderr)
	if err != nil {
		return err
	}
	if i.id == "" {
		return errors.New("No instance id provided.")
	}
	basePath := filepath.Join(i.config.BaseDir, i.config.Name)
	if err := removeOldPeersFile(basePath); err != nil {
		return err
	}
	i.raftJSONPeers = raft.NewJSONPeers(basePath, transport)
	var peers []string
	for _, member := range i.serf.Members() {
		peer, err := newPeer(member)
		if err != nil {
			return err
		}
		peers = append(peers, peer.RaftAddr.String())
	}
	if len(peers) <= 1 {
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
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
	raftClient, err := raft.NewRaft(config, i, logStore, logStore, snapshots, i.raftJSONPeers, transport)
	i.raft = raftClient
	return err
}

func (i *instance) addRaftPeer(peer *Peer) {
	if i.isLeader() {
		i.raft.AddPeer(peer.RaftAddr.String()).Error()
	}
}

func (i *instance) removeRaftPeer(peer *Peer) {
	if i.isLeader() {
		i.raft.RemovePeer(peer.RaftAddr.String()).Error()
	}
}

func removeOldPeersFile(basePath string) error {
	if err := os.RemoveAll(basePath); err != nil {
		return err
	}
	return os.MkdirAll(basePath, 0755)
}
