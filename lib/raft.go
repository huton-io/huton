package huton

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

type CmdType int

const (
	joinCmd CmdType = iota
	leaveCmd
)

type command struct {
	Cmd   CmdType          `json:"cmd"`
	Key   *json.RawMessage `json:"key,omitempty"`
	Value *json.RawMessage `json:"value,omitempty"`
}

type RaftConfig struct {
	BindAddr            string `json:"bindAddr" yaml:"bindAddr"`
	BindPort            int    `json:"bindPort" yaml:"bindPort"`
	BaseDir             string `json:"baseDir" yaml:"baseDir"`
	RetainSnapshotCount int    `json:"retainSnapshotCount" yaml:"retainSnapshotCount"`
	MaxPool             int    `json:"maxPool" yaml:"maxPool"`
	Timeout             string `json:"timeout" yaml:"timeout"`
}

func (c RaftConfig) Addr() (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort))
}

func (i *instance) setupRaft() error {
	config := raft.DefaultConfig()
	addr, err := i.config.Raft.Addr()
	if err != nil {
		return err
	}
	t, err := time.ParseDuration(i.config.Raft.Timeout)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(addr.String(), addr, i.config.Raft.MaxPool, t, os.Stderr)
	if err != nil {
		return err
	}
	peerStore := raft.NewJSONPeers(i.config.Raft.BaseDir, transport)
	peers, err := peerStore.Peers()
	if err != nil {
		return err
	}
	if len(peers) <= 1 {
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}
	snapshots, err := raft.NewFileSnapshotStore(i.config.Raft.BaseDir, i.config.Raft.RetainSnapshotCount, os.Stderr)
	if err != nil {
		return err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(i.config.Raft.BaseDir, "raft.db"))
	if err != nil {
		return err
	}
	raftClient, err := raft.NewRaft(config, (*fsm)(i), logStore, logStore, snapshots, peerStore, transport)
	i.raft = raftClient
	return err
}

func (i *instance) applyCommand(cmdType CmdType, key, value string) {
	k := json.RawMessage(key)
	v := json.RawMessage(value)
	c := &command{
		Cmd:   cmdType,
		Key:   &k,
		Value: &v,
	}
	b, _ := json.Marshal(c)
	t, _ := time.ParseDuration(i.config.Raft.Timeout)
	i.raft.Apply(b, t)
}

type fsm instance

func (f *fsm) Apply(l *raft.Log) interface{} {
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmSnapshot) Release() {

}
