package huton

import (
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/huton-io/huton/lib/proto"
)

var (
	errSnapshotsNotSupported = errors.New("snapshots not supported")
)

// Apply applies a raft log received from the leader.
func (i *Instance) Apply(l *raft.Log) interface{} {
	op := l.Data[0]
	cmd := l.Data[1:]
	i.applyCommand(op, cmd)
	return nil
}

func (i *Instance) applyCommand(op byte, cmd []byte) error {
	for j := 0; j <= i.config.Replication.ApplicationRetries; j++ {
		switch op {
		case cacheOpSet:
			var cacheSetCmd huton_proto.CacheSet
			if err := proto.Unmarshal(cmd, &cacheSetCmd); err != nil {
				continue
			}
			return i.applyCacheSet(&cacheSetCmd)
		case cacheOpDel:
			var cacheDelCmd huton_proto.CacheDel
			if err := proto.Unmarshal(cmd, &cacheDelCmd); err != nil {
				continue
			}
			return i.applyCacheDel(&cacheDelCmd)
		}
	}
	return nil
}

func (i *Instance) applyCacheSet(cmd *huton_proto.CacheSet) error {
	c, err := i.Cache(cmd.CacheName)
	if err != nil {
		return err
	}
	for j := 0; j <= i.config.Replication.ApplicationRetries; j++ {
		if err = c.executeSet(cmd.Key, cmd.Value); err == nil {
			break
		}
	}
	return err
}

func (i *Instance) applyCacheDel(cmd *huton_proto.CacheDel) error {
	c, err := i.Cache(cmd.CacheName)
	if err != nil {
		return err
	}
	for j := 0; j < i.config.Replication.ApplicationRetries; j++ {
		if err = c.executeDelete(cmd.Key); err == nil {
			break
		}
	}
	return err
}

func (i *Instance) applyLeaveCluster(name string) {
	if i.IsLeader() {
		if err := i.raft.RemoveServer(raft.ServerID(name), 0, 0).Error(); err != nil {
			i.logger.Printf("[WARN] failed to remove peer: %v", err)
		}
	}
}

// Snapshot returns a snapshot of the current instance state.
// If a snapshot cannot be created, an error is returned as well.
func (i *Instance) Snapshot() (raft.FSMSnapshot, error) {
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	caches := make([]*Cache, 0, len(i.caches))
	for _, c := range i.caches {
		caches = append(caches, c)
	}
	return &fsmSnapshot{
		caches: caches,
	}, nil
}

// Restore restores the instance's state from an snapshot. If the state
// cannot be restored, an error is returned.
func (i *Instance) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
	caches []*Cache
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *fsmSnapshot) Release() {

}
