package huton

import (
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jonbonazza/huton/lib/proto"
)

const (
	typeCacheExecute uint32 = iota
	typeLeaveCluster
)

func (i *instance) Apply(l *raft.Log) interface{} {
	var cmd huton_proto.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}
	i.applyCommand(&cmd)
	return nil
}

func (i *instance) applyCommand(cmd *huton_proto.Command) {
	for j := 0; j <= i.raftApplicationRetries; j++ {
		switch *cmd.Type {
		case typeCacheExecute:
			var cacheBatchCmd huton_proto.CacheBatch
			if err := proto.Unmarshal(cmd.Body, &cacheBatchCmd); err != nil {
				continue
			}
			i.applyCacheBatch(&cacheBatchCmd)
		case typeLeaveCluster:
			i.applyLeaveCluster(string(cmd.Body))
		}
	}
}

func (i *instance) applyCacheBatch(cmd *huton_proto.CacheBatch) {
	for j := 0; j <= i.raftApplicationRetries; j++ {
		c := i.Cache(*cmd.CacheName)
		seg := &segment{
			buf:  cmd.Buf,
			meta: cmd.Meta,
		}
		err := c.(*cache).executeBatch(seg)
		if err != nil {
			break
		}
		i.logger.Printf("[ERR] failed to execute batch: %v", err)
		if j < i.raftApplicationRetries {
			i.logger.Println("[INFO] retrying batch execution...")
		}
	}
}

func (i *instance) applyLeaveCluster(name string) {
	if i.IsLeader() {
		if err := i.raft.RemoveServer(raft.ServerID(name), 0, 0).Error(); err != nil {
			i.logger.Printf("[WARN] failed to remove peer: %v", err)
		}
	}
}

func (i *instance) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

func (i *instance) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct{}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmSnapshot) Release() {
}
