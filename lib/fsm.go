package huton

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/huton-io/huton/lib/proto"
)

const (
	typeCacheExecute uint32 = iota
	typeCacheCompaction
	typeLeaveCluster
)

var (
	errSnapshotsNotSupported = errors.New("snapshots not supported")
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
		err := c.(*cache).executeSegment(seg)
		if err == nil {
			return
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
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	caches := make([]*cache, 0, len(i.caches))
	for _, c := range i.caches {
		caches = append(caches, c)
	}
	return &fsmSnapshot{
		caches: caches,
	}, nil
}

func (i *instance) Restore(rc io.ReadCloser) error {
	i.dbMu.Lock()
	defer i.dbMu.Unlock()
	var numCaches int64
	if err := binary.Read(rc, Endianess, &numCaches); err != nil {
		return err
	}
	caches := make(map[string]*cache)
	for j := int64(0); j < numCaches; j++ {
		c, err := loadCache(rc, i)
		if err != nil {
			return err
		}
		caches[c.name] = c
	}
	i.caches = caches
	return nil
}

type fsmSnapshot struct {
	caches []*cache
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := binary.Write(sink, Endianess, int64(len(s.caches))); err != nil {
		return err
	}
	for _, c := range s.caches {
		if err := c.persist(sink); err != nil {
			return err
		}
	}
	return nil
}

func (s *fsmSnapshot) Release() {

}
