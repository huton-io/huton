package huton

import (
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jonbonazza/huton/lib/proto"
	"io"
)

const (
	typeCachePut uint32 = iota
	typeCacheDelete
)

func (i *instance) Apply(l *raft.Log) interface{} {
	for j := 0; j <= i.config.Raft.ApplicationRetries; j++ {
		var cmd huton_proto.Command
		if err := proto.Unmarshal(l.Data, &cmd); err == nil {
			i.applyCommand(&cmd)
			break
		}
	}
	return nil
}

func (i *instance) applyCommand(cmd *huton_proto.Command) {
	for j := 0; j <= i.config.Raft.ApplicationRetries; j++ {
		switch *cmd.Type {
		case typeCachePut:
			var cachePutCmd huton_proto.CachePutCommand
			if err := proto.Unmarshal(cmd.Body, &cachePutCmd); err != nil {
				continue
			}
			i.applyCachePut(&cachePutCmd)
		case typeCacheDelete:
			var cacheDeleteCmd huton_proto.CacheDeleteCommand
			if err := proto.Unmarshal(cmd.Body, &cacheDeleteCmd); err != nil {
				continue
			}
			i.applyCacheDelete(&cacheDeleteCmd)
		}
	}
}

func (i *instance) applyCachePut(cmd *huton_proto.CachePutCommand) {
	for j := 0; j <= i.config.Raft.ApplicationRetries; j++ {
		if cache, err := i.Cache(*cmd.CacheName); err == nil {
			if err := cache.set(cmd.Key, cmd.Value); err == nil {
				break
			}
		}
	}
}

func (i *instance) applyCacheDelete(cmd *huton_proto.CacheDeleteCommand) {
	for j := 0; j <= i.config.Raft.ApplicationRetries; j++ {
		if cache, err := i.Cache(*cmd.CacheName); err == nil {
			if err := cache.delete(cmd.Key); err == nil {
				break
			}
		}
	}
}

func (i *instance) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{
		db: i.cachesDB,
	}, nil
}

func (i *instance) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
	db *bolt.DB
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return f.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(sink)
		return err
	})
}

func (f *fsmSnapshot) Release() {
}
