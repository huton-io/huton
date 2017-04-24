package huton

import (
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jonbonazza/huton/lib/proto"
	"io"
	"os"
)

const (
	typeCachePut uint32 = iota
	typeCacheDelete
)

func (i *instance) Apply(l *raft.Log) interface{} {
	for j := 0; j <= i.config.RaftApplicationRetries; j++ {
		var cmd huton_proto.Command
		if err := proto.Unmarshal(l.Data, &cmd); err == nil {
			i.applyCommand(&cmd)
			break
		}
	}
	return nil
}

func (i *instance) applyCommand(cmd *huton_proto.Command) {
	for j := 0; j <= i.config.RaftApplicationRetries; j++ {
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
	for j := 0; j <= i.config.RaftApplicationRetries; j++ {
		if c, err := i.Bucket(*cmd.CacheName); err == nil {
			if err := c.(*bucket).set(cmd.Key, cmd.Value); err == nil {
				break
			}
		}
	}
}

func (i *instance) applyCacheDelete(cmd *huton_proto.CacheDeleteCommand) {
	for j := 0; j <= i.config.RaftApplicationRetries; j++ {
		if c, err := i.Bucket(*cmd.CacheName); err == nil {
			if err := c.(*bucket).del(cmd.Key); err == nil {
				break
			}
		}
	}
}

func (i *instance) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{
		db: i.db,
	}, nil
}

func (i *instance) Restore(rc io.ReadCloser) error {
	if i.db != nil {
		if err := i.db.Close(); err != nil {
			return err
		}
	}
	if err := func() error {
		f, err := os.OpenFile(i.dbFilePath, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, rc)
		return err
	}(); err != nil {
		return err
	}
	if err := i.setupDB(); err != nil {
		return err
	}
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
