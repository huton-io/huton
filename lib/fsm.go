package huton

import (
	"github.com/hashicorp/raft"
	"io"
)

func (i *instance) Apply(l *raft.Log) interface{} {
	return nil
}

func (i *instance) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

func (i *instance) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmSnapshot) Release() {}
