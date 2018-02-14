package huton

import (
	"crypto/tls"
	"io"
	"log"
	"time"
)

// Option is a function used to configure a huton instance.
type Option func(*instance)

// BindAddr sets the instance's bind address.
func BindAddr(addr string) Option {
	return func(ins *instance) {
		ins.bindAddr = addr
	}
}

// BindPort sets the instance's bind port.
func BindPort(port int) Option {
	return func(ins *instance) {
		ins.bindPort = port
	}
}

// BaseDir sets the instance's base directory.
func BaseDir(baseDir string) Option {
	return func(ins *instance) {
		ins.baseDir = baseDir
	}
}

// Bootstrap tells the instance whether or not to bootstrap its cluster, regardless of peer count.
func Bootstrap(bootstrap bool) Option {
	return func(ins *instance) {
		ins.bootstrap = bootstrap
	}
}

// BootstrapExpect tells the instance how many peers the cluster should expect.
// The cluster will not be bootstraped until the suggested number of peers are present.
func BootstrapExpect(expect int) Option {
	return func(ins *instance) {
		ins.bootstrapExpect = expect
	}
}

// RaftApplicationRetries tells the instance how many times to retry when applying raft logs.
func RaftApplicationRetries(retries int) Option {
	return func(ins *instance) {
		ins.raftApplicationRetries = retries
	}
}

// RaftApplicationTimeout tells the instance how long before it should timeout when applying raft logs.
func RaftApplicationTimeout(timeout time.Duration) Option {
	return func(ins *instance) {
		ins.raftApplicationTimeout = timeout
	}
}

// RaftTransportTimeout tells the instance how long before raft communications time out.
func RaftTransportTimeout(timeout time.Duration) Option {
	return func(ins *instance) {
		ins.raftTransportTimeout = timeout
	}
}

// RaftRetainSnapshotCount tells the instance the maximum number of raft snapshots it should retain.
// After the suggested count is reached, snapshots will be dropped, oldest first.
func RaftRetainSnapshotCount(retain int) Option {
	return func(ins *instance) {
		ins.raftRetainSnapshotCount = retain
	}
}

// LogOutput sets the io.Writer used for logging.
func LogOutput(w io.Writer) Option {
	return func(ins *instance) {
		ins.logger = log.New(w, "huton", log.LstdFlags)
	}
}

// EncryptionKey sets the secrets key used for encrypting cluster communications.
// The key MUST be exactly 32 bytes.
func EncryptionKey(key []byte) Option {
	return func(ins *instance) {
		ins.encryptionKey = key
	}
}

// TLSConfig sets the TLS configuration for Raft communications.
func TLSConfig(tlsConfig *tls.Config) Option {
	return func(ins *instance) {
		ins.tlsConfig = tlsConfig
	}
}

// CompactionInterval is the minimum interval for which cache compaction will occur.
func CompactionInterval(interval time.Duration) Option {
	return func(ins *instance) {
		ins.compactor = PeriodicCompactor(interval)
	}
}
