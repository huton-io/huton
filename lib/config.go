package huton

import (
	"crypto/tls"
	"io"
	"time"
)

// Config is a structure used to configure a hunton.Instance.
type Config struct {
	// BindHost is the host that the cluster will bind to.
	BindHost string
	// BindPort is the base port that the cluster will bind to.
	// This port will be binded by the serf cluster in particular,
	// while BindPort+1 will be bound to by the raft cluster, and
	// BindPort+2 will be bound to by the RPC server.
	BindPort int
	// Bootstrap is used to force the cluster to bootstrap the node.
	// This is useful if you wish to create a single node server for testing.
	// It is not recommended to enable this in production.
	Bootstrap bool
	// Expect is the expected number of initial nodes in the cluster. The cluster
	// will wait for this number of nodes to be available before the cluster is
	// started and usable. This must be at least 3. Bootstrap will override this setting.
	// The default is 3.
	Expect int
	// LogOutput is an io.Writer used for logging. This defaults to stdout.
	LogOutput io.Writer
	// Replication is the configuration used for Raft replication.
	Replication RaftConfig
	// SerfEncryptionKey is an optional symmetric key used to encrypt Serf traffic between nodes.
	// If no key is provided, encryption will be disabled.
	SerfEncryptionKey []byte
}

// RaftConfig is a structure used to configure a huton.Instance's internal Raft cluster.
type RaftConfig struct {
	// BaseDir is the root directory for the Raft db and snapshot directory.
	// This directory must be write accessible by the huton process. The default
	// is the current working directory.
	BaseDir string
	// ApplicationTimeout is an optional timeout for applying Raft logs. If this
	// timeout is reached, the log is rejected by that node and if enough nodes
	// reject a log, that log will not be committed and rolled back. The default is
	// no timeout.
	ApplicationTimeout time.Duration
	// TransportTimeout is a timeout used for communications between raft clients.
	// The default is no timeout.
	TransportTimeout time.Duration
	// RetainSnapshotCount is the maximum number of Raft snapshots to retain on disk
	// before old snapshots are deleted. The default is 2.
	RetainSnapshotCount int
	// TLSConfig is an optional TLS configuration for Raft communications. If no TLS
	// config is provided, the communications will be unencrypted.
	TLSConfig *tls.Config
}
