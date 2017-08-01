package huton

import (
	"io"
	"os"

	"github.com/hashicorp/serf/serf"
)

// Config provides configuration to a Huton instance. It is composed of Serf and Raft configs, as well as some
// huton-specific configurations. A few of the 3rd party configurations are ignored, such as Serf.EventCh, but most
// all of it is used.
type Config struct {
	BindAddr                string
	BindPort                int
	Peers                   []string
	Bootstrap               bool
	BootstrapExpect         int
	BaseDir                 string
	CacheDBTimeout          string
	RaftApplicationRetries  int
	RaftApplicationTimeout  string
	RaftRetainSnapshotCount int
	RaftMaxPool             int
	RaftTransportTimeout    string
	LogOutput               io.Writer
	SerfEventChannel        chan serf.Event
}

// DefaultConfig creates a Config instance initialized with default values.
func DefaultConfig() *Config {
	return &Config{
		BindAddr:                "127.0.0.1",
		BindPort:                8100,
		RaftRetainSnapshotCount: 2,
		RaftMaxPool:             3,
		RaftTransportTimeout:    "10s",
		RaftApplicationTimeout:  "10s",
		Peers:            make([]string, 0),
		CacheDBTimeout:   "10s",
		SerfEventChannel: make(chan serf.Event),
		LogOutput:        os.Stdout,
	}
}
