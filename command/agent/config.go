package agent

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/huton-io/huton/command/flags"
	"github.com/huton-io/huton/lib"
)

type config struct {
	name            string
	bindAddr        string
	bindPort        int
	bootstrap       bool
	bootstrapExpect int
	encryptionKey   string
	certFile        string
	keyFile         string
	caFile          string
	peers           []string
	compactInterval time.Duration
}

func (c config) options() ([]huton.Option, error) {
	var opts []huton.Option
	opts = append(opts, huton.Bootstrap(c.bootstrap))
	if c.bindAddr != "" {
		opts = append(opts, huton.BindAddr(c.bindAddr))
	}
	if c.bindPort >= 0 {
		opts = append(opts, huton.BindPort(c.bindPort))
	}
	if c.bootstrapExpect >= 0 {
		opts = append(opts, huton.BootstrapExpect(c.bootstrapExpect))
	}
	if c.encryptionKey != "" {
		b, err := base64.StdEncoding.DecodeString(c.encryptionKey)
		if err != nil {
			return opts, err
		}
		opts = append(opts, huton.EncryptionKey(b))
	}
	tlsConfig, err := getTLSConfig(c.certFile, c.keyFile, c.caFile)
	if err != nil {
		return opts, err
	}
	if tlsConfig != nil {
		opts = append(opts, huton.TLSConfig(tlsConfig))
	}
	if c.compactInterval > 0 {
		opts = append(opts, huton.CompactionInterval(c.compactInterval))
	}
	return opts, nil
}

func addFlags(fs *flag.FlagSet) *config {
	var c config
	fs.StringVar(&c.name, "name", "", "unique instance name")
	fs.StringVar(&c.bindAddr, "bindAddr", "", "address to bind serf to")
	fs.IntVar(&c.bindPort, "bindPort", -1, "port to bind serf to")
	fs.BoolVar(&c.bootstrap, "bootstrap", false, "bootstrap mode")
	fs.IntVar(&c.bootstrapExpect, "expect", -1, "bootstrap expect")
	fs.StringVar(&c.encryptionKey, "encrypt", "", "base64 encoded encryption key")
	fs.StringVar(&c.certFile, "cert", "", "certificate file used for secure raft communications")
	fs.StringVar(&c.keyFile, "key", "", "private key associated with cert that is used for secure raft communications")
	fs.StringVar(&c.caFile, "caFile", "", "CA file used for cert verification during secure raft communications")
	fs.Var((*flags.StringSlice)(&c.peers), "peers", "peer list")
	fs.Var((*flags.DurationString)(&c.compactInterval), "compact", "compaction interval")
	return &c
}

func getTLSConfig(certFile, keyFile, caCertFile string) (*tls.Config, error) {
	if (certFile != "" && keyFile == "") || (keyFile != "" && certFile == "") {
		return nil, errors.New("both a cert and key must be provided if one or the other is provided")
	} else if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load cert or private key: %s", err)
		}
		tlsConfig := &tls.Config{
			RootCAs:      x509.NewCertPool(),
			Certificates: []tls.Certificate{cert},
		}
		if caCertFile != "" {
			pool, err := loadCAFile(caCertFile)
			if err != nil {
				return tlsConfig, err
			}
			tlsConfig.RootCAs = pool
		}
		return tlsConfig, nil
	}
	return nil, nil
}

func loadCAFile(cacert string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(cacert)
	if err != nil {
		return nil, fmt.Errorf("Failed reading CA file: %s", err)
	}
	if ok := pool.AppendCertsFromPEM(pem); !ok {
		return nil, fmt.Errorf("Failed to parse PEM for CA cert: %s", cacert)
	}
	return pool, nil
}
