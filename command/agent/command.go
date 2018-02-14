package agent

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/huton-io/huton/command"
	"github.com/huton-io/huton/lib"
	"github.com/mitchellh/cli"
)

// Command is a CLI command use to start a huton agent.
type Command struct {
	UI       cli.Ui
	instance huton.Instance
}

// Run is called by the CLI to execute the command.
func (c *Command) Run(args []string) int {
	name, opts, peers, err := c.readConfig()
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	c.instance, err = huton.NewInstance(name, opts...)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	_, err = c.instance.Join(peers)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	return c.handleSignals()
}

// Synopsis is used by the CLI to provide a synopsis of the command.
func (c *Command) Synopsis() string {
	return ""
}

// Help is used by the CLI to provide help text for the command.
func (c *Command) Help() string {
	return ""
}

func (c *Command) readConfig() (string, []huton.Option, []string, error) {
	var name string
	var bindAddr string
	var bindPort int
	var bootstrap bool
	var bootstrapExpect int
	var encryptionKey string
	var certFile string
	var keyFile string
	var caFile string
	peers := []string{}
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	flags.StringVar(&name, "name", "", "unique instance name")
	flags.StringVar(&bindAddr, "bindAddr", "", "address to bind serf to")
	flags.IntVar(&bindPort, "bindPort", -1, "port to bind serf to")
	flags.BoolVar(&bootstrap, "bootstrap", false, "bootstrap mode")
	flags.IntVar(&bootstrapExpect, "expect", -1, "bootstrap expect")
	flags.StringVar(&encryptionKey, "encrypt", "", "base64 encoded encryption key")
	flags.StringVar(&certFile, "cert", "", "certificate file used for secure raft communications")
	flags.StringVar(&keyFile, "key", "", "private key associated with cert that is used for secure raft communications")
	flags.StringVar(&caFile, "caFile", "", "CA file used for cert verification during secure raft communications")
	flags.Var((*command.AppendSliceValue)(&peers), "peers", "peer list")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return "", nil, nil, err
	}
	var opts []huton.Option
	opts = append(opts, huton.Bootstrap(bootstrap))
	if bindAddr != "" {
		opts = append(opts, huton.BindAddr(bindAddr))
	}
	if bindPort >= 0 {
		opts = append(opts, huton.BindPort(bindPort))
	}
	if bootstrapExpect >= 0 {
		opts = append(opts, huton.BootstrapExpect(bootstrapExpect))
	}
	if encryptionKey != "" {
		b, err := base64.StdEncoding.DecodeString(encryptionKey)
		if err != nil {
			return name, opts, peers, nil
		}
		opts = append(opts, huton.EncryptionKey(b))
	}
	tlsConfig, err := getTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return "", nil, nil, err
	}
	if tlsConfig != nil {
		opts = append(opts, huton.TLSConfig(tlsConfig))
	}
	return name, opts, peers, nil
}

func (c *Command) handleSignals() int {
	signalCh := make(chan os.Signal, 3)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
		return 0
	}
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
