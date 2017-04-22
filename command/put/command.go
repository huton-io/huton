package put

import (
	"flag"
	"fmt"
	"github.com/jonbonazza/huton/command"
	"github.com/jonbonazza/huton/lib"
	"github.com/mitchellh/cli"
	"io/ioutil"
	"os"
	"strings"
)

type Command struct {
	UI cli.Ui
}

func (c *Command) Run(args []string) int {
	config := huton.DefaultConfig()
	config.Raft.LogOutput = ioutil.Discard
	config.Serf.LogOutput = ioutil.Discard
	config.Serf.MemberlistConfig.LogOutput = ioutil.Discard
	flags := flag.NewFlagSet("put", flag.ExitOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	var cacheName string
	var key string
	var value string
	flags.StringVar(&config.Serf.NodeName, "name", "", "unique instnace name")
	flags.StringVar(&config.Serf.MemberlistConfig.BindAddr, "bindAddr", "127.0.0.1", "address to bind serf to")
	flags.IntVar(&config.Serf.MemberlistConfig.BindPort, "bindPort", 8080, "port to bind serf to")
	flags.Var((*command.AppendSliceValue)(&config.Peers), "peers", "peer list")
	flags.StringVar(&cacheName, "cache", "", "")
	flags.StringVar(&key, "key", "", "")
	flags.StringVar(&value, "value", "", "")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return 1
	}
	if cacheName == "" {
		c.UI.Error("Bucket name must be provided.")
		return 1
	}
	if key == "" || value == "" {
		c.UI.Error("Key and value must be provided.")
		return 1
	}
	instance, err := huton.NewInstance(config)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	defer instance.Shutdown()
	cache, err := instance.Bucket(cacheName)
	if err != nil {
		c.UI.Error(fmt.Sprintf("Failed to get cache with name %s: %s", cacheName, err))
		return 1
	}
	if err := cache.Set([]byte(key), []byte(value)); err != nil {
		c.UI.Error(fmt.Sprintf("Failed to set value in cache: %s", err))
		return 1
	}
	return 0
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	helpText := ``
	return strings.TrimSpace(helpText)
}
