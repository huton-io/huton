package members

import (
	"flag"
	"github.com/jonbonazza/huton/command"
	"github.com/jonbonazza/huton/lib"
	"github.com/mitchellh/cli"
	"io/ioutil"
	"os"
)

type Command struct {
	UI cli.Ui
}

func (c *Command) Run(args []string) int {
	config, err := c.readConfig()
	if err != nil {
		return 1
	}
	instance, err := huton.NewInstance(config)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	defer instance.Shutdown()
	peers := instance.Peers()
	for _, peer := range peers {
		if peer.ID != config.Serf.NodeName {
			c.UI.Output(peer.String())
		}
	}
	return 0
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	return ""
}

func (c *Command) readConfig() (*huton.Config, error) {
	config := huton.DefaultConfig()
	config.Raft.LogOutput = ioutil.Discard
	config.Serf.LogOutput = ioutil.Discard
	config.Serf.MemberlistConfig.LogOutput = ioutil.Discard
	flags := flag.NewFlagSet("members", flag.ExitOnError)
	flags.Usage = func() {
		c.UI.Error(c.Help())
	}
	flags.StringVar(&config.Serf.NodeName, "name", "", "unique instnace name")
	flags.StringVar(&config.Serf.MemberlistConfig.BindAddr, "bindAddr", "127.0.0.1", "address to bind serf to")
	flags.IntVar(&config.Serf.MemberlistConfig.BindPort, "bindPort", 8080, "port to bind serf to")
	flags.Var((*command.AppendSliceValue)(&config.Peers), "peers", "peer list")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return nil, err
	}
	return config, nil
}
