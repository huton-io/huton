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
	name, config, err := c.readConfig()
	if err != nil {
		return 1
	}
	instance, err := huton.NewInstance(name, config)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	defer instance.Shutdown()
	peers := instance.Peers()
	for _, peer := range peers {
		if peer.Name != name {
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

func (c *Command) readConfig() (string, *huton.Config, error) {
	config := huton.DefaultConfig()
	config.LogOutput = ioutil.Discard
	flags := flag.NewFlagSet("members", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Error(c.Help())
	}
	var name string
	flags.StringVar(&name, "name", "members", "unique instnace name")
	flags.StringVar(&config.BindAddr, "bindAddr", config.BindAddr, "address to bind serf to")
	flags.IntVar(&config.BindPort, "bindPort", config.BindPort, "port to bind serf to")
	flags.Var((*command.AppendSliceValue)(&config.Peers), "peers", "peer list")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return "", nil, err
	}
	return name, config, nil
}
