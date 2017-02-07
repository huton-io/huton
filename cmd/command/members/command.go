package members

import (
	"flag"
	"fmt"
	"github.com/jonbonazza/huton/cmd/command"
	"github.com/jonbonazza/huton/lib"
	"github.com/mitchellh/cli"
	"os"
)

type Command struct {
	UI cli.Ui
}

func (c *Command) readConfig() (*huton.Config, error) {
	config := huton.DefaultConfig()
	flags := flag.NewFlagSet("members", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	flags.StringVar(&config.Name, "name", "", "unique instnace name")
	flags.StringVar(&config.Serf.BindAddr, "serfBind", "127.0.0.1", "address to bind serf to")
	flags.IntVar(&config.Serf.BindPort, "serfPort", 8080, "port to bind serf to")
	flags.StringVar(&config.Raft.BindAddr, "raftBind", "127.0.0.1", "address to bind raft to")
	flags.IntVar(&config.Raft.BindPort, "raftPort", 8080, "port to bind raft to")
	flags.Var((*command.AppendSliceValue)(&config.Peers), "peers", "peer list")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c *Command) Run(args []string) int {
	config, err := c.readConfig()
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	fmt.Println(config)
	instance, err := huton.NewInstance(config)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	c.UI.Output("Instance created.")
	defer instance.Close()
	peers := instance.Peers()
	for _, peer := range peers {
		c.UI.Output(peer.String())
	}
	c.UI.Output("Instance closing")
	return 0
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	return ""
}
