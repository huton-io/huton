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
	var config huton.Config
	flags := flag.NewFlagSet("members", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	flags.StringVar(&config.BindAddr, "bind", "0.0.0.0", "address to bind to")
	flags.IntVar(&config.BindPort, "port", 8080, "port to bind to")
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
	defer instance.Close()
	members := instance.Members()
	for _, member := range members {
		c.UI.Output(member)
	}
	return 0
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	return ""
}
