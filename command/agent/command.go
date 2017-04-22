package agent

import (
	"flag"
	"github.com/jonbonazza/huton/command"
	"github.com/jonbonazza/huton/lib"
	"github.com/mitchellh/cli"
	"os"
	"os/signal"
	"syscall"
)

type Command struct {
	UI         cli.Ui
	ShutdownCh <-chan struct{}
	instance   huton.Instance
}

func (c *Command) readConfig() (string, *huton.Config, error) {
	config := huton.DefaultConfig()
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	var name string
	flags.StringVar(&name, "name", "", "unique instance name")
	flags.StringVar(&config.BindAddr, "bindAddr", config.BindAddr, "address to bind serf to")
	flags.IntVar(&config.BindPort, "bindPort", config.BindPort, "port to bind serf to")
	flags.Var((*command.AppendSliceValue)(&config.Peers), "peers", "peer list")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return "", nil, err
	}
	return name, config, nil
}

func (c *Command) Run(args []string) int {
	name, config, err := c.readConfig()
	if err != nil {
		return 1
	}
	c.instance, err = huton.NewInstance(name, config)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	return c.handleSignals()
}

func (c *Command) handleSignals() int {
	signalCh := make(chan os.Signal, 3)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
		c.instance.Shutdown()
		return 0
	case <-c.ShutdownCh:
		c.instance.Shutdown()
		return 0
	}
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	return ""
}
