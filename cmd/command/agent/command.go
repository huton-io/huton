package agent

import (
	"flag"
	"fmt"
	"github.com/jonbonazza/huton/cmd/command"
	"github.com/jonbonazza/huton/lib"
	"github.com/mitchellh/cli"
	"os"
	"os/signal"
	"syscall"
)

type Command struct {
	UI         cli.Ui
	ShutdownCh <-chan struct{}
}

func (c *Command) readConfig() (*huton.Config, error) {
	config := huton.DefaultConfig()
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() {
		c.UI.Output(c.Help())
	}
	flags.StringVar(&config.Serf.BindAddr, "serfBind", "0.0.0.0", "address to bind serf to")
	flags.IntVar(&config.Serf.BindPort, "serfPort", 8080, "port to bind serf to")
	flags.StringVar(&config.Raft.BindAddr, "raftBind", "0.0.0.0", "address to bind raft to")
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
	_, err = huton.NewInstance(config)
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
		return 0
	case <-c.ShutdownCh:
		return 0
	}
}

func (c *Command) Synopsis() string {
	return ""
}

func (c *Command) Help() string {
	return ""
}
