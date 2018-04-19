package agent

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/huton-io/huton/lib"
	"github.com/mitchellh/cli"
)

const (
	synopsis = "Runs a Huton agent"
	help     = `
	Usage: huton agent [options]

	Starts a Huton agent.
`
)

// New returns a new agent command.
func New(ui cli.Ui) cli.Command {
	c := &cmd{UI: ui}
	c.init()
	return c
}

type cmd struct {
	UI       cli.Ui
	help     string
	conf     *config
	flagSet  *flag.FlagSet
	instance huton.Instance
}

func (c *cmd) Run(args []string) int {
	if err := c.flagSet.Parse(args); err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	return c.run()
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return c.help
}

func (c *cmd) run() int {
	opts, err := c.conf.options()
	if err != nil {
		c.UI.Error(fmt.Sprintf("failed to parse options: %s", err))
		return 1
	}
	c.instance, err = huton.NewInstance(c.conf.name, opts...)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	_, err = c.instance.Join(c.conf.peers)
	if err != nil {
		c.UI.Error(err.Error())
		return 1
	}
	c.instance.WaitForReady()
	return c.handleSignals()
}

func (c *cmd) init() {
	c.flagSet = flag.NewFlagSet("agent", flag.ContinueOnError)
	c.conf = addFlags(c.flagSet)
	var buf bytes.Buffer
	c.flagSet.SetOutput(&buf)
	c.flagSet.Usage()
	c.help = buf.String()
}

func (c *cmd) handleSignals() int {
	signalCh := make(chan os.Signal, 3)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
		return 0
	}
}
