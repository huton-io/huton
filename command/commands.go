package command

import (
	"os"

	"github.com/huton-io/huton/command/agent"
	"github.com/mitchellh/cli"
)

// Map returns a map of command name to cli.CommandFactory to be used by the cli runtime.
func Map() map[string]cli.CommandFactory {
	ui := &cli.BasicUi{
		Writer: os.Stdout,
	}
	return map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return agent.New(ui), nil
		},
	}
}
