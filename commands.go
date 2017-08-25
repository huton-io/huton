package main

import (
	"os"

	"github.com/huton-io/huton/command/agent"
	"github.com/mitchellh/cli"
)

func commands() map[string]cli.CommandFactory {
	ui := &cli.BasicUi{
		Writer: os.Stdout,
	}

	return map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &agent.Command{
				UI: ui,
			}, nil
		},
	}
}
