package main

import (
	"github.com/jonbonazza/huton/command/agent"
	"github.com/jonbonazza/huton/command/members"
	"github.com/mitchellh/cli"
	"os"
)

func commands() map[string]cli.CommandFactory {
	ui := &cli.BasicUi{
		Writer: os.Stdout,
	}

	return map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &agent.Command{
				UI:         ui,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"members": func() (cli.Command, error) {
			return &members.Command{
				UI: ui,
			}, nil
		},
	}
}
