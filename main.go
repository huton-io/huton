package main

import (
	"fmt"
	"os"

	"github.com/huton-io/huton/command"
	"github.com/mitchellh/cli"
)

var version string

func main() {
	c := &cli.CLI{
		Args:     os.Args[1:],
		Commands: command.Map(),
		HelpFunc: cli.BasicHelpFunc("huton"),
		Version:  version,
	}
	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		exitCode = 1
	}
	os.Exit(exitCode)
}
