package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"
)

func main() {
	c := &cli.CLI{
		Args:     os.Args[1:],
		Commands: commands(),
		HelpFunc: cli.BasicHelpFunc("huton"),
	}
	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		exitCode = 1
	}
	os.Exit(exitCode)
}
