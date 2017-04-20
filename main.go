package main

import (
	"fmt"
	"github.com/mitchellh/cli"
	"os"
)

func main() {
	c := &cli.CLI{
		Args:     os.Args[1:],
		Commands: Commands(),
		HelpFunc: cli.BasicHelpFunc("huton"),
	}
	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		exitCode = 1
	}
	os.Exit(exitCode)
}
