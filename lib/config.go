package huton

import (
	"fmt"
	"net"
)

type Config struct {
	BindAddr string   `json:"bindAddr" yaml:"bindAddr"`
	BindPort int      `json:"bindPort" yaml:"bindPort"`
	Peers    []string `json:"peers" yaml:"peers"`
}

func (c *Config) Addr() (*net.TCPAddr, error) {
	return net.ResolveIPAddr("tcp", fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort))
}
