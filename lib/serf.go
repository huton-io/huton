package huton

import (
	"fmt"
	"github.com/hashicorp/serf/serf"
	"net"
	"strconv"
)

type SerfConfig struct {
	BindAddr string `json:"bindAddr" yaml:"bindAddr"`
	BindPort int    `json:"bindPort" yaml:"bindPort"`
}

func (c *SerfConfig) Addr() (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort))
}

func (i *instance) setupSerf() error {
	config := serf.DefaultConfig()
	config.Init()
	addr, err := i.config.Serf.Addr()
	if err != nil {
		return err
	}
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	config.NodeName = fmt.Sprintf("%s:%d", config.MemberlistConfig.BindAddr, config.MemberlistConfig.BindPort)
	config.Tags["id"] = config.NodeName
	config.Tags["port"] = strconv.Itoa(addr.Port)
	config.EventCh = i.serfEventChannel
	config.EnableNameConflictResolution = false
	s, err := serf.Create(config)
	if err != nil {
		return err
	}
	i.serf = s
	if len(i.config.Peers) > 0 {
		if _, err := s.Join(i.config.Peers, true); err != nil {
			return err
		}
	}
	return nil
}

func (i *instance) handleSerfEvent(event serf.Event) {
	switch event.EventType() {
	case serf.EventMemberJoin:
		i.peerJoined(event.(serf.MemberEvent))
	case serf.EventMemberLeave, serf.EventMemberFailed:
		i.peerGone(event.(serf.MemberEvent))
	}
}

func (i *instance) peerJoined(event serf.MemberEvent) {
	for _, m := range event.Members {
		i.applyCommand(joinCmd, "", fmt.Sprintf("%s:%d", m.Addr.String(), m.Port))
	}
}

func (i *instance) peerGone(event serf.MemberEvent) {
	for _, m := range event.Members {
		i.applyCommand(leaveCmd, "", fmt.Sprintf("%s:%d", m.Addr.String(), m.Port))
	}
}
