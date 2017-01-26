package huton

import (
	"fmt"
	"github.com/hashicorp/serf/serf"
	"strconv"
)

func (i *instance) setupSerf() error {
	config := serf.DefaultConfig()
	config.Init()
	addr, err := i.config.Addr()
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
