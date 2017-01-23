package huton

import (
	"github.com/hashicorp/serf/serf"
	"os"
)

func (i *instance) setupSerf() error {
	config := serf.DefaultConfig()
	config.Init()
	addr, err := i.config.Addr()
	if err != nil {
		return err
	}
	config.MemberlistConfig.BindAddr = addr.IP
	config.MemberlistConfig.BindPort = addr.Port
	config.NodeName, _ = os.Hostname()
	config.Tags["id"] = config.NodeName
	config.Tags["port"] = addr.Port
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
