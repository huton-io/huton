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

func (i *instance) setupSerf(addr *net.TCPAddr) error {
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	config.NodeName = fmt.Sprintf("%s:%d", config.MemberlistConfig.BindAddr, config.MemberlistConfig.BindPort)
	fmt.Println(config.NodeName)
	fmt.Println(config.NodeName)
	config.Tags["id"] = config.NodeName
	config.Tags["raftIP"] = i.config.Raft.BindAddr
	config.Tags["raftPort"] = strconv.Itoa(i.config.Raft.BindPort)
	config.EventCh = i.serfEventChannel
	config.EnableNameConflictResolution = false
	s, err := serf.Create(config)
	if err != nil {
		return err
	}
	i.serf = s
	if len(i.config.Peers) > 0 {
		s.Join(i.config.Peers, true)
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
	for _, member := range event.Members {
		peer, err := newPeer(member)
		if err == nil {
			i.peersMu.Lock()
			i.peers[peer.ID] = peer
			i.peersMu.Unlock()
			i.addRaftPeer(peer)
		}
	}
}

func (i *instance) peerGone(event serf.MemberEvent) {
	for _, member := range event.Members {
		peer, err := newPeer(member)
		if err == nil {
			i.peersMu.Lock()
			delete(i.peers, peer.ID)
			i.peersMu.Unlock()
			i.removeRaftPeer(peer)
		}
	}
}
