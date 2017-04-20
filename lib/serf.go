package huton

import (
	"github.com/hashicorp/serf/serf"
	"net"
	"strconv"
)

func (i *instance) setupSerf(serfConfig *serf.Config, raftAddr *net.TCPAddr) error {
	serfConfig.EventCh = i.serfEventChannel
	tags := make(map[string]string)
	tags["id"] = serfConfig.NodeName
	tags["raftIP"] = raftAddr.IP.String()
	tags["raftPort"] = strconv.Itoa(raftAddr.Port)
	serfConfig.Tags = tags
	s, err := serf.Create(serfConfig)
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
	for _, member := range event.Members {
		peer, err := newPeer(member)
		if err == nil {
			i.peersMu.Lock()
			var exists bool
			if _, exists = i.peers[peer.ID]; !exists {
				i.peers[peer.ID] = peer
			}
			i.peersMu.Unlock()
			if !exists {
				i.addRaftPeer(peer)
			}
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
