package huton

import (
	"encoding/json"
	"github.com/hashicorp/serf/serf"
	"net"
	"strconv"
)

// Peer contains information about a cluster member.
type Peer struct {
	ID       string
	SerfAddr *net.TCPAddr
	RaftAddr *net.TCPAddr
	RPCAddr  *net.TCPAddr
}

func (p *Peer) String() string {
	b, _ := json.Marshal(p)
	return string(b)
}

func newPeer(member serf.Member) (*Peer, error) {
	raftPort, err := strconv.Atoi(member.Tags["raftPort"])
	if err != nil {
		return nil, err
	}
	rpcPort, err := strconv.Atoi(member.Tags["rpcPort"])
	if err != nil {
		return nil, err
	}
	return &Peer{
		ID: member.Tags["id"],
		SerfAddr: &net.TCPAddr{
			IP:   member.Addr,
			Port: int(member.Port),
		},
		RaftAddr: &net.TCPAddr{
			IP:   net.ParseIP(member.Tags["raftIP"]),
			Port: raftPort,
		},
		RPCAddr: &net.TCPAddr{
			IP:   net.ParseIP(member.Tags["rpcIP"]),
			Port: rpcPort,
		},
	}, nil
}

func (i *instance) Peers() []*Peer {
	i.peersMu.Lock()
	defer i.peersMu.Unlock()
	var peers []*Peer
	for _, peer := range i.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (i *instance) Local() *Peer {
	i.peersMu.Lock()
	defer i.peersMu.Unlock()
	for _, p := range i.peers {
		if p.ID == i.id {
			return p
		}
	}
	return nil
}
