package huton

import (
	"encoding/json"
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
)

// Peer contains information about a cluster member.
type Peer struct {
	Name      string
	SerfAddr  *net.TCPAddr
	RaftAddr  *net.TCPAddr
	RPCAddr   *net.TCPAddr
	Expect    int
	Bootstrap bool
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
	expect, err := strconv.Atoi(member.Tags["expect"])
	if err != nil {
		return nil, err
	}
	return &Peer{
		Name: member.Tags["id"],
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
		Expect:    expect,
		Bootstrap: member.Tags["boostrap"] == "1",
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
		if p.Name == i.name {
			return p
		}
	}
	return nil
}
