package huton

import (
	"encoding/json"
	"github.com/hashicorp/serf/serf"
	"net"
	"strconv"
)

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
	return i.peers[i.id]
}
