package huton

import (
	"errors"
	"net"
	"strconv"

	"strings"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func (i *instance) setupSerf(raftAddr *net.TCPAddr, rpcAddr *net.TCPAddr) (err error) {
	serfConfig, err := i.getSerfConfig(raftAddr, rpcAddr)
	if err != nil {
		return err
	}
	i.serf, err = serf.Create(serfConfig)
	return err
}

func (i *instance) getSerfConfig(raftAddr *net.TCPAddr, rpcAddr *net.TCPAddr) (*serf.Config, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.EnableNameConflictResolution = false
	serfConfig.MemberlistConfig.BindAddr = i.bindAddr
	serfConfig.MemberlistConfig.BindPort = i.bindPort
	serfConfig.NodeName = i.name
	serfConfig.EventCh = i.serfEventChannel
	if i.encryptionKey != nil && len(i.encryptionKey) != 32 {
		return serfConfig, errors.New("invalid encryption key length. Encryption key must be 32-bytes")
	}
	serfConfig.MemberlistConfig.SecretKey = i.encryptionKey
	tags := make(map[string]string)
	tags["id"] = serfConfig.NodeName
	tags["raftIP"] = raftAddr.IP.String()
	tags["raftPort"] = strconv.Itoa(raftAddr.Port)
	tags["rpcIP"] = rpcAddr.IP.String()
	tags["rpcPort"] = strconv.Itoa(rpcAddr.Port)
	if i.bootstrap {
		tags["bootstrap"] = "1"
	}
	tags["expect"] = strconv.Itoa(i.bootstrapExpect)
	serfConfig.Tags = tags
	return serfConfig, nil
}

func (i *instance) handleSerfEvent(event serf.Event) {
	switch event.EventType() {
	case serf.EventMemberJoin:
		i.peerJoined(event.(serf.MemberEvent))
	case serf.EventMemberLeave, serf.EventMemberFailed:
		i.peerGone(event.(serf.MemberEvent))
	default:
		i.logger.Printf("[WARN] unhandled serf event: %#v", event)
	}
}

func (i *instance) peerJoined(event serf.MemberEvent) {
	i.logger.Println("[INFO] member join")
	for _, member := range event.Members {
		peer, err := newPeer(member)
		if err != nil {
			i.logger.Printf("[ERR] failed to construct peer: %v", err)
			return
		}
		i.peersMu.Lock()
		raftAddr := peer.RaftAddr.String()
		var exists bool
		if _, exists = i.peers[raftAddr]; !exists {
			i.peers[raftAddr] = peer
		}
		i.peersMu.Unlock()
		if i.bootstrapExpect > 0 {
			i.logger.Printf("[INFO] testing bootstrap")
			i.maybeBootstrap()
		} else if i.IsLeader() {
			i.raft.AddVoter(raft.ServerID(peer.Name), raft.ServerAddress(peer.RaftAddr.String()), 0, 0)
		}
	}
}

func (i *instance) peerGone(event serf.MemberEvent) {
	for _, member := range event.Members {
		peer, err := newPeer(member)
		if err == nil {
			i.peersMu.Lock()
			delete(i.peers, peer.RaftAddr.String())
			i.peersMu.Unlock()
		}
	}
}

func (i *instance) maybeBootstrap() {
	index, err := i.raftBoltStore.LastIndex()
	if err != nil {
		i.logger.Printf("[ERR] failed to read last raft index: %v", err)
		return
	}
	if index != 0 {
		i.logger.Println("[INFO] raft data found, disabling boostrap mode")
		i.bootstrapExpect = 0
		return
	}
	var peers []Peer
	i.peersMu.Lock()
	for _, peer := range i.peers {
		if peer.Expect != 0 && peer.Expect != i.bootstrapExpect {
			i.logger.Printf("[ERR] Member %v has a conflicting expect value. All nodes should expect the same number.", peer)
			return
		}
		if peer.Bootstrap {
			i.logger.Printf("[ERR] Member %v has bootstrap mode. Expect disabled.", peer)
			return
		}
		peers = append(peers, *peer)
	}
	i.peersMu.Unlock()
	if len(peers) < i.bootstrapExpect {
		return
	}
	var configuration raft.Configuration
	var addrs []string
	for _, peer := range peers {
		addr := peer.RaftAddr.String()
		addrs = append(addrs, addr)
		id := raft.ServerID(peer.Name)
		raftServer := raft.Server{
			ID:      id,
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, raftServer)
	}
	i.logger.Printf("[INFO] found expected number of peers, attempting boostrap: %s", strings.Join(addrs, ","))
	future := i.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		i.logger.Printf("[ERR] failed to bootstrap cluster: %v", err)
	}
	i.bootstrapExpect = 0
}
