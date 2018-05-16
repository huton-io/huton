package huton

import (
	"errors"
	"net"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/huton-io/huton/lib/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// ErrUnsupportedMessageType is an error for when an RPC is of an unexpected type.
var ErrUnsupportedMessageType = errors.New("unsupported message type")

type rpcServer struct {
	delegate *grpc.Server
	instance *Instance
}

func (s *rpcServer) Set(ctx context.Context, r *huton_proto.CacheSet) (*huton_proto.CacheSetResp, error) {
	resp := &huton_proto.CacheSetResp{}
	b, err := proto.Marshal(r)
	if err != nil {
		return resp, err
	}
	err = s.instance.apply(cacheOpSet, b)
	return resp, err
}

func (s *rpcServer) Del(ctx context.Context, r *huton_proto.CacheDel) (*huton_proto.CacheDelResp, error) {
	resp := &huton_proto.CacheDelResp{}
	b, err := proto.Marshal(r)
	if err != nil {
		return resp, err
	}
	err = s.instance.apply(cacheOpDel, b)
	return resp, err
}

func (s *rpcServer) setup() error {
	bindHost := s.instance.config.BindHost
	if bindHost == "" {
		bindHost = "127.0.0.1"
	}
	addr := net.JoinHostPort(bindHost, strconv.Itoa(s.instance.config.BindPort+2))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.delegate = grpc.NewServer()
	huton_proto.RegisterCacheServer(s.delegate, s)
	reflection.Register(s.delegate)
	go s.delegate.Serve(listener)
	return nil
}

func (s *rpcServer) Stop() {
	s.delegate.Stop()
}

func (i *Instance) sendRPC(ctx context.Context, peer *Peer, msg proto.Message) error {
	conn, err := grpc.Dial(peer.RPCAddr.String(), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := huton_proto.NewCacheClient(conn)
	switch v := msg.(type) {
	case *huton_proto.CacheSet:
		_, err = client.Set(ctx, v)
	case *huton_proto.CacheDel:
		_, err = client.Del(ctx, v)
	default:
		return ErrUnsupportedMessageType
	}
	return err
}

func (i *Instance) sendRPCToLeader(ctx context.Context, msg proto.Message) error {
	leader := i.raft.Leader()
	i.peersMu.Lock()
	defer i.peersMu.Unlock()
	return i.sendRPC(ctx, i.peers[string(leader)], msg)
}

func newRPCServer(instance *Instance) (*rpcServer, error) {
	server := &rpcServer{
		instance: instance,
	}
	err := server.setup()
	return server, err
}
