package huton

import (
	"net"
	"strconv"

	"github.com/huton-io/huton/lib/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func (i *Instance) setupRPC() (err error) {
	addr := net.JoinHostPort(i.bindAddr, strconv.Itoa(i.bindPort+2))
	i.rpcListener, err = net.Listen("tcp", addr)
	if err != nil {
		return
	}
	i.rpc = grpc.NewServer()
	huton_proto.RegisterRecieverServer(i.rpc, i)
	reflection.Register(i.rpc)
	go i.rpc.Serve(i.rpcListener)
	return
}

// OnCommand is a callback triggered by the RPC server when a new huton Command is received.
func (i *Instance) OnCommand(ctx context.Context, cmd *huton_proto.Command) (*huton_proto.Response, error) {
	err := i.apply(cmd)
	return &huton_proto.Response{}, err
}

func (i *Instance) sendCommand(peer *Peer, cmd *huton_proto.Command) error {
	conn, err := grpc.Dial(peer.RPCAddr.String(), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := huton_proto.NewRecieverClient(conn)
	_, err = client.OnCommand(context.Background(), cmd)
	return err
}
