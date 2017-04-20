package huton

import (
	"github.com/jonbonazza/huton/lib/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
)

func (i *instance) setupRPC() (err error) {
	addr := net.JoinHostPort(i.config.Serf.MemberlistConfig.BindAddr, strconv.Itoa(i.config.Serf.MemberlistConfig.BindPort+2))
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

func (i *instance) OnCommand(ctx context.Context, cmd *huton_proto.Command) (*huton_proto.Response, error) {
	err := i.apply(cmd)
	return &huton_proto.Response{}, err
}

func (i *instance) sendCommand(peer *Peer, cmd *huton_proto.Command) error {
	conn, err := grpc.Dial(peer.RPCAddr.String(), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := huton_proto.NewRecieverClient(conn)
	_, err = client.OnCommand(context.Background(), cmd)
	return err
}
