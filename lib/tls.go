package huton

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type tlsStream struct {
	net.Listener
	advertise net.Addr
	tlsConfig *tls.Config
}

func (t *tlsStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{
		Timeout: timeout,
	}
	return tls.DialWithDialer(&d, "tcp", string(address), t.tlsConfig)
}

func (t *tlsStream) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.Listener.Addr()
}

func newTLSTransport(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, tlsConfig *tls.Config, logOutput io.Writer) (*raft.NetworkTransport, error) {
	l, err := tls.Listen("tcp", bindAddr, tlsConfig)
	if err != nil {
		return nil, err
	}
	stream := &tlsStream{
		Listener:  l,
		advertise: advertise,
		tlsConfig: tlsConfig,
	}
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		l.Close()
		return nil, errors.New("local address is not a TCP address")
	}
	if addr.IP.IsUnspecified() {
		l.Close()
		return nil, errors.New("local bind address is not advertisable")
	}
	trans := raft.NewNetworkTransport(stream, maxPool, timeout, logOutput)
	return trans, nil
}
