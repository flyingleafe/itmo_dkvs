package dkvs

import (
	"net"
)

type (
	ServerAddr struct {
		Host string
		Port uint16
	}
)

func SplitNetworkAddr(addr string) (ServerAddr, error) {
	if host, portStr, err := net.SplitHostPort(addr); err != nil {
		return ServerAddr{host, 0}, err
	} else if port, err := net.LookupPort(``, portStr); err != nil {
		return ServerAddr{host, 0}, err
	} else {
		return ServerAddr{host, uint16(port)}, nil
	}
}
