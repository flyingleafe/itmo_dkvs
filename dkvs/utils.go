package dkvs

import (
	"net"
	"math/rand"
	"time"
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandSeq(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
