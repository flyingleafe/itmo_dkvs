package dkvs

import (
	"time"
	"errors"
	"io"
	"bufio"
	"strings"
	"fmt"
	"strconv"
)

type (
	ServerConfig struct {
		Nodes   map[int]ServerAddr
		Timeout time.Duration
	}
)

func ParseConfig(r io.Reader) (*ServerConfig, error) {
	br := bufio.NewReader(r)
	res := &ServerConfig{
		Nodes: make(map[int]ServerAddr),
		Timeout: time.Duration(0),
	}

	line, _, err := br.ReadLine()
	for ; err == nil; line, _, err = br.ReadLine() {
		parts := strings.SplitN(string(line), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid config format: %s", line)
		}

		if parts[0] == "timeout" {
			timeout, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("Error while parsing timeout: %s", err)
			}
			res.Timeout = time.Duration(timeout) * time.Millisecond
		} else {
			num, err := strconv.Atoi(parts[0][5:])
			if err != nil {
				return nil, fmt.Errorf("Error while parsing node number: %s", err)
			}

			addr, err := SplitNetworkAddr(parts[1])
			if err != nil {
				return nil, fmt.Errorf("Error while parsing node address: %s", err)
			}

			res.Nodes[num] = addr
		}
	}

	if len(res.Nodes) == 0 {
		return nil, errors.New("No nodes provided in config")
	}

	if res.Timeout == time.Duration(0) {
		return nil, errors.New("No timeout (or zero timeout) provided in config")
	}

	return res, nil
}
