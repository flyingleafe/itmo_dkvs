package dkvs

import (
	"strings"
	"fmt"
)

type (
	MessageType int

	Message struct {
		Type   MessageType
		Params []string
	}
)

const (
	MGet MessageType = iota+1
	MSet
	MDelete
	MPing
)

func parseMsgType(s string) (MessageType, error) {
	switch s {
	case "get":
		return MGet, nil
	case "set":
		return MSet, nil
	case "delete":
		return MDelete, nil
	case "ping":
		return MPing, nil
	default:
		return 0, fmt.Errorf("Undefined message type: %s", s)
	}
}

func msgTypeToString(t MessageType) string {
	switch t {
	case MGet:
		return "get"
	case MSet:
		return "set"
	case MDelete:
		return "delete"
	}
	return "ping"
}

func ParseMessage(s string) (*Message, error) {
	parts := strings.SplitN(s, " ", 3)
	if len(parts) < 1 {
		return nil, fmt.Errorf("Empty message")
	}

	msgType, err := parseMsgType(parts[0])
	if err != nil {
		return nil, err
	}

	needArgs := 1
	switch msgType {
	case MPing:
	case MGet:
		fallthrough
	case MDelete:
		needArgs = 2
	case MSet:
		needArgs = 3
	}

	if len(parts) != needArgs {
		return nil, fmt.Errorf("Wrong number of arguments for method: %s", s)
	}
	return &Message{msgType, parts[1:]}, nil
}

func PrintMessage(m *Message) string {
	return msgTypeToString(m.Type) + " " + strings.Join(m.Params, " ")
}
