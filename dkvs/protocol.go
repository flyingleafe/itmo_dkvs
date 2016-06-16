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
	MPrepare      // Here start service messages
	MPrepareOk
	MCommit
	MStartViewChange
	MDoViewChange
	MStartView
	MRecovery
	MRecoveryResponse
	MNode
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
	case "prepare":
		return MPrepare, nil
	case "prepare_ok":
		return MPrepareOk, nil
	case "commit":
		return MCommit, nil
	case "start_view_change":
		return MStartViewChange, nil
	case "do_view_change":
		return MDoViewChange, nil
	case "start_view":
		return MStartView, nil
	case "recovery":
		return MRecovery, nil
	case "recovery_response":
		return MRecoveryResponse, nil
	case "node":
		return MNode, nil
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
	case MPrepare:
		return "prepare"
	case MPrepareOk:
		return "prepare_ok"
	case MCommit:
		return "commit"
	case MStartViewChange:
		return "start_view_change"
	case MDoViewChange:
		return "do_view_change"
	case MStartView:
		return "start_view"
	case MRecovery:
		return "recovery"
	case MRecoveryResponse:
		return "recovery_response"
	case MNode:
		return "node"
	}
	return "ping"
}

func ParseMessage(s string) (*Message, error) {
	parts := strings.SplitN(s, " ", 2)
	if len(parts) < 1 {
		return nil, fmt.Errorf("Empty message")
	}

	msgType, err := parseMsgType(parts[0])
	if err != nil {
		return nil, err
	}

	needArgs := 0
	switch msgType {
	case MPing:
	case MGet:
		fallthrough
	case MDelete:
		needArgs = 1
	case MSet:
		needArgs = 2
	case MPrepare:
		needArgs = 4
	case MPrepareOk:
		needArgs = 3
	case MCommit:
		needArgs = 2
	case MStartViewChange:
		needArgs = 2
	case MDoViewChange:
		needArgs = 6
	case MStartView:
		needArgs = 4
	case MRecovery:
		needArgs = 2
	case MRecoveryResponse:
		needArgs = 6
	}

	args := strings.SplitN(parts[0], " ", needArgs)

	if len(args) != needArgs {
		return nil, fmt.Errorf("Wrong number of arguments for method: %s", s)
	}
	return &Message{msgType, args}, nil
}

func PrintMessage(m *Message) string {
	return msgTypeToString(m.Type) + " " + strings.Join(m.Params, " ")
}

func IsChangingOp(m *Message) bool {
	return m != nil && (m.Type == MSet || m.Type == MDelete)
}

func IsServiceMsg(m *Message) bool {
	return m != nil && (m.Type > 4)
}
