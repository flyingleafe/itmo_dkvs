package dkvs

import (
	"strings"
	"fmt"
	"errors"
	"strconv"
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


func MakeMessage(tp MessageType, params ...interface{}) *Message {
	strParams := make([]string, 0)

	for _, param := range params {
		switch v := param.(type) {
		case int:
			strParams = append(strParams, strconv.Itoa(v))
		case int64:
			strParams = append(strParams, strconv.FormatInt(v, 10))
		case string:
			strParams = append(strParams, v)
		case *Message:
			strParams = append(strParams, PrintMessage(v))
		}
	}

	return &Message{tp, strParams}
}

func ParseMessage(s string) (*Message, error) {
	s = strings.Join(strings.Fields(s), " ")
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
	case MNode:
		needArgs = 1
	}

	args := []string{}

	if len(parts) > 1 {
		args = strings.SplitN(parts[1], " ", needArgs)
	}

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

func IsErrResp(resp string) bool {
	return strings.HasPrefix(resp, "ERR")
}

func ParsePrepareOk(resp string) (int64, int64, int64, error) {
	msg, err := ParseMessage(resp)
	if err != nil {
		return -1, -1, -1, err
	} else if msg.Type != MPrepareOk {
		return -1, -1, -1, errors.New("Not a PrepareOk response")
	}

	vnum, _ := strconv.ParseInt(msg.Params[0], 10, 64)
	opnum, _ := strconv.ParseInt(msg.Params[1], 10, 64)
	nodenum, _ := strconv.ParseInt(msg.Params[2], 10, 64)
	return vnum, opnum, nodenum, nil
}

func ParseRecoveryResponse(resp string) (int, string, int64, int64, int64, string, error) {
	msg, err := ParseMessage(resp)
	if err != nil {
		return -1, "", -1, -1, -1, "", err
	} else if msg.Type != MRecoveryResponse {
		return -1, "", -1, -1, -1, "", errors.New("Not a RecoveryResponse")
	}

	nodenum, _ := strconv.Atoi(msg.Params[0])
	nonce := msg.Params[1]
	vnum, _ := strconv.ParseInt(msg.Params[2], 10, 64)
	opnum, _ := strconv.ParseInt(msg.Params[3], 10, 64)
	comnum, _ := strconv.ParseInt(msg.Params[4], 10, 64)
	log := msg.Params[5]
	return nodenum, nonce, vnum, opnum, comnum, log, nil
}
