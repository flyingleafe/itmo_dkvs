package main

import (
	"time"
	"io"
	// "os"
	"fmt"
	"dkvs"
	"net"
	"bufio"
	"sync/atomic"
	"sync"
	"strconv"
	"strings"
	"bytes"
)

type (
	NodeStatus int32

	RequestPack struct {
		msg     *dkvs.Message
		opNum    int64
		respBin  chan string
	}

	ServerNode struct {
		Number   int
		Database map[string]string
		Journal  *NodeJournal
		Config   *dkvs.ServerConfig

		listener       net.Listener
		needStop       int32
		incomingReqs   chan *RequestPack
		serviceMsgs    chan *RequestPack

		siblings     map[int]*SiblingConn

		status       NodeStatus
		statusChan   chan bool
		statusLock   sync.RWMutex

		leaderNumber int // Number of current leader

		viewNumber   int64 // View number
		opNumber     int64 // Number of last view-changing operation request (set/delete)
		commitNumber int64 // Number of last commited change
	}
)

const (
	Normal NodeStatus = iota+1
	ViewChange
	Recovering
)

func MakeRequest(tp dkvs.MessageType, params ...string) *RequestPack {
	return &RequestPack{
		msg: dkvs.MakeMessage(tp, params...),
		opNum: -1,
		respBin: make(chan string, 1),
	}
}

// Initialize data structure for server node
func MakeNode(num int, journalFilename string, config *dkvs.ServerConfig) (*ServerNode, error) {
	journal, err := MakeJournal(journalFilename)
	if err != nil {
		return nil, err
	}

	return &ServerNode{
		Number:       num,
		Database:     make(map[string]string),
		Journal:      journal,
		Config:       config,
		needStop:     0,
		incomingReqs: make(chan *RequestPack, 4096),
		serviceMsgs:  make(chan *RequestPack, 4096),
		siblings:     MakeSiblings(num, config),
		status:       Recovering,
		statusChan:   make(chan bool, 1),
		statusLock:   sync.RWMutex{},
		leaderNumber: 1,
		viewNumber:   0,
		opNumber:     0,
		commitNumber: 0,
	}, nil
}

// Start server: read journal and listen to connections
func (node *ServerNode) Start() error {
	if err := node.RestoreFromJournal(-1); err != nil {
		return err
	}

	myAddr := node.Config.Nodes[node.Number]

	ln, err := net.Listen(`tcp4`, fmt.Sprintf(`%s:%d`, myAddr.Host, myAddr.Port))
	if err != nil {
		return err
	}

	aliveCount := node.TryConnectSiblings()
	node.RunSiblings()

	if aliveCount > (len(node.siblings) + 1) / 2 {
		logOk.Println("Start recovering of state from nodes")
		node.RecoveryNode()
	}

	node.listener = ln
	logOk.Printf("Started node on %s:%d\n", myAddr.Host, myAddr.Port)

	node.ChangeStatus(Normal)

	go func() {
		needStop := atomic.LoadInt32(&node.needStop)
		for ; needStop != 1; needStop = atomic.LoadInt32(&node.needStop) {
			if conn, err := ln.Accept(); err != nil {
				if needStop != 1 {
					logErr.Println(`server listen accept fail`, err)
				}
			} else {
				logOk.Println(`accepted connection`)
				go node.Interact(conn)
			}
		}

		close(node.incomingReqs)
		logOk.Println("Server stopped")
	}()

	go node.ProcessMsgs()
	go node.ProcessServiceMsgs()

	return nil
}

// Stop server
func (node *ServerNode) Stop() {
	logOk.Println(`Stopping server`)
	atomic.StoreInt32(&node.needStop, 1)
	if node.listener != nil {
		node.listener.Close()
	}
	node.listener = nil
}

func (node *ServerNode) takeMessage(br *bufio.Reader, conn net.Conn) (*dkvs.Message, bool, error) {
	msg, _, err := br.ReadLine()
	if err == io.EOF {
		return nil, true, err
	} else if err != nil {
		if strings.Contains(err.Error(), "use of closed network connection") {
			logErr.Println("Connection is already closed")
			return nil, true, err
		}
		logErr.Println(`Error while receiving message:`, err)
		return nil, false, err
	}

	mesg, err := dkvs.ParseMessage(string(msg))
	if err != nil {
		logErr.Println(`Invalid message:`, err)
		conn.Write([]byte("ERR " + err.Error() + "\n"))
		return nil, false, err
	}

	logOk.Printf("QUERY (%s): %s\n", conn.RemoteAddr().String(), msg)
	return mesg, false, nil
}

func (node *ServerNode) sendResponse(conn net.Conn, ans string) {
	logOk.Printf("RESPONSE (%s): %s\n", conn.RemoteAddr().String(), ans)

	_, err := conn.Write([]byte(ans + "\n"))
	if err != nil {
		logErr.Println(`Error while answering query:`, err)
	}
}

// Interact with one of connections
func (node *ServerNode) Interact(conn net.Conn) {
	logOk.Println("Starting interaction with", conn.RemoteAddr().String())

	conn.SetReadDeadline(time.Time{})
	br := bufio.NewReader(conn)

	needStop := atomic.LoadInt32(&node.needStop)
	for ; needStop != 1; needStop = atomic.LoadInt32(&node.needStop) {
		mesg, toBreak, err := node.takeMessage(br, conn)
		if err != nil {
			if toBreak {
				break
			}
			continue
		}

		if mesg.Type == dkvs.MNode {
			// This is sibling node wants to connect
			nodeNum, err := strconv.Atoi(mesg.Params[0])
			if err != nil {
				logErr.Println("Invalid node number:", err)
				conn.Write([]byte("ERR " + err.Error() + "\n"))
				continue
			}

			conn.Write([]byte("ACCEPTED\n"))
			node.InteractSibling(nodeNum, conn)
			break
		}

		curOpNum := int64(-1)
		if node.Number == node.GetLeaderNumber() && dkvs.IsChangingOp(mesg) {
			curOpNum = atomic.AddInt64(&node.opNumber, 1)
		}

		pack := &RequestPack{
			msg:     mesg,
			opNum:   curOpNum,
			respBin: make(chan string, 1),
		}

		node.incomingReqs <- pack
		ans := <-pack.respBin
		node.sendResponse(conn, ans)
	}

	logOk.Println("Closing connection with", conn.RemoteAddr().String())
	conn.Close()
}

func (node *ServerNode) InteractSibling(num int, conn net.Conn) {

	node.siblings[num].fromConn = conn
	br := bufio.NewReader(conn)

	needStop := atomic.LoadInt32(&node.needStop)
	for ; needStop != 1; needStop = atomic.LoadInt32(&node.needStop) {
		mesg, toBreak, err := node.takeMessage(br, conn)
		if err != nil {
			if toBreak {
				break
			}
			continue
		}

		curOpNum := int64(-1)
		if node.Number == node.GetLeaderNumber() && dkvs.IsChangingOp(mesg) {
			curOpNum = atomic.AddInt64(&node.opNumber, 1)
		}

		pack := &RequestPack{
			msg:     mesg,
			opNum:   curOpNum,
			respBin: make(chan string, 1),
		}

		if dkvs.IsServiceMsg(mesg) {
			node.serviceMsgs <- pack
		} else {
			node.incomingReqs <- pack
		}
		ans := <-pack.respBin
		node.sendResponse(conn, ans)
	}
}

// Process all the messages
func (node *ServerNode) ProcessMsgs() {
	for req := range node.incomingReqs {
		node.statusLock.RLock()

		// Make sure that current status is Normal before processing client request
		node.WaitForStatus(Normal)

		if !dkvs.IsChangingOp(req.msg) {
			req.respBin <- node.PerformAction(req.msg)
		} else {
			if node.Number == node.leaderNumber {
				node.WriteToJournal(req)
				go node.CommitRequestToAll(req)
			} else {
				node.siblings[node.leaderNumber].SendRequest(req)
			}
		}
		node.statusLock.RUnlock()
	}
}

func (node *ServerNode) PerformAction(m *dkvs.Message) string {
	switch m.Type {
	case dkvs.MPing:
		return "PONG"
	case dkvs.MGet:
		key := m.Params[0]
		if val, has := node.Database[key]; has {
			return "VALUE " + key + " " + val
		}
		return "NOT_FOUND"
	case dkvs.MSet:
		key := m.Params[0]
		val := m.Params[1]
		node.Database[key] = val
		return "STORED"
	case dkvs.MDelete:
		key := m.Params[0]
		if _, has := node.Database[key]; has {
			delete(node.Database, key)
			return "DELETED"
		}
		return "NOT_FOUND"
	}
	return "ERR Cannot be! Unsupported operation"
}

// Process all service messages
func (node *ServerNode) ProcessServiceMsgs() {
	for req := range node.serviceMsgs {
		node.statusLock.RLock()
		node.ProcessServiceMsg(req)
		node.statusLock.RUnlock()
	}
}

func (node *ServerNode) ProcessServiceMsg(req *RequestPack) {
	m := req.msg
	switch m.Type {
	case dkvs.MRecovery:

		// Start recovery only from normal status
		node.WaitForStatus(Normal)

		// nodeNum := m.Params[0]
		nonce := m.Params[1]
		vnum := atomic.LoadInt64(&node.viewNumber)
		opNum := int64(-1)
		comNum := int64(-1)
		log := ""
		if node.Number == node.leaderNumber {
			opNum = atomic.LoadInt64(&node.opNumber)
			comNum = atomic.LoadInt64(&node.commitNumber)
			log, _ = node.FetchJournal()
		}
		respMsg := dkvs.MakeMessage(dkvs.MRecoveryResponse,
			strconv.Itoa(node.Number),
			nonce,
			strconv.FormatInt(vnum, 10),
			strconv.FormatInt(opNum, 10),
			strconv.FormatInt(comNum, 10),
			log,
		)
		req.respBin <- dkvs.PrintMessage(respMsg)

	case dkvs.MPrepare:
		// Commit changes only from normal status
		node.WaitForStatus(Normal)

		vnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		onum, _ := strconv.ParseInt(m.Params[1], 10, 64)
		cnum, _ := strconv.ParseInt(m.Params[2], 10, 64)

		preparedMsg, _ := dkvs.ParseMessage(m.Params[3])
		vNum := atomic.LoadInt64(&node.viewNumber)
		if vNum != vnum {
			req.respBin <- "ERR Wrong view"
			break
		}

		opNum := atomic.LoadInt64(&node.opNumber)

		responseMsg := dkvs.PrintMessage(dkvs.MakeMessage(
			dkvs.MPrepareOk,
			strconv.FormatInt(vNum, 10),
			strconv.FormatInt(opNum + 1, 10),
			strconv.Itoa(node.Number),
		))

		if onum - 1 == opNum {
			node.CommitTail(cnum)
			opNum = atomic.AddInt64(&node.opNumber, 1)
			node.WriteToJournal(&RequestPack{preparedMsg, opNum, nil})
			req.respBin <- responseMsg
		} else {
			go func(){
				node.RecoveryNode()
				node.ChangeStatus(Normal)
				req.respBin <- responseMsg
			}()
		}
	}
}

// ACHTUNG! Only to be used while statusLock is LOCKED!
func (node *ServerNode) WaitForStatus(statuses ...NodeStatus) {
	OuterLoop: for {
		for _, st := range statuses {
			if node.status == st {
				break OuterLoop
			}
		}
		node.statusLock.RUnlock()
		<-node.statusChan
		node.statusLock.RLock()
	}
}

func (node *ServerNode) ChangeStatus(newStatus NodeStatus) {
	node.statusLock.Lock()

	prevStatus := node.status
	node.status = newStatus

	node.statusLock.Unlock()

	if prevStatus != newStatus {
		node.statusChan <- true
	}
}

func (node *ServerNode) GetStatus() NodeStatus {
	node.statusLock.RLock()
	res := node.status
	node.statusLock.RUnlock()
	return res
}

func (node *ServerNode) GetLeaderNumber() int {
	node.statusLock.RLock()
	node.WaitForStatus(Normal, Recovering)
	lnum := node.leaderNumber
	node.statusLock.RUnlock()
	return lnum
}

func (node *ServerNode) SetLeaderNumber(newLeader int) {
	node.statusLock.RLock()
	node.WaitForStatus(ViewChange)
	node.leaderNumber = newLeader
	node.statusLock.RUnlock()
}

func (node *ServerNode) CommitRequestToAll(req *RequestPack) {
	comNum := atomic.LoadInt64(&node.commitNumber)
	viewNum := atomic.LoadInt64(&node.viewNumber)

	msg := dkvs.MakeMessage(dkvs.MPrepare,
		strconv.FormatInt(viewNum, 10),
		strconv.FormatInt(req.opNum, 10),
		strconv.FormatInt(comNum, 10),
		dkvs.PrintMessage(req.msg),
	)

	OuterLoop: for {
		agg := node.BroadcastMessage(msg)
		f := (len(node.siblings) + 1) / 2

		countReady := 0
		for i := 0; i < len(node.siblings); i++ {
			resp := <-agg
			if vn, on, _, err := dkvs.ParsePrepareOk(resp); err == nil && vn == viewNum && on == req.opNum {
				countReady++
				if countReady >= f {
					break OuterLoop
				}
			}
		}
	}

	resp := node.PerformAction(req.msg)
	atomic.AddInt64(&node.commitNumber, 1)
	req.respBin <- resp
}

func (node *ServerNode) RecoveryNode() {
	node.ChangeStatus(Recovering)
	nonce := dkvs.RandSeq(10)
	msg := dkvs.MakeMessage(dkvs.MRecovery, strconv.Itoa(node.Number), nonce)

	log := ""
	vNum := int64(-1)
	opNum := int64(-1)
	comNum := int64(-1)

	retries := 0
	OuterLoop: for {
		logOk.Printf("Recovery broadcast retry #%d", retries)

		agg := node.BroadcastMessage(msg)
		f := (len(node.siblings) + 1) / 2

		countReady := 0
		hadLeader := false

		for i := 0; i < len(node.siblings); i++ {
			resp := <-agg
			nnum, theirNonce, vnum, opnum, comnum, logOut, err := dkvs.ParseRecoveryResponse(resp)
			if err == nil && nonce == theirNonce {
				countReady++
				if nnum == node.GetLeaderNumber() {
					vNum = vnum
					opNum = opnum
					comNum = comnum
					log = logOut
					hadLeader = true
				}
				if hadLeader && countReady > f {
					break OuterLoop
				}
			}
		}
		retries++
	}

	newLog := []byte(strings.Replace(log, ";", "\n", -1))
	node.SubstituteJournal(bytes.NewBuffer(newLog))
	node.RestoreFromJournal(comNum)

	atomic.StoreInt64(&node.viewNumber, vNum)
	atomic.StoreInt64(&node.opNumber, opNum)
	atomic.StoreInt64(&node.commitNumber, comNum)

	logOk.Println("Recovery completed")
}
