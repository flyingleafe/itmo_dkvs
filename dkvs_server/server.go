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

		normalViewNumber int64  // Last view number of normal state (used only while view changes)

		commitTimer *time.Timer
		lastCommit   time.Time

		startViewChangeMsgs map[int]*dkvs.Message
		doViewChangeMsgs    map[int]*dkvs.Message
	}
)

const (
	Normal NodeStatus = iota+1
	ViewChange
	Recovering
)

func MakeRequest(tp dkvs.MessageType, params ...interface{}) *RequestPack {
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
		normalViewNumber: 0,
		opNumber:     0,
		commitNumber: 0,
		commitTimer:  nil,
		lastCommit:   time.Now(),
		startViewChangeMsgs: make(map[int]*dkvs.Message),
		doViewChangeMsgs: make(map[int]*dkvs.Message),
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
	} else {
		node.ChangeStatus(Normal)
	}

	node.listener = ln
	logOk.Printf("Started node on %s:%d\n", myAddr.Host, myAddr.Port)

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

		logOk.Printf("QUERY (%s): %s\n", conn.RemoteAddr().String(), dkvs.PrintMessage(mesg))

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

		logOk.Printf("SERVICE QUERY (%s): %s\n", conn.RemoteAddr().String(), dkvs.PrintMessage(mesg))

		curOpNum := int64(-1)
		if node.status == Normal && node.Number == node.GetLeaderNumber() && dkvs.IsChangingOp(mesg) {
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
				go node.PrepareRequestToAll(req)
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
	node.resetCommitTimer(node.Config.Timeout * 2)
	go func() {
		needStop := atomic.LoadInt32(&node.needStop)
		for ; needStop != 1; needStop = atomic.LoadInt32(&node.needStop) {
			node.commitByTimer()
		}
	}()

	for req := range node.serviceMsgs {
		node.statusLock.RLock()
		switch node.status {
		case Normal:
			node.ProcessServiceNormal(req)
		case Recovering:
			node.ProcessServiceRecovering(req)
		case ViewChange:
			node.ProcessServiceViewChange(req)
		}
		node.statusLock.RUnlock()
	}
}

func (node *ServerNode) resetCommitTimer(timeout time.Duration) {
	if timeout == -1 {
		timeout = node.Config.Timeout
	}
	if node.commitTimer == nil {
		node.commitTimer = time.NewTimer(node.Config.Timeout)
	} else {
		if !node.commitTimer.Stop() {
			select {
			case <-node.commitTimer.C:
			default:
			}
		}
		node.commitTimer.Reset(node.Config.Timeout)
	}
}

func (node *ServerNode) commitByTimer() {
	curTime := <-node.commitTimer.C

	lnum := node.GetLeaderNumber()
	if lnum == node.Number {
		node.lastCommit = curTime
		node.CommitToAll()
	} else {
		logOk.Println("TIME CHECK")
		passedSinceLast := curTime.Sub(node.lastCommit)

		if passedSinceLast > node.Config.Timeout + 200 * time.Millisecond {
			logOk.Println("TIME IS OFFF!!!!")
			node.siblings[lnum].CloseBoth()
			node.InitViewChange()
		}
	}
	node.resetCommitTimer(-1)
}

func (node *ServerNode) ProcessServiceNormal(req *RequestPack) {
	m := req.msg

	switch m.Type {
	case dkvs.MRecovery:
		// nodeNum := m.Params[0]
		nonce := m.Params[1]
		vnum := atomic.LoadInt64(&node.viewNumber)
		opNum := int64(-1)
		comNum := int64(-1)
		log := "!"
		if node.Number == node.leaderNumber {
			opNum = atomic.LoadInt64(&node.opNumber)
			comNum = atomic.LoadInt64(&node.commitNumber)
			log, _ = node.FetchJournal()
		}
		respMsg := dkvs.MakeMessage(dkvs.MRecoveryResponse, node.Number, nonce, vnum, opNum, comNum, log)
		req.respBin <- dkvs.PrintMessage(respMsg)

	case dkvs.MPrepare:
		node.lastCommit = time.Now()

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

		responseMsg := dkvs.PrintMessage(dkvs.MakeMessage(dkvs.MPrepareOk, vNum, opNum + 1, node.Number))

		if onum - 1 == opNum {
			node.CommitTail(cnum)
			opNum = atomic.AddInt64(&node.opNumber, 1)
			node.WriteToJournal(&RequestPack{preparedMsg, opNum, nil})
			req.respBin <- responseMsg
		} else {
			go func(){
				node.RecoveryNode()
				req.respBin <- responseMsg
			}()
		}

	case dkvs.MCommit:
		node.lastCommit = time.Now()

		vnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		cnum, _ := strconv.ParseInt(m.Params[1], 10, 64)

		vNum := atomic.LoadInt64(&node.viewNumber)
		if vNum != vnum {
			req.respBin <- "ERR Wrong view"
			break
		}

		opNum := atomic.LoadInt64(&node.opNumber)
		comNum := atomic.LoadInt64(&node.commitNumber)

		if comNum < cnum {
			if cnum == opNum {
				node.CommitTail(cnum)
				req.respBin <- "OK"
			} else {
				go func(){
					node.RecoveryNode()
					req.respBin <- "OK"
				}()
			}
		}
		req.respBin <- "OK"

	case dkvs.MStartViewChange:
		fallthrough
	case dkvs.MDoViewChange:
		vnum, _ := strconv.ParseInt(m.Params[0], 10, 64)

		req.respBin <- "OK"

		if vnum > node.viewNumber {
			go node.InitViewChange()
		}

	default:
		req.respBin <- "ERR Invalid status"
	}
}

func (node *ServerNode) ProcessServiceRecovering(req *RequestPack) {
	node.WaitForStatus(Normal)
	node.ProcessServiceNormal(req)
}

func (node *ServerNode) ProcessServiceViewChange(req *RequestPack) {
	m := req.msg

	switch m.Type {
	case dkvs.MStartViewChange:
		vnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		nnum, _ := strconv.Atoi(m.Params[1])

		logOk.Printf("VIEW CHANGE VOTE: vnum = %d, vNumber = %d", vnum, node.viewNumber)

		if vnum == node.viewNumber {
			node.startViewChangeMsgs[nnum] = m
			f := (len(node.siblings) + 1) / 2

			logOk.Printf("CHECK CONSENSUS THRESHOLD: f = %d, %d calls received", f, len(node.startViewChangeMsgs))

			if len(node.startViewChangeMsgs) == f {
				logOk.Println("ISSUE NEXT LEADER SELECTION")
				go node.DoViewChange()
			}
		}

		req.respBin <- "OK"

	case dkvs.MDoViewChange:
		// ovnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		vnum, _ := strconv.ParseInt(m.Params[1], 10, 64)
		// opnum, _ := strconv.ParseInt(m.Params[2], 10, 64)
		// cnum, _ := strconv.ParseInt(m.Params[3], 10, 64)
		nnum, _ := strconv.Atoi(m.Params[4])


		if vnum == node.viewNumber {
			node.doViewChangeMsgs[nnum] = m
			f := (len(node.siblings) + 1) / 2

			logOk.Printf("RECEIVED do_view_change FROM NODE %d (view %d, %d votes already)", nnum, vnum, len(node.doViewChangeMsgs))

			if len(node.doViewChangeMsgs) == f + 1 {
				logOk.Println("ISSUE NEW VIEW STARTING")
				go node.StartNewView()
			}

			req.respBin <- "OK"
		} else {
			req.respBin <- "ERR Invalid view number"
		}

	case dkvs.MStartView:

		logOk.Println("START VIEW RECEIVED")

		vnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		nnum, _ := strconv.Atoi(m.Params[1])
		cnum, _ := strconv.ParseInt(m.Params[2], 10, 64)

		node.RefreshJounalState(m.Params[3], cnum, vnum)
		node.leaderNumber = nnum
		node.changeStatus(Normal)
		node.lastCommit = time.Now()
		node.resetCommitTimer(-1)

		req.respBin <- "OK"

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

func (node *ServerNode) ChangeStatus(newStatus NodeStatus) bool {
	node.statusLock.Lock()
	res := node.changeStatus(newStatus)
	node.statusLock.Unlock()
	return res
}

func (node *ServerNode) changeStatus(newStatus NodeStatus) bool {
	prevStatus := node.status
	node.status = newStatus

	if prevStatus != newStatus {
		select {
		case node.statusChan <- true:
		default:
		}
		return true
	}
	return false
}

func (node *ServerNode) GetStatus() NodeStatus {
	node.statusLock.RLock()
	res := node.status
	node.statusLock.RUnlock()
	return res
}

func (node *ServerNode) GetLeaderNumber() int {
	node.statusLock.RLock()
	node.WaitForStatus(Normal)
	lnum := node.leaderNumber
	node.statusLock.RUnlock()
	return lnum
}

func (node *ServerNode) SetLeaderNumber(newLeader int) {
	node.statusLock.RLock()
	node.WaitForStatus(ViewChange, Recovering)
	node.leaderNumber = newLeader
	node.statusLock.RUnlock()
}

func (node *ServerNode) PrepareRequestToAll(req *RequestPack) {
	comNum := atomic.LoadInt64(&node.commitNumber)
	viewNum := atomic.LoadInt64(&node.viewNumber)

	msg := dkvs.MakeMessage(dkvs.MPrepare, viewNum, req.opNum, comNum, req.msg)

	node.resetCommitTimer(-1)

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
	node.resetCommitTimer(-1)
}

func (node *ServerNode) CommitToAll() {
	comNum := atomic.LoadInt64(&node.commitNumber)
	viewNum := atomic.LoadInt64(&node.viewNumber)

	msg := dkvs.MakeMessage(dkvs.MCommit, viewNum, comNum)

	logOk.Println("COMMIT BROADCAST")
	node.BroadcastMessage(msg)
}

func (node *ServerNode) RecoveryNode() {
	node.ChangeStatus(Recovering)

	nonce := dkvs.RandSeq(10)
	msg := dkvs.MakeMessage(dkvs.MRecovery, strconv.Itoa(node.Number), nonce)

	log := ""
	vNum := int64(-1)
	comNum := int64(-1)
	leaderNum := -1

	retries := 0
	OuterLoop: for {
		logOk.Printf("Recovery broadcast retry #%d", retries)

		agg := node.BroadcastMessage(msg)
		f := (len(node.siblings) + 1) / 2

		countReady := 0
		leaderNum = -1

		for i := 0; i < len(node.siblings); i++ {
			resp := <-agg
			nnum, theirNonce, vnum, _, comnum, logOut, err := dkvs.ParseRecoveryResponse(resp)
			if err == nil && nonce == theirNonce {
				countReady++
				if comnum != -1 {
					vNum = vnum
					comNum = comnum
					log = logOut
					leaderNum = nnum
				}
				if leaderNum != -1 && countReady > f {
					break OuterLoop
				}
			}
		}
		retries++
	}

	node.RefreshJounalState(log, comNum, vNum)
	node.SetLeaderNumber(leaderNum)
	logOk.Println("Recovery completed")

	node.ChangeStatus(Normal)
}

func (node *ServerNode) InitViewChange() {
	node.statusLock.Lock()

	// We don't need to start view change again, if we are already busy with that
	if node.status == ViewChange {
		node.statusLock.Unlock()
		return
	}

	node.changeStatus(ViewChange)

	atomic.StoreInt64(&node.normalViewNumber, atomic.LoadInt64(&node.viewNumber))
	newVnum := atomic.AddInt64(&node.viewNumber, 1)

	node.startViewChangeMsgs = make(map[int]*dkvs.Message)
	node.doViewChangeMsgs = make(map[int]*dkvs.Message)

	startViewChangeMsg := dkvs.MakeMessage(dkvs.MStartViewChange, newVnum, node.Number)

	logOk.Println("START VIEW CHANGE BROADCAST")
	node.BroadcastMessage(startViewChangeMsg)

	node.statusLock.Unlock()
}

func (node *ServerNode) DoViewChange() {
	node.statusLock.Lock()

	oldVnum := atomic.LoadInt64(&node.normalViewNumber)
	newVnum := atomic.LoadInt64(&node.viewNumber)
	opNum := atomic.LoadInt64(&node.opNumber)
	commNum := atomic.LoadInt64(&node.commitNumber)
	log, _ := node.FetchJournal()

	doViewChangeMsg := dkvs.MakeMessage(dkvs.MDoViewChange, oldVnum, newVnum, opNum, commNum, node.Number, log)

	nextLeader := node.leaderNumber
	req := &RequestPack{
		msg: doViewChangeMsg,
		opNum: -1,
		respBin: make(chan string, 1),
	}

	for {
		nextLeader = (nextLeader + 1) % (len(node.siblings) + 2)
		if nextLeader == 0 {
			nextLeader = 1
		}

		logOk.Printf("DO VIEW CHANGE TO %d", nextLeader)

		if nextLeader == node.Number {
			node.serviceMsgs <- req
		} else {
			node.siblings[nextLeader].SendRequest(req)
		}

		node.statusLock.Unlock()
		ans := <-req.respBin
		node.statusLock.Lock()

		if ans == "OK" {
			break
		}
	}

	node.leaderNumber = nextLeader
	node.statusLock.Unlock()
}

func (node *ServerNode) StartNewView() {
	node.statusLock.Lock()

	maxCommit := int64(0)
	maxOldView := int64(0)
	maxOpNum := int64(0)
	var maxMsg *dkvs.Message = nil

	if len(node.doViewChangeMsgs) == 0 {
		panic("COME ON DAT's IMPOSSIBLE!")
	}

	for _, m := range node.doViewChangeMsgs {
		ovnum, _ := strconv.ParseInt(m.Params[0], 10, 64)
		opnum, _ := strconv.ParseInt(m.Params[2], 10, 64)
		cnum, _ := strconv.ParseInt(m.Params[3], 10, 64)

		if maxCommit < cnum {
			maxCommit = cnum
		}

		if maxOldView < ovnum || (maxOldView == ovnum && maxOpNum < opnum) {
			maxOldView = ovnum
			maxOpNum = opnum
			maxMsg = m
		}
	}

	viewNum, _ := strconv.ParseInt(maxMsg.Params[1], 10, 64)

	node.RefreshJounalState(maxMsg.Params[5], maxCommit, viewNum)
	startMsg := dkvs.MakeMessage(dkvs.MStartView, viewNum, node.Number, maxCommit, maxMsg.Params[5])

	node.BroadcastMessage(startMsg)
	node.changeStatus(Normal)
	node.lastCommit = time.Now()
	node.resetCommitTimer(-1)

	node.statusLock.Unlock()
}
