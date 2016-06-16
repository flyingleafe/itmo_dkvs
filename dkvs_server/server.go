package main

import (
	"time"
	"io"
	"os"
	"fmt"
	"dkvs"
	"net"
	"bufio"
	"sync/atomic"
	"sync"
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

// Initialize data structure for server node
func MakeNode(num int, journalFilename sting, config *dkvs.ServerConfig) (*ServerNode, error) {
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
		status:       Recovering,
		statusChan:   make(chan bool, 1),
		statusLock:   sync.RWMutex{},
		leaderNumber: 0,
		viewNumber:   0,
		opNumber:     0,
		commitNumber: 0,
		clientTable:  make(map[string]ClientInfo),
	}, nil
}

// Start server: read journal and listen to connections
func (node *ServerNode) Start() error {
	if err := node.RestoreFromJournal(); err != nil {
		return err
	}

	myAddr := node.Config.Nodes[node.Number]

	ln, err := net.Listen(`tcp4`, fmt.Sprintf(`%s:%d`, myAddr.Host, myAddr.Port))
	if err != nil {
		return err
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

// Interact with one of connections
func (node *ServerNode) Interact(conn net.Conn) {
	logOk.Println("Starting interaction with", conn.RemoteAddr().String())

	conn.SetReadDeadline(time.Time{})
	br := bufio.NewReader(conn)

	needStop := atomic.LoadInt32(&node.needStop)
	for ; needStop != 1; needStop = atomic.LoadInt32(&node.needStop) {
		msg, _, err := br.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			logErr.Println(`Error while receiving message:`, err)
			continue
		}

		mesg, err := dkvs.ParseMessage(string(msg))
		if err != nil {
			logErr.Println(`Invalid message:`, err)
			conn.Write([]byte("ERR " + err.Error() + "\n"))
			continue
		}

		logOk.Printf("QUERY (%s): %s\n", conn.RemoteAddr().String(), msg)

		curOpNum := int64(-1)
		if dkvs.IsChangingOp(mesg) {
			curOpNum = atomic.AddInt64(&node.opNumber, 1)
		}

		pack := &RequestPack{
			msg:     mesg,
			opNum:   curOpNum,
			respBin: make(chan string, 1),
		}

		node.incomingReqs <- pack
		ans := <-pack.respBin

		_, err = conn.Write([]byte(ans + "\n"))
		if err != nil {
			logErr.Println(`Error while answering query:`, err)
			continue
		}
	}

	logOk.Println("Closing connection with", conn.RemoteAddr().String())
	conn.Close()
}

// Process all the messages
func (node *ServerNode) ProcessMsgs() {
	for req := range node.incomingReqs {
		node.statusLock.RLock()

		// Make sure that current status is Normal before processing client request
		for {
			if node.status == Normal {
				break
			}
			node.statusLock.RUnlock()
			<-node.statusChan
			node.statusLock.RLock()
		}

		if !dkvs.IsChangingOp(req.msg) {
			req.respBin <- node.PerformAction(req.msg)
		} else {
			if node.Number == node.leaderNumber {

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
