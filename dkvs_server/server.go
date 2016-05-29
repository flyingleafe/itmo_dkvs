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
)

type (
	RequestPack struct {
		msg     *dkvs.Message
		respBin  chan string
	}

	ServerNode struct {
		Number   int
		Database map[string]string
		Journal  *os.File
		Config   *dkvs.ServerConfig

		listener       net.Listener
		needStop       int32
		incomingReqs   chan *RequestPack
	}
)

// Initialize data structure for server node
func MakeNode(num int, journalFile *os.File, config *dkvs.ServerConfig) *ServerNode {
	return &ServerNode{
		Number:       num,
		Database:     make(map[string]string),
		Journal:      journalFile,
		Config:       config,
		needStop:     0,
		incomingReqs: make(chan *RequestPack, 4096),
	}
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

		pack := &RequestPack{
			msg:     mesg,
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
		req.respBin <- node.ProcessMsg(req.msg, true)
	}
}

// Process one particular message
func (node *ServerNode) ProcessMsg(m *dkvs.Message, writeJournal bool) string {
	var ans string

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
		ans = "STORED"
	case dkvs.MDelete:
		key := m.Params[0]
		if _, has := node.Database[key]; has {
			delete(node.Database, key)
			ans = "DELETED"
		} else {
			ans = "NOT_FOUND"
		}
	}

	if writeJournal {
		fmt.Fprintln(node.Journal, dkvs.PrintMessage(m))
	}
	return ans
}

func (node *ServerNode) RestoreFromJournal() error {
	logOk.Println(`Restoring from journal`)

	br := bufio.NewReader(node.Journal)
	opNum := 0

	line, _, err := br.ReadLine()
	for ; err == nil; line, _, err = br.ReadLine() {
		msg, err := dkvs.ParseMessage(string(line))
		if err != nil {
			logErr.Println(`Invalid journal entry encountered`)
			return err
		}

		node.ProcessMsg(msg, false)
		opNum++
	}

	if err != io.EOF {
		return err
	}
	logOk.Printf("%d operations restored from journal\n", opNum)
	return nil
}
