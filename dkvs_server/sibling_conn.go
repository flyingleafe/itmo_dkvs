package main

import (
	"net"
	"dkvs"
	"sync"
	"sync/atomic"
	"time"
	"io"
	"strconv"
	// "errors"
	"fmt"
	// "syscall"
	"bufio"
)

type (
	SiblingConn struct {
		sync.RWMutex
		toConn    net.Conn
		fromConn  net.Conn
		reqsChan  chan *RequestPack

		number    int
		addr      dkvs.ServerAddr
		timeout   time.Duration

		timer    *time.Timer

		isShut    int32
	}
)

func MakeSiblingConn(num int, addr dkvs.ServerAddr, timeout time.Duration) *SiblingConn {
	return &SiblingConn{
		toConn: nil,
		fromConn: nil,
		reqsChan: make(chan *RequestPack, 4096),
		number: num,
		addr: addr,
		timeout: timeout,
		isShut: 0,
	}
}

func (sc *SiblingConn) CloseFrom() {
	sc.Lock()
	if sc.fromConn != nil {
		sc.fromConn.Close()
		sc.fromConn = nil
	}
	sc.Unlock()
}

func (sc *SiblingConn) CloseTo() {
	sc.Lock()

	if sc.toConn != nil {
		sc.toConn.Close()
		sc.toConn = nil
	}
	sc.Unlock()
}

func (sc *SiblingConn) CloseBoth() {
	sc.CloseTo()
	sc.CloseFrom()
}

func (sc *SiblingConn) Shutdown() {
	atomic.CompareAndSwapInt32(&sc.isShut, 0, 1)
	close(sc.reqsChan)
	sc.CloseBoth()
}

func (sc *SiblingConn) Connect() error {
	sc.Lock()
	defer sc.Unlock()

	if sc.toConn == nil {
		var err error
		if sc.toConn, err = net.DialTimeout(`tcp4`, fmt.Sprintf(`%s:%d`, sc.addr.Host, sc.addr.Port), sc.timeout); err != nil {
			logErr.Printf("Error while connecting to sibling %d: %s", sc.number, err)
			sc.toConn = nil
			return err
		}
	}
	return nil
}

func (sc *SiblingConn) Run(ownNum int) {
	for {
		isShut := atomic.LoadInt32(&sc.isShut)
		if isShut == 1 {
			break
		}

		err := sc.Connect()
		if err != nil {
			time.Sleep(sc.timeout) // Wait before attempt to reconnect
		} else {

			if err := sc.InitConnection(ownNum); err != nil {
				logErr.Println("Sibling handshake failed:", err)
				sc.CloseTo()
				continue
			}

			br := bufio.NewReader(sc.toConn)

			for {
				req := <-sc.reqsChan

				if _, err := sc.toConn.Write([]byte(dkvs.PrintMessage(req.msg) + "\n")); err != nil {
					logErr.Println("Error while sending request to " + sc.toConn.RemoteAddr().String(), err)
					sc.CloseTo()
					req.respBin <- "ERR " + err.Error()
					break
				}
				logOk.Printf("SEND REQ to node %d: %s", sc.number, dkvs.PrintMessage(req.msg))

				sc.RLock()
				if sc.toConn == nil {
					sc.RUnlock()
					continue
				}

				remoteAddr := sc.toConn.RemoteAddr().String()
				sc.toConn.SetReadDeadline(time.Now().Add(sc.timeout))
				msg, _, err := br.ReadLine()
				sc.RUnlock()

				if err != nil {
					if err == io.EOF {
						logErr.Println("Sibling " + remoteAddr + " closed connection prematurely")
					} else if netop, ok := err.(*net.OpError); ok && netop.Timeout() {
						logErr.Println("Read timeout " + remoteAddr)
					} else if err != nil {
						logErr.Println("Error while getting response from sibling " + remoteAddr, err)
					}

					sc.CloseBoth()
					req.respBin <- "ERR " + err.Error()
					break
				}

				logOk.Printf("RESP RECV from node %d: %s", sc.number, string(msg))
				req.respBin <- string(msg)
			}
		}
	}
}

func (sc *SiblingConn) SendRequest(req *RequestPack) bool {
	sc.RLock()
	defer sc.RUnlock()

	if sc.toConn != nil {
		sc.reqsChan <- req
		return true
	}
	req.respBin <- "ERR Connection to sibling is not established"
	return false
}

// Synchronous handshake
func (sc *SiblingConn) InitConnection(ownNum int) error {
	sc.RLock()
	defer sc.RUnlock()

	msg := dkvs.MakeMessage(dkvs.MNode, strconv.Itoa(ownNum))
	_, err := sc.toConn.Write([]byte(dkvs.PrintMessage(msg) + "\n"))
	if err != nil {
		return err
	}

	sc.toConn.SetReadDeadline(time.Now().Add(sc.timeout))

	br := bufio.NewReader(sc.toConn)
	ans, _, err := br.ReadLine()
	if err != nil {
		return err
	}

	if string(ans) != "ACCEPTED" {
		return fmt.Errorf("Unexpected handshake answer: %s", ans)
	}

	return nil
}

func MakeSiblings(ownNumber int, config *dkvs.ServerConfig) (map[int]*SiblingConn) {
	res := make(map[int]*SiblingConn)
	for i, addr := range config.Nodes {
		if i != ownNumber {
			res[i] = MakeSiblingConn(i, addr, config.Timeout)
		}
	}
	return res
}

func (node *ServerNode) TryConnectSiblings() int {
	count := 0
	for _, sib := range node.siblings {
		err := sib.Connect()
		if err == nil {
			count++
		}
	}
	return count
}

func (node *ServerNode) RunSiblings() {
	for _, sib := range node.siblings {
		go sib.Run(node.Number)
	}
}

func (node *ServerNode) BroadcastMessage(msg *dkvs.Message) chan string {
	aggregator := make(chan string, len(node.siblings))
	for _, sib := range node.siblings {

		req := &RequestPack{
			msg: msg,
			opNum: -1,
			respBin: make(chan string, 1),
		}

		go func(rq *RequestPack) {
			s := <-rq.respBin
			aggregator <- s
		}(req)

		sib.SendRequest(req)
	}

	return aggregator
}
