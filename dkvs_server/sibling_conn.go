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
		timer: nil,
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
	if sc.timer != nil {
		if !sc.timer.Stop() {
			select {
			case <-sc.timer.C:
			default:
			}
		}
	}

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
	sc.timer = nil
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
		sc.timer = time.NewTimer(0)
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

			if !sc.timer.Stop() {
				<-sc.timer.C
			}
			sc.timer.Reset(sc.timeout - 10*time.Millisecond)

			br := bufio.NewReader(sc.toConn)

			var req *RequestPack
			for {
				req = nil

				select {
				case req = <-sc.reqsChan:
					if _, err := sc.toConn.Write([]byte(dkvs.PrintMessage(req.msg) + "\n")); err != nil {
						logErr.Println("Error while sending request to " + sc.toConn.RemoteAddr().String(), err)
						sc.CloseBoth()
						req.respBin <- "ERR " + err.Error()
						break
					}
					logOk.Printf("SEND REQ to node %d: %s", sc.number, dkvs.PrintMessage(req.msg))
				case <-sc.timer.C:
					if _, err := sc.toConn.Write([]byte("ping\n")); err != nil {
						logErr.Println("Error while sending ping to " + sc.toConn.RemoteAddr().String(), err)
						sc.CloseBoth()
						break
					}
					logOk.Printf("SEND PING to node %d", sc.number)
				}

				sc.RLock()
				if sc.toConn == nil {
					sc.RUnlock()
					continue
				}

				remoteAddr := sc.toConn.RemoteAddr().String()
				sc.toConn.SetReadDeadline(time.Now().Add(sc.timeout))
				msg, _, err := br.ReadLine()
				sc.RUnlock()

				if err == io.EOF {
					logErr.Println("Sibling " + remoteAddr + " closed connection prematurely")
					sc.CloseBoth()

					if req != nil {
						req.respBin <- "ERR " + err.Error()
					}
					break
				} else if netop, ok := err.(*net.OpError); ok && netop.Timeout() {
					logErr.Println("Read timeout " + remoteAddr)
					sc.CloseBoth()

					if req != nil {
						req.respBin <- "ERR " + err.Error()
					}
					break
				} else if err != nil {
					logErr.Println("Error while getting response from sibling " + remoteAddr, err)
					sc.CloseBoth()

					if req != nil {
						req.respBin <- "ERR " + err.Error()
					}
					break
				}

				logOk.Printf("RESP RECV from node %d: %s", sc.number, string(msg))
				if req != nil {
					req.respBin <- string(msg)
				}

				if !sc.timer.Stop() && req != nil {
					<-sc.timer.C
				}
				sc.timer.Reset(sc.timeout - 10*time.Millisecond)
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

		logOk.Println("COME ON MUTHAFUCKER")
		go func(rq *RequestPack) {
			logOk.Println("HEEEEY BITCH")
			s := <-rq.respBin
			logOk.Println("GOTCHAAAA")
			aggregator <- s
		}(req)

		sib.SendRequest(req)
	}
	return aggregator
}
