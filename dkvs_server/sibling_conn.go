package main

import (
	"net"
	"dkvs"
	"sync"
	"sync/atomic"
	"time"
	"io"
)

type (
	SiblingConn struct {
		sync.Mutex
		toConn    net.Conn
		fromConn  net.Conn
		reqsChan  chan *RequestPack

		addr     *dkvs.ServerAddr
		timeout   time.Duration

		timer    *time.Timer

		isShut    int32
	}
)

func MakeSiblingConn(addr *dkvs.ServerAddr, timeout time.Duration) {
	return &SiblingConn{
		toConn: nil,
		fromConn: nil,
		reqsChan: make(chan *RequestPack, 4096),
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
			<-sc.timer.C
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
			return err
		}
		sc.timer = time.NewTimer(0)
		if !sc.timer.Stop() {
			<-sc.timer
		}
	}
	return nil
}

func (sc *SiblingConn) Run() {
	for {
		isShut := atomic.LoadInt32(&sc.isShut)
		if isShut {
			break
		}

		err := sc.Connect()
		if err != nil {
			time.Sleep(sc.timeout) // Wait before attempt to reconnect
		} else {

			for {
				req := nil

				select {
				case req = <-sc.reqsChan:
					if _, err := c.toConn.Write(dkvs.PrintMessage(req.msg) + "\n"); err != nil {
						logErr.Println("Error while sending request to " + sc.toConn.RemoteAddr().String(), err)
						continue
					}
				case <-sc.timer.C:
					if _, err := c.toConn.Write("ping\n"); err != nil {
						logErr.Println("Error while sending ping to " + sc.toConn.RemoteAddr().String(), err)
						continue
					}
				}

				sc.Lock()
				if sc.toConn == nil {
					sc.Unlock()
					continue
				}

				remoteAddr := sc.toConn.RemoteAddr().String()
				sc.toConn.SetReadDeadline(time.Now().Add(sc.timeout))
				msg, _, err := sc.toConn.ReadLine()
				sc.Unlock()

				if err == io.EOF {
					logErr.Println("Sibling " + remoteAddr + " closed connection prematurely")
					sc.CloseTo()
					break
				} else if netop, ok := err.(*net.OpError); ok && netop.Timeout() {
					logErr.Println("Read timeout " + remoteAddr)
					sc.CloseBoth()
					break
				} else if err != nil {
					logErr.Println("Error while getting response from sibling " + remoteAddr, err)
					if req != nil {
						req.respBin <- "ERR " + err.Error()
					}
				}

				if req != nil {
					req.respBin <- msg
				}
			}
		}
	}
}
