package main

import (
	"os"
	"sync"
	"io"
	"dkvs"
	"fmt"
	"bufio"
	"strings"
	"strconv"
	"io/ioutil"
	"bytes"
	"sync/atomic"
)

type (
	NodeJournal struct {
		sync.RWMutex
		filename  string
		file     *os.File
	}
)

func MakeJournal(filename string) (*NodeJournal, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0775)
	if err != nil {
		return nil, err
	}

	return &NodeJournal{
		filename: filename,
		file: file,
	}, nil
}

func (node *ServerNode) WriteToJournal(req *RequestPack) {
	node.Journal.Lock()
	fmt.Fprintf(node.Journal.file, "%d %s\n", req.opNum, dkvs.PrintMessage(req.msg))
	node.Journal.Unlock()
}

func (node *ServerNode) RestoreFromJournal(commNum int64) error {
	node.Journal.RLock()
	defer node.Journal.RUnlock()

	logOk.Println(`Restoring from journal`)

	// Clean the hashmap
	node.Database = make(map[string]string)

	br := bufio.NewReader(node.Journal.file)
	opCount := 0

	line, _, err := br.ReadLine()
	for ; err == nil; line, _, err = br.ReadLine() {
		record := string(line)
		parts := strings.SplitN(record, " ", 2)
		if len(parts) < 2 {
			logErr.Println(`Invalid journal entry encountered`)
			return err
		}

		opNum, err := strconv.Atoi(parts[0])
		if err != nil {
			logErr.Println(`Invalid journal entry encountered`)
			return err
		}

		atomic.StoreInt64(&node.opNumber, int64(opNum))

		msg, err := dkvs.ParseMessage(parts[1])
		if err != nil {
			logErr.Println(`Invalid journal entry encountered`)
			return err
		}

		if commNum < 0 || commNum >= int64(opNum) {
			node.PerformAction(msg)
			opCount++
		}
	}

	if commNum < 0 {
		commNum = atomic.LoadInt64(&node.opNumber)
	}
	atomic.StoreInt64(&node.commitNumber, commNum)

	if err != io.EOF {
		return err
	}
	logOk.Printf("%d operations restored from journal\n", opCount)
	return nil
}

func (node *ServerNode) CommitTail(comNum int64) error {
	node.Journal.RLock()
	defer node.Journal.RUnlock()

	oldComNum := atomic.LoadInt64(&node.commitNumber)

	contents, err := ioutil.ReadFile(node.Journal.filename)
	if err != nil {
		return err
	}

	br := bufio.NewReader(bytes.NewBuffer(contents))
	line, _, err := br.ReadLine()
	for ; err == nil; line, _, err = br.ReadLine() {
		record := string(line)
		parts := strings.SplitN(record, " ", 2)
		if len(parts) < 2 {
			return err
		}

		onum, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}

		if int64(onum) > oldComNum {
			if int64(onum) > comNum {
				break
			}

			msg, err := dkvs.ParseMessage(parts[1])
			if err != nil {
				return err
			}

			node.PerformAction(msg)
		}
	}

	atomic.StoreInt64(&node.commitNumber, comNum)

	logOk.Printf("TAIL commited: commit number %d", comNum)
	return nil
}

func (node *ServerNode) SubstituteJournal(src io.Reader) error {
	node.Journal.Lock()
	defer node.Journal.Unlock()

	err := node.Journal.file.Close()
	if err != nil {
		return err
	}

	node.Journal.file, err = os.OpenFile(node.Journal.filename, os.O_WRONLY, 0775)
	if err != nil {
		return err
	}

	defer func() {
		node.Journal.file.Close()
		node.Journal.file, err = os.OpenFile(node.Journal.filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0775)
	}()

	_, err = io.Copy(node.Journal.file, src)
	if err != nil {
		return err
	}

	return nil
}

func (node *ServerNode) FetchJournal() (string, error) {
	node.Journal.RLock()
	defer node.Journal.RUnlock()

	contents, err := ioutil.ReadFile(node.Journal.filename)
	if err != nil {
		return "", err
	}

	return strings.Replace(string(contents), "\n", ";", -1), nil
}
