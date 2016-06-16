package main

import (
	"os"
	"sync"
	"io"
	"dkvs"
)

type (
	NodeJournal struct {
		sync.RWMutex
		filename  string
		file     *os.File
	}
)

func MakeJournal(filename string) (*Journal, error) {
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
	fmt.Fprintln(j.file, dkvs.PrintMessage(req.msg))
	node.Jounal.Unlock()
}

func (node *ServerNode) RestoreFromJournal() error {
	node.Journal.RLock()
	defer node.Journal.RUnlock()

	logOk.Println(`Restoring from journal`)

	br := bufio.NewReader(node.Journal.file)
	opNum := 0

	line, _, err := br.ReadLine()
	for ; err == nil; line, _, err = br.ReadLine() {
		msg, err := dkvs.ParseMessage(string(line))
		if err != nil {
			logErr.Println(`Invalid journal entry encountered`)
			return err
		}

		node.PerformAction(msg)
		opNum++
	}

	if err != io.EOF {
		return err
	}
	logOk.Printf("%d operations restored from journal\n", opNum)
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

	_, err := io.Copy(node.Journal.file, src)
	if err != nil {
		return err
	}

	return nil
}
