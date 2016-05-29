package main

import (
	"flag"
	"strconv"
	"os/signal"
	"syscall"
	"os"
	"log"
	"fmt"
	"dkvs"
)

const (
	PropertiesFile = `dkvs.properties`
	JournalPrefix = `dkvs_`
)

var (
	params struct {
		ConfigPath string
		LogPrefix  string
		Verbose    bool
		NodeNum    int
	}
	logOk  *log.Logger
	logErr *log.Logger
)

func init() {
	flag.StringVar(&params.ConfigPath, `c`, PropertiesFile, `Path to config file`)
	flag.StringVar(&params.LogPrefix, `l`, JournalPrefix, `Prefix for journal files`)
	flag.BoolVar(&params.Verbose, `v`, false, `Set verbose output`)

	logOk = log.New(os.Stdout, `LOG `, log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	logErr = log.New(os.Stderr, `ERR `, log.LstdFlags|log.Lmicroseconds|log.Lshortfile)

	flag.Parse()
}

func main() {
	if len(flag.Args()) == 0 {
		fmt.Fprintln(os.Stderr, "Node number is required")
		flag.Usage()
		return
	}

	num, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		logErr.Println("Invalid node number:", flag.Arg(0))
		return
	}

	params.NodeNum = num
	configFile, err := os.Open(params.ConfigPath)
	if err != nil {
		logErr.Println("Cannot open config file:", err)
		return
	}

	config, err := dkvs.ParseConfig(configFile)
	if err != nil {
		logErr.Println("Error while parsing config:", err)
		return
	}

	journalFile, err := os.OpenFile(params.LogPrefix + strconv.Itoa(params.NodeNum) + `.log`, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0775)
	if err != nil {
		logErr.Println("Cannot open journal file:", err)
		return
	}

	node := MakeNode(params.NodeNum, journalFile, config)
	if err := node.Start(); err != nil {
		logErr.Println(err)
		node.Stop()
		return
	}

	defer node.Stop()

	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, syscall.SIGINT)
	signal.Notify(sigs, syscall.SIGTERM)

	logOk.Println("Node started")
	<-sigs
}
