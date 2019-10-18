/*
 * S3pool - S3 cache on local disk
 * Copyright (c) 2019 CK Tan
 * cktanx@gmail.com
 *
 *
 * S3Pool can be used for free under the GNU General Public License
 * version 3 (where anything released into public must be open source) or
 * under a commercial license if such has been acquired (send email to
 * cktanx@gmail.com). The commercial license does not cover derived or
 * ported versions created by third parties under GPL.
 */

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"s3pool/op"
	"s3pool/tcp_server"
	"time"
)

func watchlog() {

	curfname := ""
	var curfp *os.File
	for {
		tm := time.Now()
		fname := fmt.Sprintf("log/s3pool-%04d%02d%02d.log", tm.Year(), tm.Month(), tm.Day())
		if curfname == fname {
			time.Sleep(60 * time.Second)
			continue
		}

		nextfp, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot create log file %s - %v", fname, err)
		}

		log.SetOutput(nextfp)

		if curfp != nil {
			curfp.Close()
		}
		curfp = nextfp
		curfname = fname
	}
}

var Port int
var HomeDir string
var notifyBucket chan string

func mkdirall(dir string) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error mkdir(%s): %v\n", dir, err)
		os.Exit(1)
	}
}

func checkdirs() {
	// create the log, tmp and data directories
	mkdirall("log")
	mkdirall("tmp")
	mkdirall("data")
}

func sendReply(c *tcp_server.Client, status, reply, request string, elapsed int) {
	c.Send(status)
	c.Send("\n")
	c.Send(reply)
	c.Send("\n")
	log.Printf("%s [%s, %d bytes, %d ms]\n", request, status, len(reply), elapsed/1000)
}

// Callback function for each new request
func serve(c *tcp_server.Client, request string) {
	startTime := time.Now()
	var reply string
	var err error

	// when the function finishes, send a reply and log the request
	defer func() {
		endTime := time.Now()
		elapsed := int(endTime.Sub(startTime) / 1000)
		if err != nil {
			sendReply(c, "ERROR", err.Error(), request, elapsed)
		} else {
			sendReply(c, "OK", reply, request, elapsed)
		}
	}()

	// extract cmd, args from the request
	var args []string
	err = json.Unmarshal([]byte(request), &args)
	if err != nil {
		err = errors.New("Invalid JSON in request")
		return
	}

	var cmd string
	if len(args) >= 1 {
		cmd = args[0]
	}

	// dispatch cmd
	switch cmd {
	case "PULL":
		reply, err = op.Pull(args[1:])
		if err != nil {
			notifyBucket <- args[1]
		}
	case "REFRESH":
		reply, err = op.Refresh(args[1:])
		if err != nil {
			notifyBucket <- args[1]
		}
	case "PUSH":
		reply, err = op.Push(args[1:])
		if err != nil {
			notifyBucket <- args[1]
		}
	default:
		err = errors.New("Bad command: " + cmd)
	}
}

func parseArgs() {
	portPtr := flag.Int("p", 0, "port number")
	dirPtr := flag.String("D", "", "home directory")
	flag.Parse()

	if len(flag.Args()) != 0 {
		fmt.Fprintf(os.Stderr, "Extra arguments\n")
		os.Exit(1)
	}

	Port = *portPtr
	HomeDir = *dirPtr
	if !(0 < Port && Port <= 65535) {
		fmt.Fprintf(os.Stderr, "Missing or invalid port number\n")
		os.Exit(1)
	}
	if "" == HomeDir {
		fmt.Fprintf(os.Stderr, "Missing or invalid home directory name\n")
		os.Exit(1)
	}
}

func main() {

	parseArgs()

	if err := os.Chdir(HomeDir); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// create the necessary directories
	checkdirs()

	// start log and sleep a bit for watchlog to init first log file
	go watchlog()
	time.Sleep(1 * time.Second)

	// start the disk space monitor
	go diskmon()

	// start the listmon
	notifyBucket = make(chan string, 10)
	go listmon(notifyBucket)

	// start server
	server := tcp_server.New(fmt.Sprintf("localhost:%d", Port), serve)

	// keep serving
	if err := server.Loop(); err != nil {
		log.Fatal("Listen() failed - %v", err)
	}
}
