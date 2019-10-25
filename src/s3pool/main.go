/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktan@gmail.com.
 */
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"s3pool/mon"
	"s3pool/op"
	"s3pool/pidfile"
	"s3pool/tcp_server"
	"strings"
	"time"
)

func checkawscli() bool {
	cmd := exec.Command("aws", "--version")
	err := cmd.Run()
	return err == nil
}

var Port int
var HomeDir string
var NoDaemon bool
var SetSid bool

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
	// log the request/response
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
	case "GLOB":
		reply, err = op.Glob(args[1:])
	case "REFRESH":
		reply, err = op.Refresh(args[1:])
	case "PUSH":
		reply, err = op.Push(args[1:])
	default:
		err = errors.New("Bad command: " + cmd)
	}
}

func parseArgs() error {
	portPtr := flag.Int("p", 0, "port number")
	dirPtr := flag.String("D", "", "home directory")
	noDaemonPtr := flag.Bool("n", false, "do not run as daemon")
	setSidPtr := flag.Bool("s", false, "internal... do not use")

	flag.Parse()

	if len(flag.Args()) != 0 {
		return errors.New("Extra arguments")
	}

	Port = *portPtr
	HomeDir = *dirPtr
	NoDaemon = *noDaemonPtr
	SetSid = *setSidPtr
	if !(0 < Port && Port <= 65535) {
		return errors.New("Missing or invalid port number")
	}
	if "" == HomeDir {
		return errors.New("Missing or invalid home directory path")
	}

	return nil
}

func exit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	log.Println(msg)
	os.Exit(1)
}

func main() {
	// make sure that the aws cli is installed
	if !checkawscli() {
		exit("Cannot launch 'aws' command. Please install aws cli.")
	}

	// check flags
	if err := parseArgs(); err != nil {
		exit(err.Error())
	}

	// get into the home dir
	if err := os.Chdir(HomeDir); err != nil {
		exit(err.Error())
	}

	// setup log file
	mon.SetLogPrefix("log/s3pool")
	log.Println(os.Args)

	// setup and check pid file
	pidfile.SetFname(fmt.Sprintf("s3pool.%d.pid", Port))
	if strings.Contains(pidfile.PsOutput(), "s3pool") {
		exit("Error: another s3pool is running")
	}

	// create the necessary directories
	checkdirs()

	// start log
	go mon.Logmon()

	// Run as daemon?
	if !NoDaemon {
		mon.Daemonize(SetSid)
	}

	// write pid to pidfile
	pidfile.Write()

	// start the disk space monitor
	go mon.Diskmon()

	// start server, keep serving
	server := tcp_server.New(fmt.Sprintf("localhost:%d", Port), serve)
	if err := server.Loop(); err != nil {
		log.Fatal("Listen() failed - %v", err)
	}
}
