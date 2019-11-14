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
 *  cktanx@gmail.com.
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
	"time"
)

func dummy() {
	/*
		byt, err := ioutil.ReadFile("t.json")
		if err != nil {
			log.Fatal(err)
		}
		collection := &ListCollection{Contents: make([]ListRecord, 0, 100)}
		json.Unmarshal(byt, collection)
		fmt.Println(collection)
	*/
}

func checkawscli() bool {
	cmd := exec.Command("aws", "--version")
	err := cmd.Run()
	return err == nil
}

func checkdirs() {
	// create the log, tmp and data directories
	mkdirall := func(dir string) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error mkdir(%s): %v\n", dir, err)
			os.Exit(1)
		}
	}

	mkdirall("log")
	mkdirall("tmp")
	mkdirall("data")
}

// Callback function for each new request
func serve(c *tcp_server.Client, request string) {

	sendReply := func(status, reply string, elapsed int) {
		// send network reply
		c.Send(status)
		c.Send("\n")
		c.Send(reply)

		// log the request/response
		errstr := ""
		if status == "ERROR" {
			// for errors, we also want to log the error str
			errstr = "..." + reply + "\n"
		}
		log.Printf("%s [%s, %d bytes, %d ms]\n%s",
			request, status, len(reply), elapsed, errstr)
	}

	startTime := time.Now()
	var reply string
	var err error

	// when the function finishes, send a reply and log the request
	defer func() {
		endTime := time.Now()
		elapsed := int(endTime.Sub(startTime) / time.Millisecond)
		status := "OK"
		if err != nil {
			status = "ERROR"
			reply = err.Error()
		}
		sendReply(status, reply, elapsed)
	}()

	// extract cmd, args from the request
	var args []string
	err = json.Unmarshal([]byte(request), &args)
	if err != nil {
		err = errors.New("Invalid JSON in request")
		return
	}

	var cmd string
	var cmdargs []string
	if len(args) >= 1 {
		cmd = args[0]
		cmdargs = args[1:]
	}

	// dispatch cmd
	switch cmd {
	case "PULL":
		reply, err = op.Pull(cmdargs)
	case "GLOB":
		reply, err = op.Glob(cmdargs)
		if err == nil {
			mon.NotifyBucketmon(cmdargs[0])
		}
	case "GLOBX":
		reply, err = op.Globx(cmdargs)
		if err == nil {
			mon.NotifyBucketmon(cmdargs[0])
		}
	case "REFRESH":
		reply, err = op.Refresh(cmdargs)
	case "PUSH":
		reply, err = op.Push(cmdargs)
	case "SET":
		reply, err = op.Set(cmdargs)
	default:
		err = errors.New("Bad command: " + cmd)
	}
}

type progArgs struct {
	port       *int
	dir        *string
	noDaemon   *bool
	daemonPrep *bool
	pidFile    *string
}

func parseArgs() (p progArgs, err error) {
	p.port = flag.Int("p", 0, "port number")
	p.dir = flag.String("D", "", "home directory")
	p.noDaemon = flag.Bool("n", false, "do not run as daemon")
	p.daemonPrep = flag.Bool("daemonprep", false, "internal, do not use")
	p.pidFile = flag.String("pidfile", "", "store pid in this path")

	flag.Parse()

	if len(flag.Args()) != 0 {
		err = errors.New("Extra arguments")
		return
	}

	if !(0 < *p.port && *p.port <= 65535) {
		err = errors.New("Missing or invalid port number")
		return
	}
	if "" == *p.dir {
		err = errors.New("Missing or invalid home directory path")
		return
	}

	return
}

func exit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	log.Println(msg)
	os.Exit(1)
}

func boot() {
	// for debug only
	f, err := os.OpenFile("/tmp/text.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		exit(err.Error())
	}
	log.SetOutput(f)
	log.Println(os.Args)
}

func main() {
	// boot()
	dummy()

	// make sure that the aws cli is installed
	if !checkawscli() {
		exit("Cannot launch 'aws' command. Please install aws cli.")
	}

	// check flags
	p, err := parseArgs()
	if err != nil {
		exit(err.Error())
	}

	// get into the home dir
	if err := os.Chdir(*p.dir); err != nil {
		exit(err.Error())
	}

	// create the necessary directories
	checkdirs()

	// setup log file
	mon.SetLogPrefix("log/s3pool")
	log.Println("Starting:", os.Args)

	// setup and check pid file
	if *p.pidFile == "" {
		s := fmt.Sprintf("s3pool.%d.pid", *p.port)
		p.pidFile = &s
	}
	pidfile.SetFname(*p.pidFile)
	if pidfile.IsRunning() {
		exit("Error: another s3pool is running")
	}

	// Run as daemon?
	if !(*p.noDaemon) {
		// prepare the argv.
		// We need replace -D homedir with -D . because we have cd into homedir
		argv := append([]string(nil), os.Args[1:]...)
		for i := range argv {
			if argv[i] == "-D" {
				argv[i+1] = "."
			}
		}
		mon.Daemonize(*p.daemonPrep, argv)
	}

	// write pid to pidfile
	pidfile.Write()

	// start log
	mon.Logmon()

	// start the disk space monitor
	mon.Diskmon()

	// start pidfile monitor
	mon.Pidmon()

	// start Bucket monitor
	mon.Bucketmon()

	// start server
	server, err := tcp_server.New(fmt.Sprintf("0.0.0.0:%d", *p.port), serve)
	if err != nil {
		log.Fatal("Listen() failed - %v", err)
	}

	// keep serving
	err = server.Loop()
	if err != nil {
		log.Fatal("Loop() failed - %v", err)
	}
}
