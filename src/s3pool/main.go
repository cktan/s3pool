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

var Port int
var HomeDir string
var NoDaemon bool
var DaemonPrep bool
var PidFile string

var BucketmonChannel chan<- string

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
	if len(args) >= 1 {
		cmd = args[0]
	}

	// dispatch cmd
	switch cmd {
	case "PULL":
		reply, err = op.Pull(args[1:])
		if err != nil {
			BucketmonChannel <- args[1]
		}
	case "GLOB":
		reply, err = op.Glob(args[1:])
		if err != nil {
			BucketmonChannel <- args[1]
		}
	case "REFRESH":
		reply, err = op.Refresh(args[1:])
		if err != nil {
			BucketmonChannel <- args[1]
		}
	case "PUSH":
		reply, err = op.Push(args[1:])
		if err != nil {
			BucketmonChannel <- args[1]
		}
	default:
		err = errors.New("Bad command: " + cmd)
	}
}

func parseArgs() error {
	portPtr := flag.Int("p", 0, "port number")
	dirPtr := flag.String("D", "", "home directory")
	noDaemonPtr := flag.Bool("n", false, "do not run as daemon")
	daemonPrepPtr := flag.Bool("daemonprep", false, "internal, do not use")
	pidFilePtr := flag.String("pidfile", "", "store pid in this path")

	flag.Parse()

	if len(flag.Args()) != 0 {
		return errors.New("Extra arguments")
	}

	Port = *portPtr
	HomeDir = *dirPtr
	NoDaemon = *noDaemonPtr
	PidFile = *pidFilePtr
	DaemonPrep = *daemonPrepPtr
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

func boot() {
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
	if err := parseArgs(); err != nil {
		exit(err.Error())
	}

	// get into the home dir
	if err := os.Chdir(HomeDir); err != nil {
		exit(err.Error())
	}

	// create the necessary directories
	checkdirs()

	// setup log file
	mon.SetLogPrefix("log/s3pool")
	log.Println("Starting:", os.Args)

	// setup and check pid file
	if PidFile == "" {
		PidFile = fmt.Sprintf("s3pool.%d.pid", Port)
	}
	pidfile.SetFname(PidFile)
	if pidfile.IsRunning() {
		exit("Error: another s3pool is running")
	}

	// Run as daemon?
	if !NoDaemon {
		// prepare the argv.
		// We need replace -D homedir with -D . because we have cd into homedir
		argv := append([]string(nil), os.Args[1:]...)
		for i := range argv {
			if argv[i] == "-D" {
				argv[i+1] = "."
			}
		}
		mon.Daemonize(DaemonPrep, argv)
	}

	// write pid to pidfile
	pidfile.Write()

	// start log
	go mon.Logmon()

	// start the disk space monitor
	go mon.Diskmon()

	// start pidfile monitor
	go mon.Pidmon()

	// start Bucket monitor
	BucketmonChannel = mon.Bucketmon()

	// start server
	server, err := tcp_server.New(fmt.Sprintf("0.0.0.0:%d", Port), serve)
	if err != nil {
		log.Fatal("Listen() failed - %v", err)
	}

	// keep serving
	err = server.Loop()
	if err != nil {
		log.Fatal("Loop() failed - %v", err)
	}
}
