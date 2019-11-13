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
package mon

import (
	"log"
	"os"
	"os/exec"
	"syscall"
)

func redirectFd() {
	syscall.Close(0)
	syscall.Open("/dev/null", syscall.O_RDWR, 0666)

	syscall.Close(1)
	syscall.Dup(0)

	if false {
		// do not close stderr; Golang panic() output always goes there
		syscall.Close(2)
		syscall.Dup(0)
	}
}

// Daemonize will be called twice in two different processes
// First time with daemonprep == false
//    -> we want to fork and let the child run --daemonprep, and EXIT
// Second time with daemonprep == true
//    -> we will setsid, umask, redirectfd, and RETURN
func Daemonize(daemonprep bool, argv []string) {

	if daemonprep {
		// set the sid
		syscall.Setsid()

		syscall.Umask(0)
		redirectFd()

		return

	}

	execpath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	// prepend the flag --daemonprep
	argv = append([]string{"--daemonprep"}, argv...)

	// exec it
	cmd := exec.Command(execpath, argv...)
	err = cmd.Start()
	if err != nil {
		log.Fatal("daemonize: ", err)
	}

	os.Exit(0)
}
