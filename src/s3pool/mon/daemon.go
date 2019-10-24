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
package mon

import (
	"syscall"
	"log"
	"os"
	"os/exec"
)






// This will be called twice.
// First time with '-s' for setsid
// Second time with '-n' for do-not-daemonize
func makeArgv(flag string) ([]string, error) {
	argv := []string{}
	argv = append(argv, flag)
	argv = append(argv, os.Args[1:]...)
	for i := range(argv) {
		if argv[i] == "-D" {
			cwd, err := os.Getwd()
			if err != nil {
				return []string{}, err
			}
			argv[i+1] = cwd
		}
	}
	if flag == "-n" {
		for i := range(argv) {
			if argv[i] == "-s" {
				argv = append(argv[:i], argv[i+1:]...)
				break
			}
		}
	}
	return argv, nil
}

func redirectFd() {
	syscall.Close(0)
	syscall.Close(1)
	syscall.Close(2)

	syscall.Open("/dev/null", syscall.O_RDWR, 0666)
	syscall.Dup(0)
	syscall.Dup(0)
}




// Daemonize will be called twice in two different processes
// First time with setsid == False
//    -> we want to fork and let the child run -s
// Second time with setsid == True
//    -> we will setsid and then let the child run with -n 
func Daemonize(setsid bool) {
	
	var flag string
	if (setsid) {
		// set the sid
		syscall.Setsid()

		syscall.Umask(0)
		redirectFd()
		// next time run with no-daemonize 
		flag = "-n"
	} else {
		// next time run with setsid
		flag = "-s"
	}

	execpath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	argv, err := makeArgv(flag)
	if err != nil {
		log.Fatal("daemonize: ", err)
	}

	cmd := exec.Command(execpath, argv...)
	err = cmd.Start()
	if err != nil {
		log.Fatal("daemonize: ", err)
	}

	os.Exit(0)
}
