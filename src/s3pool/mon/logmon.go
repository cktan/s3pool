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
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

var logprefix string
var logfname string
var logfp *os.File

func redirectStderr(fname string) {
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to redirect stderr to file: %v", err)
	}
	err = syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stderr to file: %v", err)
	}
}

func checklog() {
	tm := time.Now()
	fname := fmt.Sprintf("%s-%04d%02d%02d.log", logprefix, tm.Year(), tm.Month(), tm.Day())
	if logfname != fname {
		nextfp, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot create log file %s - %v", fname, err)
		}

		log.SetOutput(nextfp)
		if logfp != nil {
			logfp.Close()
		}
		logfp = nextfp
		logfname = fname

		redirectStderr(fmt.Sprintf("%s-stderr.log", logprefix))
	}
}

func SetLogPrefix(s string) {
	logprefix = s
	checklog()
}

func Logmon() {
	for {
		checklog()
		time.Sleep(60 * time.Second)
	}
}
