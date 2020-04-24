/**
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019-2020 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktanx@gmail.com.
 */
package pidfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var pidFname string = "pidfile.pid"

func SetFname(s string) {
	pidFname = s
}

func Read() int {
	dat, err := ioutil.ReadFile(pidFname)
	if err != nil {
		return 0
	}
	pidstr := strings.Trim(string(dat), " \t\r\n")
	ret, err := strconv.Atoi(pidstr)
	if err != nil {
		return 0
	}
	return ret
}

func Write() {
	pid := os.Getpid()
	byt := []byte(fmt.Sprintf("%d\n", pid))
	ioutil.WriteFile(pidFname, byt, 0644)
}

func PsCommand() string {
	pid := Read()
	if pid == 0 {
		return ""
	}

	cmd := exec.Command("ps", "-h", "-p", strconv.Itoa(pid), "-o", "comm")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.Trim(string(out), " \t\r\n")
}

func IsRunning() bool {
	// my exec name
	a, _ := os.Executable()
	a = filepath.Base(a)

	// get name of process with pidfile
	s := PsCommand()
	s = strings.Trim(s, " \t\r\n")
	s = filepath.Base(s)

	return s == a
}
