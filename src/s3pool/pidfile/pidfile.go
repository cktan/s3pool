package pidfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
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

func PsOutput() string {
	pid := Read()
	if pid == 0 {
		return ""
	}

	cmd := exec.Command("ps", "-h", "-p", strconv.Itoa(pid))
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	return string(out)
}


