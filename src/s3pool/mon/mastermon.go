package mon

import (
	"bufio"
	"fmt"
	"net"
	"s3pool/conf"
	"strconv"
	"time"
)

var Master = "localhost"

func ping(host string) bool {
	// send a message to host and see if we get a (good) reply
	conn, err := net.Dial("tcp", host+":"+strconv.Itoa(conf.Port))
	if err != nil {
		return false
	}
	defer conn.Close()

	fmt.Fprintf(conn, "[\"%s\"]\n", "STATUS")
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false
	}
	if status != "OK" {
		return false
	}
	return true
}

func decideMaster() {
	// if master is configured as localhost, and since we are localhost and we are alive, just set master to self
	if conf.Master == "localhost" {
		Master = conf.Master
		return
	}

	// if we can ping conf.Master, set to conf.Master
	if ping(conf.Master) {
		Master = conf.Master
		return
	}

	// if there is a standby and we can ping it, set to standby
	if conf.Standby != "" && ping(conf.Standby) {
		Master = conf.Standby
		return
	}

	// can't ping master or standby! just to localhost
	Master = "localhost"
}

func GetMasterAddr() string {
	return Master + ":" + strconv.Itoa(conf.Port)
}

func GetMasterAddr2() string {
	decideMaster()
	return Master + ":" + strconv.Itoa(conf.Port)
}

func mastermonLoop() {
	for {
		select {
		case <-time.After(60 * time.Second):
			decideMaster()
		}
	}
}

func Mastermon() {
	go mastermonLoop()
}
