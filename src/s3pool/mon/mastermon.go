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
	"time"
	"net"
	"fmt"
	"bytes"
	"io"
	"strings"
	"sync"
)


type mastermonCb struct {
	sync.Mutex
	master, standby string
	currMaster string
}

var cb = &mastermonCb{}
var port int

func Get(exclude string) string {
	cb.Lock()
	defer cb.Unlock()
	
	if exclude == cb.currMaster {
		cb.currMaster = cb.standby
		if exclude == cb.currMaster {
			cb.currMaster = cb.master
		}
	}
	return cb.currMaster
}

func Mastermon(master, standby string, port_ int) error {
	if master == "" {
		return fmt.Errorf("mastermon: missing master param")
	}
	if standby == "" {
		standby = master
	}
	cb.Lock()
	cb.master = master
	cb.standby = standby
	cb.currMaster = master
	cb.Unlock()

	port = port_
	for {
		time.Sleep(60 * time.Second)
	}
}



func NetPing(host string) bool {
	reply, _ := remoteCall(host, []string{"PING"})
	return len(reply) > 3 && reply[:3] == "OK\n" 
}

func NetGlob(host, bucket, pattern string) (reply string, err error) {
	reply, err = remoteCall(host, []string{"GLOBX", bucket, pattern})
	return
}

func remoteCall(host string, args []string) (reply string, err error) {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range(args) {
		sep := ","
		if i == 0 {
			sep = "["
		}
		fmt.Fprintf(&sb, "%s\"%s\"", sep, args[i])
	}
	sb.WriteString("]");
	request := sb.String()

	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return 
	}
	defer func() { conn.Close() }()

	fmt.Fprintln(conn, request)
	
	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)
	if err != nil {
		return
	}
	reply = string(buf.Bytes())
	
	return 
}

