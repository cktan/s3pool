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
	"sync/atomic"
	"strings"
)


var master, standby string
var masterIsDown int32
var port int

func Get() string {
}


func SetMasterIsDown(flag bool) {
	var val int32
	if flag {
		val = 1
	}
	atomic.StoreInt32(&masterIsDown, val)
}


func IsMasterDown() bool {
	val := atomic.LoadInt32(&masterIsDown)
	return val != 0
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



func netPing(host string) bool {
	reply, _ := remoteCall(host, []string{"PING"})
	return len(reply) > 3 && reply[:3] == "OK\n" 
}

func netGlob(host, bucket, pattern string) (reply string, err error) {
	reply, err = remoteCall(host, []string{"GLOBX", bucket, pattern})
	return
}

func Mastermon(master_, standby_ string, port_ int) {
	port = port_
	master, standby = master_, standby_
	for {
		time.Sleep(60 * time.Second)
	}
}
