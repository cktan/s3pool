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
package mop

/* master op */

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"s3pool/mon"
	"strings"
)

func readLines(conn net.Conn) ([]string, error) {
	scanner := bufio.NewScanner(conn)
	var ret []string
	for scanner.Scan() {
		ret = append(ret, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func invoke(req string) (ret []string, err error) {
	addr := mon.GetMasterAddr()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println("s3meta: cannot connect to", addr)
		addr = mon.GetMasterAddr2()
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			log.Println("s3meta: cannot connect to standby", addr)
			return
		}
	}
	defer conn.Close()

	// write the request to conn
	fmt.Fprintf(conn, "%s\n", req)

	// read reply
	line, err := readLines(conn)
	if err != nil {
		log.Println("err:", err)
		return
	}
	if len(line) == 0 {
		err = errors.New("invalid reply")
		return
	}
	status, ret := line[0], line[1:]
	if status != "OK" {
		err = errors.New(strings.Join(ret, "\n"))
		return
	}
	ret = line[1:]
	return
}

func mkstr(s string) string {
	return "\"" + s + "\""
}

func GetETag(bucket, key string) (string, error) {
	s := fmt.Sprintf("[%s, %s, %s]", mkstr("_GETETAG"), mkstr(bucket), mkstr(key))
	res, err := invoke(s)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0], nil
}

func SetETag(bucket, key, etag string) error {
	s := fmt.Sprintf("[%s, %s, %s, %s]", mkstr("_SETETAG"), mkstr(bucket), mkstr(key), mkstr(etag))
	if _, err := invoke(s); err != nil {
		return err
	}
	return nil
}

func RemoveKey(bucket, key string) error {
	s := fmt.Sprintf("[%s, %s, %s]", mkstr("_REMOVEKEY"), mkstr(bucket), mkstr(key))
	if _, err := invoke(s); err != nil {
		return err
	}
	return nil
}

func ListPrefix(bucket, prefix string) ([]string, error) {
	s := fmt.Sprintf("[%s, %s, %s]", mkstr("_LISTPREFIX"), mkstr(bucket), mkstr(prefix))
	res, err := invoke(s)
	if err != nil {
		return []string{}, err
	}
	return res, nil
}

func ListDrop(bucket string) error {
	s := fmt.Sprintf("[%s, %s]", mkstr("_LISTDROP"), mkstr(bucket))
	if _, err := invoke(s); err != nil {
		return err
	}
	return nil
}
