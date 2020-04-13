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
package s3

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"s3pool/conf"
	"strings"
)

type ListRecord struct {
	Key  string
	Etag string
}

type ListCollection struct {
	Contents []ListRecord
}

func ListObjects(bucket string, prefix string, notify func(key, etag string)) error {
	if conf.Verbose(1) {
		log.Println("s3 list-objects", bucket)
	}

	var err error

	// invoke s3api to list objects
	var cmd *exec.Cmd
	if prefix == "" {
		cmd = exec.Command("aws", "s3api", "list-objects-v2",
			"--bucket", bucket,
			"--query", "Contents[].{Key: Key, ETag: ETag}")
	} else {
		cmd = exec.Command("aws", "s3api", "list-objects-v2",
			"--bucket", bucket,
			"--prefix", prefix,
			"--query", "Contents[].{Key: Key, ETag: ETag}")
	}
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %s", string(errbuf.Bytes()))
	}
	defer cmd.Wait()

	// read stdout of cmd
	scanner := bufio.NewScanner(pipe)
	var key string
	var etag string
	for scanner.Scan() {
		s := scanner.Text()
		// Parse s of the form
		//   {
		//       "Key" : "key value"
		//       "ETag" : "\"etag\""
		//   }
		// Note: the order of Key and ETag is random, but one must follow another.
		nv := strings.SplitN(s, ":", 2)
		if len(nv) != 2 {
			// reset if not a key value
			key, etag = "", ""
			continue
		}

		// extract key value
		name := strings.Trim(nv[0], " \t\",")
		value := strings.Trim(nv[1], " \t\",\\")
		switch name {
		case "Key":
			key = value
		case "ETag":
			etag = value
		}

		// if both filled, we have a record
		if key != "" && etag != "" {
			notify(key, etag)
			key, etag = "", ""
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	// clean up
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	return nil
}
