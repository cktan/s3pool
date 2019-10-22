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
package op

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
)

//
// aws s3 cp src dst
//
func s3cp(src, dst string) error {
	cmd := exec.Command("aws", "s3", "cp", src, dst)
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("aws cp failed -- %v", err)
	}
	return nil
}

func Push(args []string) (string, error) {
	if len(args) != 3 {
		return "", errors.New("Expected 3 arguments for PUSH")
	}
	bucket, key, path := args[0], args[1], args[2]

	url := fmt.Sprintf("s3://%s/%s", url.PathEscape(bucket), url.PathEscape(key))
	if err := s3cp(path, url); err != nil {
		return "", err
	}

	return "\n", nil
}
