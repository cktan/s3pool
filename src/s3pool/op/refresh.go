/*
 * S3pool - S3 cache on local disk
 * Copyright (c) 2019 CK Tan
 * cktanx@gmail.com
 *
 *
 * S3Pool can be used for free under the GNU General Public License
 * version 3 (where anything released into public must be open source) or
 * under a commercial license if such has been acquired (send email to
 * cktanx@gmail.com). The commercial license does not cover derived or
 * ported versions created by third parties under GPL.
 */
package op

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

func s3ListObjects(bucket string) error {
	cmd := exec.Command("aws", "s3api", "list-objects",
		"--bucket", bucket,
		"--query", "Contents[].{Key: Key}")
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	list := strings.Split(string(outbuf.Bytes()), "\n")

	fp, err := ioutil.TempFile("tmp", "s3l_")
	if err != nil {
		return fmt.Errorf("Cannot create temp file -- %v", err)
	}
	defer fp.Close()
	defer os.Remove(fp.Name())

	for _, s := range list {
		nv := strings.SplitN(strings.Trim(s, " \t"), ":", 2)
		if len(nv) == 1 {
			continue
		}
		for i := range nv {
			nv[i] = strings.Trim(nv[i], " \t\"")
		}
		name, value := nv[0], nv[1]
		if name != "Key" {
			continue
		}
		if len(value) >= 1 && value[len(value)-1] == '/' {
			continue
		}
		fp.WriteString(value)
		fp.WriteString("\n")
	}
	fp.Close()

	if err = moveFile(fp.Name(), fmt.Sprintf("data/%s/__list__", bucket)); err != nil {
		return err
	}

	return nil
}

func Refresh(args []string) (reply string, err error) {
	if len(args) != 1 {
		err = errors.New("expects 1 argument for REFRESH")
		return
	}
	bucket := args[0]

	err = s3ListObjects(bucket)
	if err != nil {
		return
	}

	return
}
