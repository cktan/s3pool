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
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)


func s3ListObjects_old(bucket string) error {

	// write to a temp file and then move it into bucket/__list__
	fp, err := ioutil.TempFile("tmp", "s3l_")
	if err != nil {
		return fmt.Errorf("Cannot create temp file -- %v", err)
	}
	defer fp.Close()
	defer os.Remove(fp.Name())

	// invoke s3api to list objects
	cmd := exec.Command("aws", "s3api", "list-objects-v2",
		"--bucket", bucket,
		"--query", "Contents[].{Key: Key}")

	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}
	defer cmd.Wait()

	// read stdout of cmd 
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		s := scanner.Text()
		// Parse s of the form 
		//       "Key" : "key value"
		nv := strings.SplitN(s, ":", 2)
		if len(nv) != 2 {
			continue
		}
		name := strings.Trim(nv[0], " \t\"")
		if name != "Key" {
			continue
		}
		value := strings.Trim(nv[1], " \t\"")
		// ignore empty value or value that looks like a DIR (ending with / )
		if len(value) == 0 || value[len(value)-1] == '/' {
			continue
		}
		fp.WriteString(value)
		fp.WriteString("\n")
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	// done writing to temp file
	fp.Close()

	// clean up
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	// move the temp file to data/bucket/__list__
	if err = moveFile(fp.Name(), fmt.Sprintf("data/%s/__list__", bucket)); err != nil {
		return err
	}

	return nil
}



func Refresh(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("expects 1 argument for REFRESH")
	}
	bucket := args[0]

	file, err := ioutil.TempFile("tmp", "s3f_")
	if err != nil {
		return "", err
	}
	defer file.Close()
	defer os.Remove(file.Name())
	
	if err = s3ListObjects(bucket, file); err != nil {
		return "", err
	}

	if err = s3PutObject(bucket, "__list__", file.Name()); err != nil {
		return "", err
	}

	return "\n", nil
}
