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
	"errors"
	"io/ioutil"
	"os"
)

func Refresh(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("expects 1 argument for REFRESH")
	}
	bucket := args[0]

	// Create a temp file to store the list
	file, err := ioutil.TempFile("tmp", "s3f_")
	if err != nil {
		return "", err
	}
	defer file.Close()
	defer os.Remove(file.Name())

	if err = s3ListObjects(bucket, file); err != nil {
		return "", err
	}

	// Save the list file to S3
	if err = s3PutObject(bucket, "__list__", file.Name()); err != nil {
		return "", err
	}

	return "\n", nil
}
