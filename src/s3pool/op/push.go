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
package op

import (
	"errors"
	"fmt"
	"s3pool/s3"
	"s3pool/conf"
)

func Push(args []string) (string, error) {
	conf.CountPush++
	if len(args) != 3 {
		return "", errors.New("Expected 3 arguments for PUSH")
	}
	bucket, key, path := args[0], args[1], args[2]
	if len(path) > 0 && path[0] != '/' {
		return "", fmt.Errorf("Path parameter must be an absolute path")
	}
	if err := checkCatalog(bucket); err != nil {
		return "", err
	}

	err := s3.PutObject(bucket, key, path)
	if err != nil {
		return "", err
	}

	return "\n", nil
}
