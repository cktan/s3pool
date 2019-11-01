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
)

func Push(args []string) (string, error) {
	if len(args) != 3 {
		return "", errors.New("Expected 3 arguments for PUSH")
	}
	bucket, key, path := args[0], args[1], args[2]
	if err := checkCatalog(bucket); err != nil {
		return "", err
	}

	err := s3PutObject(bucket, key, path)
	if err != nil {
		return "", err
	}

	return "\n", nil
}
