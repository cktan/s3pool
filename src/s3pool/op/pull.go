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

func Pull(args []string) (string, error) {
	if len(args) != 2 && len(args) != 3 {
		return "", errors.New("Expected 2 or 3 arguments for PULL")
	}
	bucket, key := args[0], args[1]

	// retrieve the object
	path, err := s3GetObject(bucket, key)
	if err != nil {
		return "", err
	}

	if len(args) == 3 {
		nextKey := args[2]
		// prefetch ... fire and forget
		go s3GetObject(bucket, nextKey)
	}

	return path + "\n", nil
}
