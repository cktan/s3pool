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
package op

import (
	"errors"
	"log"
	"s3pool/conf"
	"s3pool/mop"
)

/*
  1. List all objects in bucket
  2. save the key[] and etag[] to catalog
*/
func Refresh(args []string) (string, error) {
	conf.CountRefresh++

	if len(args) != 1 {
		return "", errors.New("expects 1 argument for REFRESH")
	}
	bucket := args[0]
	// DO NOT checkCatalog here. We will update it!

	log.Println(" ... invalidate s3meta bucket", bucket)
	if err := mop.ListDrop(bucket); err != nil {
		return "", err
	}
	return "\n", nil
}
