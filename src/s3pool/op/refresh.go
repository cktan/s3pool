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
	"s3pool/cat"
)

/*
  1. List all objects in bucket
  2. save the key[] and etag[] to catalog
*/
func Refresh(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("expects 1 argument for REFRESH")
	}
	bucket := args[0]
	// DO NOT checkCatalog here. We will update it!

	key := make([]string, 0, 100)
	etag := make([]string, 0, 100)
	save := func(k, t string) {
		if k[len(k)-1] == '/' {
			// skip DIR
			return
		}
		key = append(key, k)
		etag = append(etag, t)
	}

	if err := s3ListObjects(bucket, save); err != nil {
		return "", err
	}

	cat.Store(bucket, key, etag)

	return "\n", nil
}
