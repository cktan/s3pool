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
	"s3pool/conf"
	"s3pool/s3"
	"s3pool/s3meta"
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

	if cat.UseS3Meta {
		s3meta.Invalidate(bucket)
		return "\n", nil
	}

	

	numItems := 0
	/*
		log.Println("REFRESH start on", bucket)
		startTime := time.Now()
		defer func() {
			endTime := time.Now()
			elapsed := int(endTime.Sub(startTime) / time.Millisecond)
			log.Printf("REFRESH fin on %s, %d items, elapsed %d ms\n", bucket, numItems, elapsed)
		}()
	*/

	key := make([]string, 0, 100)
	etag := make([]string, 0, 100)
	save := func(k, t string) {
		if k[len(k)-1] == '/' {
			// skip DIR
			return
		}
		key = append(key, k)
		etag = append(etag, t)
		numItems++
	}

	err := s3.ListObjects(bucket, "", save)
	cat.Store(bucket, key, etag, err)

	if err != nil {
		return "", err
	}

	return "\n", nil
}
