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
package s3meta

import (
	"s3pool/strlock"
)

func Drop(bucket string) {
	invalidate(bucket)
}

func Get(bucket, key string) (etag string) {
	store := getStore(bucket)
	etag = store.get(key)
	return
}

func Set(bucket, key, etag string) {
	store := getStore(bucket)
	store.set(key, etag)
}

func Remove(bucket, key string) {
	store := getStore(bucket)
	store.set(key, "")
}

func List(bucket string, prefix string) (key []string, err error) {
	lock := strlock.Lock("s3meta/" + bucket + "/" + prefix)
	key, err = list(bucket, prefix)
	strlock.Unlock(lock)
	return
}


