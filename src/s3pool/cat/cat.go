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
package cat

import (
	"log"
	"s3pool/s3meta"
)

var bm = newBucketMap()
var trace = true
var UseS3Meta = true

func KnownBuckets() []string {
	var ret []string
	if UseS3Meta {
		ret = s3meta.KnownBuckets()
	} else {
		ret = bm.keys()
	}
	return ret
}

func Find(bucket, key string) (etag string) {
	// returns etag == "" if not found
	if trace {
		defer func() {
			log.Println("Catalog.Find", bucket, key, " -- ", etag)
		}()
	}
	if UseS3Meta {
		etag = s3meta.SearchExact(bucket, key)
	} else {
		km := bm.get(bucket)
		if km != nil {
			etag = km.searchExact(key)
		}
	}
	return
}

func Upsert(bucket, key, etag string) {
	if trace {
		log.Println("Catalog.Update", bucket, key, etag)
	}
	if UseS3Meta {
		s3meta.SetETag(bucket, key, etag)
	} else {
		km := bm.get(bucket)
		if km != nil {
			km.upsert(key, etag)
		}
	}
}

func Delete(bucket, key string) {
	if trace {
		log.Println("Catalog.Delete", bucket, key)
	}
	if UseS3Meta {
		s3meta.Delete(bucket, key)
	} else {
		km := bm.get(bucket)
		if km != nil {
			km.delete(key)
		}
	}
}

func Scan(bucket string, prefix string, filter func(string) bool) (key []string) {
	if trace {
		log.Println("Catalog.Scan", bucket)
	}

	if UseS3Meta {

		xkey, xetag, err := s3meta.List(bucket, prefix)
		if err != nil {
			log.Println("Error:", err)
			key = make([]string, 0)
		} else {
			key = make([]string, 0, len(xkey))
			for i := 0; i < len(xkey); i++ {
				if xetag[i] != "" {
					key = append(key, xkey[i])
				}
			}
		}

	} else {

		key = make([]string, 0, 100)
		km := bm.get(bucket)
		if km != nil {
			item := km.searchPrefix(prefix)
			if trace {
				log.Println("Catalog.Scan bucket", bucket, "prefix", prefix, "found", len(item), "items")
			}
			for _, v := range item {
				if v.ETag == "" {
					continue
				}
				if filter(v.Key) {
					key = append(key, v.Key)
				}
			}
		}
	}

	return
}

func Store(bucket string, key, etag []string, err error) {

	if trace {
		log.Println("Catalog.Store", bucket, "#keys", len(key))
	}

	if UseS3Meta {
		panic("do not call this when UseS3Meta")

	} else {

		km, err := newKeyMap(key, etag, err)
		if err != nil {
			return
		}
		bm.put(bucket, km)
	}
}

func Exists(bucket string) (ok bool, err error) {
	if UseS3Meta {
		ok = true
		return

	} else {
		km := bm.get(bucket)
		if km != nil {
			ok = true
			err = km.err
		}
	}
	return
}
