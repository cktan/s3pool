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
)

var bm = newBucketMap()
var trace = true

func KnownBuckets() []string {
	return bm.Keys()
}

func Find(bucket, key string) (etag string) {
	// returns etag == "" if not found
	if trace {
		defer func() {
			log.Println("Catalog.Find", bucket, key, " -- ", etag)
		}()
	}
	km := bm.Get(bucket)
	if km == nil {
		return
	}

	etag = km.SearchExact(key)
	return
}

func Upsert(bucket, key, etag string) {
	if trace {
		log.Println("Catalog.Update", bucket, key, etag)
	}

	km := bm.Get(bucket)
	if km == nil {
		return
	}

	km.Upsert(key, etag)
}

func Delete(bucket, key string) {
	if trace {
		log.Println("Catalog.Delete", bucket, key)
	}

	km := bm.Get(bucket)
	if km == nil {
		return
	}

	km.Delete(key)
}

func Scan(bucket string, prefix string, filter func(string) bool) (key []string) {
	if trace {
		log.Println("Catalog.Scan", bucket)
	}

	key = make([]string, 0, 100)
	km := bm.Get(bucket)
	if km == nil {
		return
	}

	item := km.SearchPrefix(prefix)
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
	return
}

func Store(bucket string, key, etag []string, err error) {
	if trace {
		log.Println("Catalog.Store", bucket, "#keys", len(key))
	}
	km, err := NewKeyMap(key, etag, err)
	if err != nil {
		return
	}
	bm.Put(bucket, km)
}

func Exists(bucket string) (ok bool, err error) {
	if trace {
		log.Println("Catalog.Exists", bucket)
	}
	km := bm.Get(bucket)
	if km == nil {
		return
	}
	ok = true
	err = km.err
	return
}
