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
var trace bool

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
	km, ok := bm.Get(bucket)
	if ok {
		km.RLock()
		etag, ok = (*km.Map)[key]
		km.RUnlock()
	}
	return
}

func Update(bucket, key, etag string) {
	if trace {
		log.Println("Catalog.Update", bucket, key, etag)
	}
	km, ok := bm.Get(bucket)
	if ok {
		km.Lock()
		(*km.Map)[key] = etag
		km.Unlock()
	}
}

func Delete(bucket, key string) {
	if trace {
		log.Println("Catalog.Delete", bucket, key)
	}
	km, ok := bm.Get(bucket)
	if ok {
		km.Lock()
		delete(*km.Map, key)
		km.Unlock()
	}
}

func Scan(bucket string, filter func(string) bool) (key []string) {
	if trace {
		log.Println("Catalog.Scan", bucket)
	}
	key = make([]string, 0, 100)
	km, ok := bm.Get(bucket)
	if !ok {
		return
	}

	km.RLock()
	for kkk := range *km.Map {
		if filter(kkk) {
			key = append(key, kkk)
		}
	}
	km.RUnlock()
	return
}

func Store(bucket string, key, etag []string, err error) {
	if trace {
		log.Println("Catalog.Store", bucket)
	}
	dict := make(map[string]string)
	for i := range key {
		dict[key[i]] = etag[i]
	}
	bm.Put(bucket, &dict, err)
}

func Exists(bucket string) (ok bool, err error) {
	var km *KeyMap
	km, ok = bm.Get(bucket)
	if ok {
		km.Lock()
		err = km.err
		km.Unlock()
	}

	return
}
