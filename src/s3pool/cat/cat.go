package cat

import (
	"log"
	"time"
)

var bm = newBucketMap()
var trace = false

func KnownBuckets() []string {
	return bm.Keys()
}

func Exists(bucket string) bool {
	km, ok := bm.Get(bucket)
	if !ok {
		return false
	}

	km.RLock()
	exp := km.Expires
	km.RUnlock()

	return time.Now().Before(exp)
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
		etag, ok = km.Map[key]
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
		km.Map[key] = etag
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
		delete(km.Map, key)
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
	for kkk := range km.Map {
		if filter(kkk) {
			key = append(key, kkk)
		}
	}
	km.RUnlock()
	return
}

func Store(bucket string, key, etag []string) {
	if trace {
		log.Println("Catalog.Store", bucket)
	}
	km := &KeyMap{Map: make(map[string]string)}
	for i := range key {
		km.Map[key[i]] = etag[i]
	}

	// Set Expiration to 15 minutes later
	km.Expires = time.Now().Add(time.Duration(15) * time.Minute)

	bm.Put(bucket, km)
}
