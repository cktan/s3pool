package cat

import (
	"log"
)

var bm = newBucketMap()
var trace = false

func KnownBuckets() []string {
	return bm.Keys()
}

func Exists(bucket string) bool {
	_, ok := bm.Get(bucket)
	return ok
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
	bm.Put(bucket, km)
}
