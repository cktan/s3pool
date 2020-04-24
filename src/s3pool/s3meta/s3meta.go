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


