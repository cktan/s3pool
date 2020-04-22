package s3meta

import (
	"s3pool/strlock"
)

func KnownBuckets() []string {
	return getKnownBuckets()
}

func Invalidate(bucket string) {
	invalidate(bucket)
}

func SearchExact(bucket, key string) (etag string) {
	store := getStore(bucket)
	etag = store.getETag(key)
	return
}

func SetETag(bucket, key, etag string) {
	store := getStore(bucket)
	store.setETag(key, etag)
}

func Delete(bucket, key string) {
	store := getStore(bucket)
	store.setETag(key, "")
}

func List(bucket string, prefix string) (key, etag []string, err error) {
	lock := strlock.Lock("s3meta/" + bucket + "/" + prefix)
	key, etag, err = list(bucket, prefix)
	strlock.Unlock(lock)
	return
}
