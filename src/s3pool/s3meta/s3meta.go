package s3meta

import (
	"hash/fnv"
)


type requestType struct {
	command string
	param []string
	reply chan *replyType
}


type replyType struct {
	err error
	key []string
	etag []string
}


type serverCB struct {
	ch chan *requestType
}

var server []*serverCB
var nserver uint32

func newServer() *serverCB {
	s := &serverCB{make(chan *requestType)}
	go s.run()
	return s
}

func Initialize(n int) {
	if (n <= 0) {
		n = 29
	}
	nserver = uint32(n)
	server = make([]*serverCB, n)
	for i := 0; i < n; i++ {
		server[i] = newServer()
	}
}

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


func List(bucket string, prefix string) (error, []string, []string) {
	ch := make(chan *replyType)
	h := fnv.New32a()
	h.Write([]byte(bucket))
	h.Write([]byte{0})
	h.Write([]byte(prefix))	
	server[h.Sum32() % nserver].ch <- &requestType{"LIST", []string{bucket, prefix}, ch}
	reply := <- ch
	return reply.err, reply.key, reply.etag
}

