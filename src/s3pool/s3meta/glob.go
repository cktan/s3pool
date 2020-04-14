package s3meta

import (
	"errors"
	"strings"
	globutil "github.com/cktan/glob"
	"s3pool/s3"
)

func globPrefix(pattern string) string {
	s := strings.SplitN(pattern, "*", 2)[0]
	s = strings.SplitN(s, "?", 2)[0]
	if s == pattern {
		s = ""
	}
	return s
}


func (p *serverCB) glob(req *requestType) (reply *replyType) {
	if len(req.param) != 2 {
		reply = &replyType{err: errors.New("GLOB requires param (bucket, key)")}
		return
	}
	bucket, pattern := req.param[0], req.param[1]
	query := bucket + "/" + pattern
	if reply = store.find(query); reply != nil {
		return
	}

	reply = &replyType{}
	g, err := globutil.Compile(pattern, '/')
	if err != nil {
		reply.err = err
		return
	}
	filter := func(key string) bool {
		return g.Match(key)
	}

	key := make([]string, 0, 100)
	etag := make([]string, 0, 100)

	prefix := globPrefix(pattern)
	err = s3.ListObjects(bucket, prefix, func(k, t string) {
		if filter(k) {
			if k[len(k)-1] == '/' {
				// skip DIR
				return
			}
			key = append(key, k)
			etag = append(etag, t)
		}
	})
	if err != nil {
		reply.err = err
		return
	}

	reply.key = key
	reply.etag = etag
	store.insert(query, reply)
	return
}
