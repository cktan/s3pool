package s3meta


func list(bucket, prefix string) (key, etag []string, err error) {
	store := getStore(bucket)
	if xkey, xetag, ok := store.retrieve(prefix); ok {
		// make a copy of key and etag
		key = append(xkey[:0:0], xkey...) 
		etag = append(xetag[:0:0], xetag...)
		return
	}

	err = s3ListObjects(bucket, prefix, func(k, t string) {
		if k[len(k)-1] == '/' {
			// skip DIR
			return
		}
		key = append(key, k)
		etag = append(etag, t)
	})

	if err != nil {
		return
	}

	store.insert(prefix, key, etag)
	return
}
