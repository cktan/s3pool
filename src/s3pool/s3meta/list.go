package s3meta

func list(bucket, prefix string) (key []string, err error) {
	store := getStore(bucket)
	if xkey, xetag, ok := store.retrieve(prefix); ok {
		// make a copy of key
		key = make([]string, 0, len(xkey))
		for i := range xkey {
			if xetag[i] != "" {
				key = append(key, xkey[i])
			}
		}
		return
	}

	var etag []string
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
