/**
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019-2020 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktanx@gmail.com.
 */
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
