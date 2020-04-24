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
package s3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func mktmpfile() (path string, err error) {
	fp, err := ioutil.TempFile("tmp", "s3f_")
	if err != nil {
		return
	}
	defer fp.Close()
	path, err = filepath.Abs(fp.Name())
	return

}

// move file src to dst while ensuring that
// the dst's dir is created if necessary
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	idx := strings.LastIndexByte(dst, '/')
	if idx > 0 {
		dirpath := dst[:idx]
		if err := os.MkdirAll(dirpath, 0755); err != nil {
			return fmt.Errorf("Cannot mkdir %s -- %v", dirpath, err)
		}
	}

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("Cannot mv file -- %v", err)
	}

	return nil
}

// Read the ETag entry from a FNAME__meta__ file
func extractETag(path string) string {
	byt, err := ioutil.ReadFile(path)
	if err != nil {
		return ""
	}

	var dat map[string]interface{}
	err = json.Unmarshal(byt, &dat)
	if err != nil {
		return ""
	}

	ret, ok := dat["ETag"]
	if !ok {
		return ""
	}

	etag := ret.(string)
	etag = strings.Trim(etag, "\"")

	return etag
}

func mapToPath(bucket, key string) (path string, err error) {
	path, err = filepath.Abs(fmt.Sprintf("data/%s/%s", bucket, key))
	return
}

func fileReadable(path string) bool {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
	}
	return err == nil
}
