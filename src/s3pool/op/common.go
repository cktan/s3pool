package op

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func mapToPath(bucket, key string) (path string, err error) {
	path, err = filepath.Abs(fmt.Sprintf("data/%s/%s", bucket, key))
	return
}

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
			err = errors.New(fmt.Sprintf("Cannot mkdir %s -- %s", dirpath, err.Error()))
			return err
		}
	}

	if err := os.Rename(src, dst); err != nil {
		err = errors.New(fmt.Sprintf("Cannot mv file -- %s", err.Error()))
		return err
	}

	return nil
}
