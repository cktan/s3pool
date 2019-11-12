/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktan@gmail.com.
 */
package op

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"s3pool/cat"
	"s3pool/strlock"
	"strings"
	"syscall"
	"time"
)

func statTimes(path string) (atime, mtime, ctime time.Time, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return
	}
	mtime = fi.ModTime()
	stat := fi.Sys().(*syscall.Stat_t)
	atime = time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
	ctime = time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec))
	return
}

func fileMtimeSince(path string) (time.Duration, error) {
	_, mtime, _, err := statTimes(path)
	if err != nil {
		return 0, err
	}
	return time.Since(mtime), nil
}

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
			return fmt.Errorf("Cannot mkdir %s -- %v", dirpath, err)
		}
	}

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("Cannot mv file -- %v", err)
	}

	return nil
}

// Check that we have a catalog on bucket. If not, create it.
func checkCatalog(bucket string) error {

	// serialize check catalog on bucket to prevent
	// concurrent creation of catalog on the same bucket.
	lockname, err := strlock.Lock("refresh " + bucket)
	if err != nil {
		return err
	}
	defer strlock.Unlock(lockname)

	if cat.Exists(bucket) {
		return nil
	}

	// Refresh() will (re)create it
	log.Println("Refresh due to missing catalog")
	_, err = Refresh([]string{bucket})
	return err
}
