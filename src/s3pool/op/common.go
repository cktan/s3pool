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
 *  cktanx@gmail.com.
 */
package op

import (
	"os"
	"s3pool/cat"
	"s3pool/conf"
	"s3pool/strlock"
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

// Check that we have a catalog on bucket. If not, create it.
func checkCatalog(bucket string) error {

	// serialize refresh on bucket
	lockname := strlock.Lock("refresh " + bucket)
	defer strlock.Unlock(lockname)

	ok, err := cat.Exists(bucket)
	if err != nil {
		return err
	}
	if !ok {
		// notify bucketmon; it will invoke refresh to create entry in catalog.
		conf.NotifyBucketmon(bucket)

		// wait for it
		for !ok {
			time.Sleep(time.Second)
			ok, err = cat.Exists(bucket)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
