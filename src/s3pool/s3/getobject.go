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
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"s3pool/conf"
	"s3pool/s3meta"
	"s3pool/strlock"
	"strings"
)

//
// Invoke aws s3api to retrieve a file. Form:
//
//   aws s3api get-object --bucket BUCKET --key KEY --if-none-match ETAG tmppath
//
func GetObject(bucket string, key string, force bool) (retpath string, hit bool, err error) {
	if conf.Verbose(1) {
		log.Println("s3 get-objects", bucket, key)
	}

	// lock to serialize pull on same (bucket,key)
	lockname := strlock.Lock(bucket + ":" + key)
	defer strlock.Unlock(lockname)

	// Get destination path
	path, err := mapToPath(bucket, key)
	if err != nil {
		err = fmt.Errorf("Cannot map bucket+key to path -- %v", err)
		return
	}

	// Get etag from meta file
	metapath := path + "__meta__"
	etag := extractETag(metapath)
	catetag := s3meta.Get(bucket, key)

	// check that destination path exists
	if !fileReadable(path) {
		if conf.Verbose(1) {
			log.Println(" ... file does not exist")
		}
		etag = ""
	}

	// If the file's etag and our catalog's etag matched, don't go fetch it
	if etag != "" && etag == catetag && !force {
		// this optimization completely avoids a call to S3, so it
		// is rather important. We can't just rely on the file's etag
		// because the file may have changed on S3. We hope that the
		// etag in Catalog is refreshed from time to time.
		if conf.Verbose(1) {
			log.Println(" ... cache hit:", key)
		}
		retpath = path
		hit = true
		return
	}

	if conf.Verbose(1) {
		log.Println(" ... cache miss:", key)
		if catetag == "" {
			log.Println(" ... missing catalog entry")
		}
	}

	// Prepare to write to tmp file
	tmppath, err := mktmpfile()
	if err != nil {
		err = fmt.Errorf("Cannot create temp file -- %v", err)
		return
	}
	defer os.Remove(tmppath)

	// Run the command
	cmd := exec.Command("aws", "s3api", "get-object",
		"--bucket", bucket,
		"--key", key,
		"--if-none-match", etag,
		tmppath)
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	if err = cmd.Run(); err != nil {
		errstr := string(errbuf.Bytes())
		notModified := strings.Contains(errstr, "Not Modified") && strings.Contains(errstr, "(304)")
		if notModified {
			// File was cached and was not modified at source
			err = nil
			if conf.Verbose(1) {
				log.Println(" ... file not modified")
			}
			log.Println("   ... etag", etag)
			log.Println("   ... catetag", catetag)
			if etag != catetag && etag != "" {
				log.Println(" ... update", key, etag)
				s3meta.Set(bucket, key, etag)
			}
			retpath = path
			return
		}
		noSuchKey := strings.Contains(errstr, "NoSuchKey")
		if noSuchKey {
			s3meta.Remove(bucket, key)
		}
		err = fmt.Errorf("aws s3api get-object failed -- %s", errstr)
		return
	}

	// The file has been downloaded to tmppath. Now move it to the right place.
	if err = moveFile(tmppath, path); err != nil {
		return
	}

	// Save the meta info
	ioutil.WriteFile(metapath, outbuf.Bytes(), 0644)

	// Update catalog with the new etag
	etag = extractETag(metapath)
	if etag != "" {
		//log.Println(" ... update", key, etag)
		s3meta.Set(bucket, key, etag)
	}

	// Done!
	retpath = path
	return
}
