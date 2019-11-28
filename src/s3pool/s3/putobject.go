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
package s3

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"s3pool/cat"
	"s3pool/conf"
	"s3pool/strlock"
)

//
// aws s3api put-object
//
func PutObject(bucket, key, fname string) error {
	if conf.Verbose(1) {
		log.Println("s3 put-object", bucket, key, fname)
	}

	if len(fname) > 0 && fname[0] != '/' {
		return fmt.Errorf("Filename parameter must be an absolute path")
	}

	// lock to serialize on (bucket,key)
	lockname, err := strlock.Lock(bucket + ":" + key)
	if err != nil {
		return err
	}
	defer strlock.Unlock(lockname)

	// we need to remove the file and meta file from cache if they are there
	datapath, err := mapToPath(bucket, key)
	if err != nil {
		return err
	}
	metapath := datapath + "__meta__"
	os.Remove(metapath)
	os.Remove(datapath)

	// push the file to AWS
	cmd := exec.Command("aws", "s3api", "put-object",
		"--bucket", bucket,
		"--key", key,
		"--body", fname)
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("aws s3api put-object failed -- %s", errbuf.String())
	}

	// reflect the new file in our catalog
	cat.Update(bucket, key, "new")
	return nil
}
