package op

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"s3pool/cat"
	"s3pool/strlock"
	"strings"
	"time"
)

var trace_s3api bool = false
var use_goapi bool = false

func s3ListObjects(bucket string, notify func(key, etag string)) error {
	if trace_s3api {
		log.Println("s3 list-objects", bucket)
	}

	var err error

	// invoke s3api to list objects
	cmd := exec.Command("aws", "s3api", "list-objects-v2",
		"--bucket", bucket,
		"--query", "Contents[].{Key: Key, ETag: ETag}")
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %s", string(errbuf.Bytes()))
	}
	defer cmd.Wait()

	// read stdout of cmd
	scanner := bufio.NewScanner(pipe)
	var key string
	for scanner.Scan() {
		s := scanner.Text()
		// Parse s of the form
		//       "Key" : "key value"
		//       "ETag" : "\"etag\""
		nv := strings.SplitN(s, ":", 2)
		if len(nv) != 2 {
			continue
		}
		name := strings.Trim(nv[0], " \t\",")
		value := strings.Trim(nv[1], " \t\",\\")

		if name == "Key" {
			key = value
			continue
		}
		if name == "ETag" {
			// ignore empty value or value that looks like a DIR (ending with / )
			if len(key) > 0 && key[len(key)-1] != '/' {
				etag := value
				notify(key, etag)
			}
			key = ""
			continue
		}
		log.Println("ignore", name)
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}

	// clean up
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
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

//
// Invoke aws s3api to retrieve a file. Form:
//
//   aws s3api get-object --bucket BUCKET --key KEY --if-none-match ETAG tmppath
//
func s3GetObject(bucket string, key string, force bool) (string, error) {
	if trace_s3api {
		log.Println("s3 get-objects", bucket, key)
	}

	// lock to serialize pull on same (bucket,key)
	lockname, err := strlock.Lock(bucket + ":" + key)
	if err != nil {
		return "", err
	}
	defer strlock.Unlock(lockname)

	// Get destination path
	path, err := mapToPath(bucket, key)
	if err != nil {
		return "", fmt.Errorf("Cannot map bucket+key to path -- %v", err)
	}

	// Get etag from meta file
	metapath := path + "__meta__"
	etag := extractETag(metapath)
	catetag := cat.Find(bucket, key)

	// If etag did not change, don't go fetch it
	if etag != "" && etag == catetag && !force {
		if trace_s3api {
			log.Println(" ... cache hit:", key)
		}
		return path, nil
	}

	// Prepare to write to tmp file
	tmppath, err := mktmpfile()
	if err != nil {
		return "", fmt.Errorf("Cannot create temp file -- %v", err)
		return "", err
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
	err = cmd.Run()
	errstr := string(errbuf.Bytes())
	notModified := strings.Contains(errstr, "Not Modified") && strings.Contains(errstr, "(304)")
	if notModified {
		// File was cached and was not modified at source
		// change local mtime so we don't keep calling s3 to check etag
		now := time.Now()
		os.Chtimes(path, now, now)
		if etag != catetag {
			cat.Update(bucket, key, etag)
		}
		return path, nil
	}
	if err != nil {
		return "", fmt.Errorf("aws s3api get-object failed -- %s", errstr)
	}

	// The file has been downloaded to tmppath. Now move it to the right place.
	if err = moveFile(tmppath, path); err != nil {
		return "", err
	}

	// Save the meta info
	ioutil.WriteFile(metapath, outbuf.Bytes(), 0644)

	// Update catalog with the new etag
	etag = extractETag(metapath)
	if etag != "" {
		cat.Update(bucket, key, etag)
	}

	// Done!
	return path, nil
}

//
// aws s3api put-object
//
func s3PutObject(bucket, key, fname string) error {
	if trace_s3api {
		log.Println("s3 put-object", bucket, key, fname)
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
		return fmt.Errorf("aws s3api put-object failed -- %s", string(errbuf.Bytes()))
	}

	// reflect the new file in our catalog
	cat.Update(bucket, key, "new")
	return nil
}
