package op

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"s3pool/strlock"
	"strings"
)

var trace_s3api bool = false

func s3ListObjects(bucket string, wr io.Writer) error {
	if trace_s3api {
		log.Println("s3 list-objects", bucket)
	}

	var err error

	// invoke s3api to list objects
	cmd := exec.Command("aws", "s3api", "list-objects-v2",
		"--bucket", bucket,
		"--query", "Contents[].{Key: Key}")

	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("aws s3api list-objects failed -- %v", err)
	}
	defer cmd.Wait()

	// read stdout of cmd
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		s := scanner.Text()
		// Parse s of the form
		//       "Key" : "key value"
		nv := strings.SplitN(s, ":", 2)
		if len(nv) != 2 {
			continue
		}
		name := strings.Trim(nv[0], " \t\"")
		if name != "Key" {
			continue
		}
		value := strings.Trim(nv[1], " \t\"")
		// ignore empty value or value that looks like a DIR (ending with / )
		if len(value) == 0 || value[len(value)-1] == '/' {
			continue
		}
		value = value + "\n"
		wr.Write([]byte(value))
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
		return "dummy"
	}

	var dat map[string]interface{}
	err = json.Unmarshal(byt, &dat)
	if err != nil {
		return "dummy"
	}

	ret, ok := dat["ETag"]
	if !ok {
		return "dummy"
	}

	return ret.(string)
}

//
// Invoke aws s3api to retrieve a file. Form:
//
//   aws s3api get-object --bucket BUCKET --key KEY --if-none-match ETAG tmppath
//
func s3GetObject(bucket string, key string) (string, error) {
	if trace_s3api {
		log.Println("s3 get-objects", bucket, key)
	}

	// lock to serialize pull on same (bucket,key)
	s, err := strlock.Lock(bucket + ":" + key)
	if err != nil {
		return "", err
	}
	defer strlock.Unlock(s)

	// Get destination path
	path, err := mapToPath(bucket, key)
	if err != nil {
		return "", fmt.Errorf("Cannot map bucket+key to path -- %v", err)
	}

	// Get etag from meta file
	metapath := path + "__meta__"
	etag := extractETag(metapath)

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
		return path, nil
	}
	if err != nil {
		return "", fmt.Errorf("aws s3api get-object failed -- %v", err)
	}

	// The file has been downloaded to tmppath. Now move it to the right place.
	if err = moveFile(tmppath, path); err != nil {
		return "", err
	}

	// Save the meta info
	ioutil.WriteFile(metapath, outbuf.Bytes(), 0644)

	// Done!
	return path, nil
}

//
// aws s3 cp src dst
//
func s3PutObject(bucket, key, fname string) error {
	if trace_s3api {
		log.Println("s3 put-object", bucket, key, fname)
	}

	cmd := exec.Command("aws", "s3api", "put-object",
		"--bucket", bucket,
		"--key", key,
		"--body", fname)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("aws cp failed -- %v", err)
	}
	return nil
}
