package op

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

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
	// Get destination path
	path, err := mapToPath(bucket, key)
	if err != nil {
		err = errors.New(fmt.Sprintf("Cannot map bucket+key to path -- %v", err))
		return "", err
	}

	// Get etag from meta file
	metapath := path + "__meta__"
	etag := extractETag(metapath)

	// Prepare to write to tmp file
	tmppath, err := mktmpfile()
	if err != nil {
		err = errors.New(fmt.Sprintf("Cannot create temp file -- %v", err))
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
		err = errors.New(fmt.Sprintf("aws s3api get-object failed -- %v", err))
		return "", err
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

func Pull(args []string) (reply string, err error) {
	if len(args) != 2 {
		err = errors.New("Expected 2 arguments for PULL")
		return
	}
	bucket, key := args[0], args[1]

	path, err := s3GetObject(bucket, key)
	if err != nil {
		return
	}

	reply = path
	return
}
