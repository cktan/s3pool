package op

import (
	"errors"
	"strings"
	"s3pool/s3meta"
)


func GetETag(args []string) (string, error) {
	if len(args) != 2 {
		return "", errors.New("Expected 2 arguments for _GETETAG")
	}
	bucket, key := args[0], args[1]

	etag := s3meta.Get(bucket, key)
	return etag + "\n", nil
}

func SetETag(args []string) (string, error) {
	if len(args) != 3 {
		return "", errors.New("Expected 2 arguments for _SETETAG")
	}
	bucket, key, etag := args[0], args[1], args[2]

	s3meta.Set(bucket, key, etag)
	return "\n", nil
}

func RemoveKey(args []string) (string, error) {
	if len(args) != 2 {
		return "", errors.New("Expected 2 arguments for _REMOVEKEY")
	}
	bucket, key := args[0], args[1]
	s3meta.Remove(bucket, key)
	return "\n", nil
}

func ListPrefix(args []string) (string, error) {
	if len(args) != 1 && len(args) != 2 {
		return "", errors.New("Expected 1 or 2 arguments for _LISTPREFIX")
	}
	bucket := args[0]
	prefix := ""
	if len(args) == 2 {
		prefix = args[1]
	}
	key, err := s3meta.List(bucket, prefix)
	if err != nil {
		return "", err
	}
	
	var reply strings.Builder
	for _, k := range(key) {
		reply.WriteString(k)
		reply.WriteString("\n")
	}

	return reply.String(), nil
}
