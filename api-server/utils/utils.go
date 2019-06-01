package utils

import (
	"net/http"
	"strconv"
	"strings"
)

func GetObjectName(r *http.Request) string {
	return strings.Split(r.URL.EscapedPath(), "/")[2]
}

func GetHashFromHeader(r *http.Request) string {
	digest := r.Header.Get("digest")
	if len(digest) < 9 {
		return ""
	}

	if digest[:8] != "SHA-256=" {
		return ""
	}

	return digest[8:]
}

func GetSizeFromHeader(r *http.Request) int64 {
	size, _ := strconv.ParseInt(r.Header.Get("Content-length"), 10, 64)
	return size
}
