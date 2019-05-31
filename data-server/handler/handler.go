package handler

import (
	"io"
	"log"
	"net/http"
	"os"
	"oss/data-server/g"
	"oss/data-server/locate"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if method == "PUT" {
		put(w, r)
		return
	}

	if method == "GET" {
		get(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func put(w http.ResponseWriter, r *http.Request) {
	key := strings.Split(r.URL.EscapedPath(), "/")[2]

	filepath := g.GetFilePath(key)
	f, err := os.Create(filepath)
	if err != nil {
		log.Printf("create file failed, error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(f, r.Body)
	locate.AddObject(key)
}

func get(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath() // /objects/xxxx
	key := strings.Split(path, "/")[2]

	filepath := g.GetFilePath(key)
	f, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		log.Printf("open file error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(w, f)
}
