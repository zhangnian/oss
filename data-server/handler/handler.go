package handler

import (
	"io"
	"log"
	"net/http"
	"os"
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
	path := r.URL.EscapedPath() // /objects/xxxx
	key := strings.Split(path, "/")[2]

	filepath := os.Getenv("DS_PATH") + "/objects/" + key
	f, err := os.Create(filepath)
	if err != nil {
		log.Printf("create file failed, error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(f, r.Body)
}

func get(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath() // /objects/xxxx
	key := strings.Split(path, "/")[2]

	filepath := os.Getenv("DS_PATH") + "/objects/" + key
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
