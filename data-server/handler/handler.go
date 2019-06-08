package handler

import (
	"io"
	"net/http"
	"os"
	"oss/common"
	"oss/data-server/g"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method

	if method == "GET" {
		get(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func get(w http.ResponseWriter, r *http.Request) {
	key := common.GetObjectName(r)
	filepath := g.GetFilePath(key)
	f, err := os.Open(filepath)
	if err != nil{
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	hash := common.CalculateHash(f)
	if hash != key{
		os.Remove(filepath)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	f.Close()

	sendFile(w, filepath)
}

func sendFile(w io.Writer, filePath string){
	f, _ := os.Open(filePath)
	defer f.Close()

	io.Copy(w, f)
}
