package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"oss/api-server/heartbeat"
	"oss/api-server/locate"
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

	dsAddr := heartbeat.ChooseRandomDataServer()
	if dsAddr == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	log.Printf("开始上传对象，选取的data server：%s\n", dsAddr)

	dsUrl := fmt.Sprintf("http://%s/objects/%s", dsAddr, key)

	req, err := http.NewRequest("PUT", dsUrl, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		log.Printf("client do error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Write([]byte(dsAddr))
}

func get(w http.ResponseWriter, r *http.Request) {
	key := strings.Split(r.URL.EscapedPath(), "/")[2]
	log.Printf("开始获取对象：%s\n", key)

	datasvrAddr := locate.Locate(key)
	if datasvrAddr == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	url := fmt.Sprintf("http://%s/objects/%s", datasvrAddr, key)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("get object error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.Copy(w, resp.Body)
}
