package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"oss/api-server/heartbeat"
	"oss/api-server/locate"
	"oss/api-server/utils"
	"oss/common"
	"strconv"
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

	if method == "DELETE"{
		delete(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}


func put(w http.ResponseWriter, r *http.Request) {
	hash := utils.GetHashFromHeader(r)
	if hash == ""{
		log.Println("missing hash header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dsAddr := heartbeat.ChooseRandomDataServer()
	if dsAddr == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	log.Printf("开始上传对象: %s，选取的data server：%s\n", hash, dsAddr)

	dsUrl := fmt.Sprintf("http://%s/objects/%s", dsAddr, hash)

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

	name := utils.GetObjectName(r)
	size := utils.GetSizeFromHeader(r)

	err = common.AddVersion(name, hash, size)
	if err != nil{
		log.Printf("更新对象元数据失败, error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("更新对象: %s %s元数据成功\n", name, hash)
	w.Write([]byte(dsAddr))
}

func get(w http.ResponseWriter, r *http.Request) {
	name := utils.GetObjectName(r)
	ver := r.URL.Query()["version"]
	version := 0
	if len(ver) > 0{
		version, _ = strconv.Atoi(ver[0])
	}

	meta, err := common.GetMeatadata(name, version)
	if err != nil{
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if meta.Hash == ""{
		w.WriteHeader(http.StatusNotFound)
		return
	}

	log.Printf("开始获取对象：%s\n", meta.Hash)

	datasvrAddr := locate.Locate(meta.Hash)
	if datasvrAddr == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	url := fmt.Sprintf("http://%s/objects/%s", datasvrAddr, meta.Hash)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("get object error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.Copy(w, resp.Body)
}


func delete(w http.ResponseWriter, r *http.Request){
	name := utils.GetObjectName(r)

	meta, err := common.SearchLastVersion(name)
	if err != nil{
		log.Printf("删除对象失败，err: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = common.PutMetadata(name, meta.Version+1, 0, "")
	if err != nil{
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
