package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"oss/api-server/heartbeat"
	"oss/api-server/objectstream"
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

	if method == "DELETE" {
		delete(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func storeObject(r io.Reader, name string) (int, error) {
	stream, err := putStream(name)
	if err != nil {
		return http.StatusServiceUnavailable, err
	}

	io.Copy(stream, r)
	err = stream.Close()
	if err != nil {
		return http.StatusServiceUnavailable, err
	}

	return http.StatusOK, nil
}

func putStream(name string) (*objectstream.PutStream, error) {
	dsAddr := heartbeat.ChooseRandomDataServer()
	if dsAddr == "" {
		return nil, fmt.Errorf("can't find any dataserver")
	}

	return objectstream.NewPutStream(dsAddr, name), nil
}

func put(w http.ResponseWriter, r *http.Request) {
	hash := utils.GetHashFromHeader(r)
	if hash == "" {
		log.Println("missing hash header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	code, err := storeObject(r.Body, hash)
	if err != nil {
		w.WriteHeader(code)
		return
	}

	name := utils.GetObjectName(r)
	size := utils.GetSizeFromHeader(r)

	err = common.AddVersion(name, hash, size)
	if err != nil {
		log.Printf("更新对象元数据失败, error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("更新对象: %s %s元数据成功\n", name, hash)
}

func get(w http.ResponseWriter, r *http.Request) {
	name := utils.GetObjectName(r)
	ver := r.URL.Query()["version"]
	version := 0
	if len(ver) > 0 {
		version, _ = strconv.Atoi(ver[0])
	}

	meta, err := common.GetMeatadata(name, version)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if meta.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	log.Printf("开始获取对象：%s\n", meta.Hash)

	stream, err := objectstream.NewGetStream(meta.Hash)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	io.Copy(w, stream)
}

func delete(w http.ResponseWriter, r *http.Request) {
	name := utils.GetObjectName(r)

	meta, err := common.SearchLastVersion(name)
	if err != nil {
		log.Printf("删除对象失败，err: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = common.PutMetadata(name, meta.Version+1, 0, "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
