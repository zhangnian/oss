package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"oss/api-server/heartbeat"
	"oss/api-server/locate"
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

func storeObject(r io.Reader, hash string, size int64) (int, error) {
	if locate.Exist(hash) {
		log.Printf("对象：%s已存在", hash)
		return http.StatusOK, nil
	}

	stream, err := putStream(hash, size)
	if err != nil {
		return http.StatusServiceUnavailable, err
	}

	if stream == nil {
		log.Println("putSteam failed")
		return http.StatusInternalServerError, err
	}

	reader := io.TeeReader(r, stream)
	computedHash := common.CalculateHash(reader)
	log.Printf("computedHash: %s, hash: %s\n", computedHash, hash)
	if computedHash != hash {
		log.Println("hash不匹配，撤销临时对象")
		stream.Commit(false)
		return http.StatusBadRequest, nil
	}

	stream.Commit(true)
	log.Println("提交临时对象")

	return http.StatusOK, nil
}

func putStream(name string, size int64) (*objectstream.TempPutStream, error) {
	dsAddr := heartbeat.ChooseRandomDataServer()
	if dsAddr == "" {
		return nil, fmt.Errorf("can't find any dataserver")
	}

	log.Printf("随机选取的Data Server为：%s\n", dsAddr)
	return objectstream.NewTempPutStream(dsAddr, name, size)
}

func put(w http.ResponseWriter, r *http.Request) {
	hash := utils.GetHashFromHeader(r)
	if hash == "" {
		log.Println("missing hash header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	size := utils.GetSizeFromHeader(r)

	log.Printf("api size: %d\n", size)

	code, err := storeObject(r.Body, hash, size)
	if err != nil {
		w.WriteHeader(code)
		return
	}

	if code != http.StatusOK {
		w.WriteHeader(code)
		return
	}

	name := utils.GetObjectName(r)
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
