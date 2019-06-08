package temp

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"oss/common"
	"oss/data-server/g"
	"oss/data-server/locate"
	"strconv"
	"strings"
)

type tempInfo struct {
	UUID string
	Name string
	Size int64
}

func (t *tempInfo) writeToFile() error {
	filePath := g.GetMetaFilePath(t.UUID)
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	b, _ := json.Marshal(t)
	f.Write(b)
	return nil
}

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method

	if method == "PUT" {
		put(w, r)
		return
	}

	if method == "PATCH" {
		patch(w, r)
		return
	}

	if method == "POST" {
		post(w, r)
		return
	}

	if method == "DELETE" {
		delete(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func post(w http.ResponseWriter, r *http.Request) {
	output, _ := exec.Command("uuidgen").Output()
	uuid := strings.Trim(string(output), "\n")
	log.Printf("generated uuid: %s\n", uuid)

	name := common.GetObjectName(r)
	size, _ := strconv.ParseInt(r.Header.Get("size"), 10, 64)

	t := tempInfo{
		UUID: uuid,
		Name: name,
		Size: size,
	}

	log.Printf("file meta info: %v\n", t)

	err := t.writeToFile()
	if err != nil {
		log.Printf("write to file error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = os.Create(g.GetTempDataFilePath(t.UUID))
	if err != nil {
		log.Printf("create temp date file error: %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write([]byte(uuid))
}

func readFromFile(uuid string) (*tempInfo, error) {
	filePath := g.GetMetaFilePath(uuid)

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var t tempInfo
	err = json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func patch(w http.ResponseWriter, r *http.Request) {
	uuid := common.GetObjectName(r)

	tempInfo, err := readFromFile(uuid)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	metaFilePath := g.GetMetaFilePath(uuid)
	dataFilePath := g.GetTempDataFilePath(uuid)
	f, err := os.OpenFile(dataFilePath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	_, err = io.Copy(f, r.Body)
	if err != nil {
		log.Println("io.Copy error")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	info, _ := f.Stat()
	actualSize := info.Size()
	if actualSize != tempInfo.Size {
		os.Remove(metaFilePath)
		os.Remove(dataFilePath)
		log.Printf("文件大小不匹配，元数据文件大小：%d，实际文件大小：%d\n", tempInfo.Size, actualSize)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func put(w http.ResponseWriter, r *http.Request) {
	uuid := common.GetObjectName(r)

	metaInfo, err := readFromFile(uuid)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	metaFilePath := g.GetMetaFilePath(uuid)
	tempDataFilePath := g.GetTempDataFilePath(uuid)

	f, err := os.Open(tempDataFilePath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	actualSize := metaInfo.Size
	os.Remove(metaFilePath)

	if fileInfo.Size() != actualSize {
		os.Remove(tempDataFilePath)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	newDataFilePath := g.GetFilePath(metaInfo.Name)

	err = os.Rename(tempDataFilePath, newDataFilePath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	locate.AddObject(metaInfo.Name)
}

func delete(w http.ResponseWriter, r *http.Request) {
	uuid := common.GetObjectName(r)

	metaFilePath := g.GetMetaFilePath(uuid)
	tempDataFilePath := g.GetTempDataFilePath(uuid)

	os.Remove(metaFilePath)
	os.Remove(tempDataFilePath)
}
