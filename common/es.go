package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	url2 "net/url"
	"strings"
)

type Metadata struct {
	Name    string
	Version int
	Size    int64
	Hash    string
}

type hit struct {
	Source Metadata `json:"_source"`
}

type searchResult struct {
	Hits struct {
		Total int
		Hits  []hit
	}
}

func getMetadata(name string, version int) (m Metadata, err error) {
	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/objects/%s_%d/_source", name, version)

	r, err := http.Get(url)
	if err != nil {
		return
	}

	if r.StatusCode != http.StatusOK {
		log.Printf("failed to get %s_%d\n", name, version)
		return
	}

	b, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(b, &m)
	return
}

func SearchLastVersion(name string) (m Metadata, err error) {
	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/_search?q=name:%s&size=1&sort=version:desc",
		url2.PathEscape(name))

	log.Println(url)
	r, err := http.Get(url)
	if err != nil {
		log.Printf("http get error: %s\n", err.Error())
		return
	}

	if r.StatusCode != http.StatusOK {
		log.Printf("failed to search %s, http status code: %d\n", name, r.StatusCode)
		return
	}

	b, _ := ioutil.ReadAll(r.Body)
	var sr searchResult
	json.Unmarshal(b, &sr)

	if len(sr.Hits.Hits) == 0 {
		return
	}
	m = sr.Hits.Hits[0].Source
	return
}

func GetMeatadata(name string, version int) (Metadata, error) {
	if version == 0 {
		return SearchLastVersion(name)
	}

	return getMetadata(name, version)
}

func PutMetadata(name string, version int, size int64, hash string) error {
	doc := fmt.Sprintf(`{"name": "%s", "version": %d, "size": %d, "hash": "%s"}`,
		name, version, size, hash)

	client := http.Client{}

	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/objects/%s_%d?op_type=create", name, version)
	req, _ := http.NewRequest("POST", url, strings.NewReader(doc))
	req.Header.Add("content-type", "application/json; charset=UTF-8")
	r, err := client.Do(req)
	if err != nil {
		return err
	}

	if r.StatusCode == http.StatusConflict {
		return PutMetadata(name, version+1, size, hash)
	}

	if r.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to put object, status code: %d\n", r.StatusCode)
	}

	return nil
}

func AddVersion(name, hash string, size int64) error {
	meta, err := SearchLastVersion(name)
	if err != nil {
		PutMetadata(name, 1, size, hash)
	}

	return PutMetadata(name, meta.Version+1, size, hash)
}

func SearchAllVersion(name string, from, size int) ([]Metadata, error) {
	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/_search?sort=name,version&from=%d&size=%d", from, size)

	if name != "" {
		url += "&q=name:" + name
	}

	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	metas := make([]Metadata, 0)
	b, _ := ioutil.ReadAll(r.Body)

	var sr searchResult
	json.Unmarshal(b, &sr)

	for i := range sr.Hits.Hits {
		metas = append(metas, sr.Hits.Hits[i].Source)
	}

	return metas, nil
}
