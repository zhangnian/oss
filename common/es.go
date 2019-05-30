package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	url2 "net/url"
)

type Metadata struct{
	Name string
	Version int
	Size int64
	Hash string
}

type hit struct{
	Source Metadata `json:"_source"`
}

type searchResult struct{
	Hits struct{
		Total int
		Hits []hit
	}
}


func getMetadata(name string, version int) (m Metadata, err error){
	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/objects/%s_%d/_source", name, version)

	r, err := http.Get(url)
	if err != nil{
		return
	}

	if r.StatusCode != http.StatusOK{
		log.Printf("failed to get %s_%d\n", name, version)
		return
	}

	b, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(b, &m)
	return
}


func SearchLastVersion(name string) (m Metadata, err error){
	url := fmt.Sprintf("http://182.61.19.174:9200/metadata/_search?q=name:%s&size=1&sort=version:desc",
		url2.PathEscape(name))

	r, err := http.Get(url)
	if err != nil{
		return
	}

	if r.StatusCode != http.StatusOK{
		log.Printf("failed to search %s\n", name)
		return
	}

	b, _ := ioutil.ReadAll(r.Body)
	var sr searchResult
	json.Unmarshal(b, sr)

	if len(sr.Hits.Hits) == 0{
		return
	}

	m = sr.Hits.Hits[0].Source
	return
}
