package objectstream

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type TempPutStream struct {
	Server string
	UUID   string
}

func NewTempPutStream(server, name string, size int64) (*TempPutStream, error) {
	url := fmt.Sprintf("http://%s/temp/%s", server, name)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("size", fmt.Sprintf("%d", size))

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("client.Do error: %s\n", err.Error())
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST return status code: %d\n", resp.StatusCode)
	}

	uuid, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if string(uuid) == "" {
		return nil, fmt.Errorf("uuid is empty")
	}

	log.Printf("uuid: %s\n", string(uuid))

	return &TempPutStream{server, string(uuid)}, nil
}

func (t *TempPutStream) Write(p []byte) (int, error) {
	log.Printf("patch len: %d\n", len(p))

	url := fmt.Sprintf("http://%s/temp/%s", t.Server, t.UUID)
	req, err := http.NewRequest("PATCH", url, strings.NewReader(string(p)))
	if err != nil {
		return 0, err
	}

	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(p)))

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("client.Do error: %s\n", err.Error())
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("patch status code: %d\n", resp.StatusCode)
		return 0, fmt.Errorf("dataServer return status code: %d", resp.StatusCode)
	}

	return len(p), nil
}

func (t *TempPutStream) Commit(ok bool) {
	method := "DELETE"
	if ok {
		method = "PUT"
	}

	url := fmt.Sprintf("http://%s/temp/%s", t.Server, t.UUID)
	req, _ := http.NewRequest(method, url, nil)

	client := http.Client{}
	client.Do(req)
}
