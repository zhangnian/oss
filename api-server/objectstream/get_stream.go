package objectstream

import (
	"fmt"
	"io"
	"net/http"
	"oss/api-server/locate"
)

type GetStream struct{
	reader io.Reader
}

func NewGetStream(name string) (*GetStream, error){
	dsAddr := locate.Locate(name)
	if dsAddr == ""{
		return nil, fmt.Errorf("no dataserver found")
	}

	url := fmt.Sprintf("http://%s/objects/%s", dsAddr, name)
	r, err := http.Get(url)
	if err != nil{
		return nil, err
	}

	if err == nil && r.StatusCode != http.StatusOK{
		return nil, fmt.Errorf("dataserver return error: %s", err.Error())
	}

	return &GetStream{r.Body}, nil
}

func (s *GetStream) Read(p []byte) (int, error){
	return s.reader.Read(p)
}