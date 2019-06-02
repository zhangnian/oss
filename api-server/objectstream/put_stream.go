package objectstream

import (
	"fmt"
	"io"
	"net/http"
)

type PutStream struct {
	writer *io.PipeWriter
	c      chan error
}

func NewPutStream(server, name string) *PutStream {
	reader, writer := io.Pipe()
	c := make(chan error)

	go func() {
		url := fmt.Sprintf("http://%s/objects/%s", server, name)
		req, err := http.NewRequest("PUT", url, reader)
		client := http.Client{}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("dataserver return http code: %d", resp.StatusCode)
		}
		c <- err
	}()

	return &PutStream{writer, c}
}

func (s *PutStream) Write(b []byte) (int, error) {
	return s.writer.Write(b)
}

func (s *PutStream) Close() error {
	s.writer.Close()
	return <-s.c
}
