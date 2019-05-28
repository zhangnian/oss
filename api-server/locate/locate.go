package locate

import (
	"encoding/json"
	"log"
	"net/http"
	"oss/common"
	"strings"
	"time"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	key := strings.Split(r.URL.EscapedPath(), "/")[2]
	addr := Locate(key)
	if addr == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write([]byte(addr))
}

func Locate(key string) string {
	mq := common.NewRabbitMQ("amqp://182.61.19.174:5672")
	defer mq.Close()

	msg := map[string]string{
		"key": key,
	}

	mq.Publish("dataserver", msg)

	c := mq.Consume()
	go func() {
		time.Sleep(time.Second * 1)
		mq.Close()
	}()

	m := <-c
	if len(m.Body) == 0 {
		log.Printf("查找对象key：%s失败\n", key)
		return ""
	}

	retMsg := make(map[string]string)
	if err := json.Unmarshal(m.Body, &retMsg); err != nil {
		panic(err)
	}

	return retMsg["addr"]
}
