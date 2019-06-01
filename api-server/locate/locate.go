package locate

import (
	"encoding/json"
	"log"
	"net/http"
	"oss/api-server/g"
	"oss/api-server/utils"
	"oss/common"
	"time"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	name := utils.GetObjectName(r)

	meta, err := common.SearchLastVersion(name)
	if err != nil {
		log.Printf("查找对象：%s元数据失败\n", name)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	log.Println(meta)
	if meta.Hash == "" {
		log.Printf("对象：%s已被删除\n", name)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	addr := Locate(meta.Hash)
	if addr == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write([]byte(addr))
}

func Locate(key string) string {
	mq := common.NewRabbitMQ(g.MQ_ADDR)
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
