package locate

import (
	"encoding/json"
	"log"
	"os"
	"oss/common"
)

func locate(key string) bool {
	log.Printf("开始定位对象：%s\n", key)

	filepath := os.Getenv("DS_PATH") + "/objects/" + key
	_, err := os.Stat(filepath)
	return !os.IsNotExist(err)
}

func StartLocate(addr string) {
	mq := common.NewRabbitMQ("amqp://182.61.19.174:5672")
	defer mq.Close()

	mq.Bind("dataserver")
	ch := mq.Consume()

	for msg := range ch {
		log.Println("收到对象定位请求")

		objectMsg := make(map[string]string)
		err := json.Unmarshal(msg.Body, &objectMsg)
		if err != nil {
			log.Printf("unmarshal error: %s\n", err.Error())
			continue
		}

		key := objectMsg["key"]
		if locate(key) {
			objectAddr := map[string]string{
				"key":  key,
				"addr": addr,
			}
			mq.Send(msg.ReplyTo, objectAddr)
			log.Printf("对象：%s定位成功，数据节点地址：%s\n", key, addr)
			continue
		}

		log.Printf("对象：%s定位失败\n", key)
	}
}
