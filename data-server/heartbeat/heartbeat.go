package heartbeat

import (
	"oss/common"
	"time"
)

func StartHeartbeat(listenAddr string) {
	mq := common.NewRabbitMQ("amqp://182.61.19.174:5672")
	defer mq.Close()

	for {
		mq.Publish("apiserver", map[string]string{"addr": listenAddr})
		time.Sleep(time.Second * 5)
	}
}
