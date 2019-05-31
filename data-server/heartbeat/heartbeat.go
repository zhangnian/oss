package heartbeat

import (
	"oss/common"
	"oss/data-server/g"
	"time"
)

func StartHeartbeat(listenAddr string) {
	mq := common.NewRabbitMQ(g.MQ_ADDR)
	defer mq.Close()

	for {
		mq.Publish("apiserver", map[string]string{"addr": listenAddr})
		time.Sleep(time.Second * 5)
	}
}
