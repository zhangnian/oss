package locate

import (
	"encoding/json"
	"log"
	"oss/common"
	"oss/data-server/g"
	"path/filepath"
	"sync"
)

var objectMap = make(map[string]int)
var objectMapLocker sync.RWMutex

func locate(key string) bool {
	objectMapLocker.RLock()
	defer objectMapLocker.RUnlock()

	log.Println("locate key: " + key)
	_, ok := objectMap[key]
	return ok
}

func AddObject(key string) {
	objectMapLocker.Lock()
	defer objectMapLocker.Unlock()

	objectMap[key] = 1
}

func RemoveObject(key string) {
	objectMapLocker.Lock()
	defer objectMapLocker.Unlock()

	delete(objectMap, key)
}

func ScanObjects() {
	objectMapLocker.Lock()
	defer objectMapLocker.Unlock()

	files, err := filepath.Glob(g.DataDir + "/objects/*")
	if err != nil {
		return
	}

	for i := range files {
		key := filepath.Base(files[i])
		objectMap[key] = 1
	}

	log.Printf("扫描结束，共：%d个对象\n", len(objectMap))
}

func StartLocate(addr string) {
	mq := common.NewRabbitMQ(g.MQ_ADDR)
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
