package heartbeat

import (
	"encoding/json"
	"log"
	"math/rand"
	"oss/api-server/g"
	"oss/common"
	"sync"
	"time"
)

var m_dataservers = make(map[string]time.Time)
var m_dataservers_mutex sync.Mutex

func StartHeartbeat() {
	mq := common.NewRabbitMQ(g.MQ_ADDR)
	defer mq.Close()

	mq.Bind("apiserver")
	c := mq.Consume()

	go removeExpiredDataServers()

	for msg := range c {
		m := make(map[string]string)
		err := json.Unmarshal(msg.Body, &m)
		if err != nil {
			log.Printf("json unmarshal error: %s\n", err.Error())
			continue
		}

		addr := m["addr"]
		m_dataservers_mutex.Lock()
		m_dataservers[addr] = time.Now()
		m_dataservers_mutex.Unlock()
	}
}

func removeExpiredDataServers() {
	for {

		m_dataservers_mutex.Lock()
		for addr, t := range m_dataservers {
			if t.Add(time.Second * 10).Before(time.Now()) {
				delete(m_dataservers, addr)
			}
		}
		m_dataservers_mutex.Unlock()

		time.Sleep(time.Second * 10)
	}
}

func GetDataServers() []string {
	servers := make([]string, 0)

	m_dataservers_mutex.Lock()
	defer m_dataservers_mutex.Unlock()

	for addr, _ := range m_dataservers {
		servers = append(servers, addr)
	}

	return servers
}

func ChooseRandomDataServer() string {
	ds := GetDataServers()
	if len(ds) == 0 {
		return ""
	}

	return ds[rand.Intn(len(ds))]
}
