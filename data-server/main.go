package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"oss/data-server/handler"
	"oss/data-server/heartbeat"
	"oss/data-server/locate"
)

func main() {
	host := flag.String("h", "0.0.0.0", "listen host")
	port := flag.Int("p", 12345, "listen port")

	flag.Parse()

	listenAddr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println(listenAddr)

	go heartbeat.StartHeartbeat(listenAddr)
	go locate.StartLocate(listenAddr)

	http.HandleFunc("/objects/", handler.Handler)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
