package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"oss/api-server/handler"
	"oss/api-server/heartbeat"
	"oss/api-server/locate"
	"oss/api-server/version"
)

func main() {
	host := flag.String("h", "0.0.0.0", "listen host")
	port := flag.Int("p", 12345, "listen port")

	flag.Parse()

	listenAddr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println(listenAddr)

	go heartbeat.StartHeartbeat()

	http.HandleFunc("/objects/", handler.Handler)
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/version/", version.Handler)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
