package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"oss/data-server/g"
	"oss/data-server/handler"
	"oss/data-server/heartbeat"
	"oss/data-server/locate"
	"oss/data-server/temp"
)

func main() {
	host := flag.String("h", "0.0.0.0", "listen host")
	port := flag.Int("p", 9001, "listen port")
	ds_path := flag.String("d", "/tmp/ds", "data storage dir path")

	flag.Parse()

	g.DataDir = *ds_path
	listenAddr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println(listenAddr)

	locate.ScanObjects()

	go heartbeat.StartHeartbeat(listenAddr)
	go locate.StartLocate(listenAddr)

	http.HandleFunc("/objects/", handler.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
