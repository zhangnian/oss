package version

import (
	"log"
	"net/http"
	"strings"
)


func Handler(w http.ResponseWriter, r *http.Request){
	method := r.Method
	if method != http.MethodGet{
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	from := 0
	size := 1000
	name := strings.Split(r.URL.EscapedPath(), "/")[2]

	log.Println(name, from, size)
}
