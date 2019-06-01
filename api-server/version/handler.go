package version

import (
	"encoding/json"
	"net/http"
	"oss/api-server/utils"
	"oss/common"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	from := 0
	size := 4
	name := utils.GetObjectName(r)
	for {
		metas, err := common.SearchAllVersion(name, from, size)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for i := range metas {
			if metas[i].Hash == "" {
				continue
			}
			b, _ := json.Marshal(metas[i])
			w.Write(b)
			w.Write([]byte("\n"))
		}

		if len(metas) < size {
			return
		}

		from += size
	}
}
