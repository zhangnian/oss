package g

var (
	DataDir string
	MQ_ADDR = "amqp://182.61.19.174:5672"
)

func GetFilePath(key string) string {
	return DataDir + "/objects/" + key
}
