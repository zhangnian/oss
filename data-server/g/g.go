package g

var (
	DataDir string
	MQ_ADDR = "amqp://182.61.19.174:5672"
)

func GetFilePath(key string) string {
	return DataDir + "/objects/" + key
}

func GetMetaFilePath(key string) string {
	return DataDir + "/temp/" + key
}

func GetTempDataFilePath(key string) string {
	return DataDir + "/temp/" + key + ".dat"
}
