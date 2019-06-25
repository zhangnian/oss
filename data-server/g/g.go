package g

var (
	DataDir string
	MQ_ADDR = "amqp://120.132.116.122:5672"
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
