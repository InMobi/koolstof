package logging

import (
	"encoding/json"
	"log"
	"os"
)

func MakeLogger(name string) *log.Logger {
	return log.New(os.Stdout, name, log.LstdFlags)
}

func ObjectJsonifier(x interface{}) string {
	buf, _ := json.Marshal(x)
	return string(buf)
}
