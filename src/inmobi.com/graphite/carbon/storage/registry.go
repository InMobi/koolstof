package storage

import (
	"inmobi.com/graphite/carbon/logging"
	"log"
)

var logger *log.Logger
var adapters = make(map[string]StorageAdapter)

func init() {
	logger = logging.MakeLogger("storage-gateway: ")
}

func RegisterAdapter(name string, x StorageAdapter) {
	if x == nil {
		logger.Panicf("Trying to register nil for %s", name)
	}
	if _, dup := adapters[name]; dup {
		logger.Panicf("Adapter already registered under the name of %s", name)
	} else {
		logger.Printf("Registering storage engine named %s", name)
		adapters[name] = x
	}
}

func GetAdapter(name string) StorageAdapter {
	return adapters[name]
}
