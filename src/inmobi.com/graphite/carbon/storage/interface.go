package storage

import (
	"inmobi.com/graphite/carbon/mq"
)

type StorageAdapter interface {
	Init(config map[string]string)
	GetMetric(metric string) (interface{}, bool)
	CreateMetric(metric string) (interface{}, bool)
	UncheckedWrite(lookupRef interface{}, x mq.MetricReading) bool
	Release()
}
