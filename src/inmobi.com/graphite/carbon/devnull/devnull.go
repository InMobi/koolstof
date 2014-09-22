package devnull

import (
	"inmobi.com/graphite/carbon/mq"
	"inmobi.com/graphite/carbon/storage"
)

type devNullStorage struct {
}

func (this *devNullStorage) Init(config map[string]string) {
}

func (this *devNullStorage) GetMetric(metric string) (interface{}, bool) {
	return nil, true
}

func (this *devNullStorage) CreateMetric(metric string) (interface{}, bool) {
	return nil, true
}

func (this *devNullStorage) UncheckedWrite(lookupRef interface{}, x mq.MetricReading) bool {
	return true
}

func (this *devNullStorage) Release() {
}

func init() {
	x := devNullStorage{}
	storage.RegisterAdapter("null", &x)
}
