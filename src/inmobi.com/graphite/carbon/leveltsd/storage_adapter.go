package leveltsd

import (
	"inmobi.com/graphite/carbon/mq"
	"inmobi.com/graphite/carbon/storage"
	"strconv"
)

type LevelDbStorage struct {
	federator *levelfederator
}

func (this *LevelDbStorage) Init(config map[string]string) {
	this.federator = buildStorage(config)
	port, _ := strconv.Atoi(config["reader-port"])
	reader := make_rpc_server(this.federator, uint16(port))
	go reader()
}

func (this *LevelDbStorage) GetMetric(metric string) (interface{}, bool) {
	return this.federator.getMetric(metric)
}

func (this *LevelDbStorage) CreateMetric(metric string) (interface{}, bool) {
	return this.federator.createMetric(metric)
}

func (this *LevelDbStorage) UncheckedWrite(lookupRef interface{}, x mq.MetricReading) bool {
	return this.federator.uncheckedWrite(lookupRef.(*metricIndex), x)
}

func (this *LevelDbStorage) Release() {
	this.federator.release()
	this.federator = nil
}

func init() {
	x := LevelDbStorage{}
	storage.RegisterAdapter("leveltsd", &x)
}
