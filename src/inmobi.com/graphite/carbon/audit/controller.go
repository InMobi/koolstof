package audit

import (
	"fmt"
	"inmobi.com/graphite/carbon/logging"
	"inmobi.com/graphite/carbon/mq"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var metrics *CarbonStats
var create sync.Mutex

var metricPrefix = _makeMetricPrefix()

var logger *log.Logger

func init() {
	logger = logging.MakeLogger("audit: ")
}

func GetMetrics() *CarbonStats {
	return metrics
}

func InitMetrics(c chan<- mq.MetricReading, f QueueDepths) {
	create.Lock()
	defer create.Unlock()

	if metrics == nil {
		metrics = new(CarbonStats)

		ticker := time.Tick(60 * time.Second)
		go func() {
			for {
				<-ticker
				resetMetrics(c, f)
			}
		}()
	}
}

func resetMetrics(c chan<- mq.MetricReading, f QueueDepths) {
	var snapshot *CarbonStats
	snapshot, metrics = metrics, new(CarbonStats)
	now := time.Now()
	go snapshot.writeInstance(c, now, f)
}

func (this *CarbonStats) writeInstance(c chan<- mq.MetricReading, t time.Time, f QueueDepths) {
	time.Sleep(10 * time.Second) // Wait for things to catchup
	logger.Printf("%d %s\n", t, logging.ObjectJsonifier(this))
	ts := uint64(t.Unix())

	_write32(c, metricPrefix+"metrics_received", this.Metrics_received, ts)
	_write32(c, metricPrefix+"garbled_reception", this.Garbled_reception, ts)
	this.Writer.writeInstance(c, metricPrefix+"writer.", ts)
	queue_stats := f()
	_write32(c, metricPrefix+"writer.cached_datapoints", uint32(queue_stats.getUsage()) , ts)
}

func (this *WriterStats) writeInstance(c chan<- mq.MetricReading, prefix string, ts uint64) {
	this.Write_microseconds.writeInstance(c, prefix+"write_microseconds.", ts)
	this.Create_microseconds.writeInstance(c, prefix+"create_microseconds.", ts)

	_write32(c, prefix+"cache_full_events", this.Cache_full_events, ts)
	_write32(c, prefix+"create_ratelimit_exceeded", this.Create_ratelimit_exceeded, ts)
	_write32(c, prefix+"datapoints_written", this.Datapoints_written, ts)
	_write32(c, prefix+"metric_create_errors", this.Metric_create_errors, ts)
	_write32(c, prefix+"metrics_created", this.Metrics_created, ts)
	_write32(c, prefix+"write_errors", this.Write_errors, ts)
	_write32(c, prefix+"write_operations", this.Write_operations, ts)
	_write32(c, prefix+"write_ratelimit_exceeded", this.Write_ratelimit_exceeded, ts)
}

func (this *MinAvgMax) writeInstance(c chan<- mq.MetricReading, prefix string, ts uint64) {
	_write32p(c, prefix+"min", this.Min, ts)
	_write32p(c, prefix+"max", this.Max, ts)
	_write32(c, prefix+"total", this.N, ts)
	if this.N != 0 {
		c <- mq.MetricReading{prefix + "avg", float64(this.Total) / float64(this.N), ts}
	} else {
		_write32(c, prefix+"avg", 0, ts)
	}
}

func _write32p(c chan<- mq.MetricReading, metric string, val *uint32, ts uint64) {
	if val != nil {
		_write32(c, metric, *val, ts)
	}
}
func _write32(c chan<- mq.MetricReading, metric string, val uint32, ts uint64) {
	c <- mq.MetricReading{metric, float64(val), ts}
}

func _makeMetricPrefix() string {
	template := "carbon.carbon-daemons.%s.carbon-storage-go."
	hostname, _ := os.Hostname()
	hostname = strings.Replace(hostname, ".", "_", -1)
	return fmt.Sprintf(template, hostname)
}
