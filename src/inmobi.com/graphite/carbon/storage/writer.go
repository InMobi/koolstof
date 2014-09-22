package storage

import (
	"inmobi.com/graphite/carbon/audit"
	"inmobi.com/graphite/carbon/mq"
	"strconv"
	"sync/atomic"
	"time"
)

type StorageCore struct {
	adapter StorageAdapter
}

var max_write_rpm uint32
var max_create_rpm uint32

/* Make a new dispatcher factory given a set of config
This does not instantiate or start any dispatchers themselves */
func BuildDispatcher(config map[string]string, engine_conf map[string]string) StorageCore {
	max_write_rpm = getValue(config, "max_write_rpm")
	max_create_rpm = getValue(config, "max_create_rpm")

	storage := GetAdapter(config["engine"])
	if storage == nil {
		panic("nil storage")
	}
	storage.Init(engine_conf)
	retval := StorageCore{storage}
	return retval
}

/* Start "n" dispatchers working off a given command queue

This method returns immediately after starting the dispatchers. Also, there
is no way to stop the dispatchers
*/
func (x *StorageCore) DispatchLoop(c <-chan mq.MetricReading, offload chan<- mq.MetricReading, enforceLimits bool, concurrency int) {
	for i := 0; i < concurrency; i++ {
		go x._dispatchLoop(c, offload, enforceLimits)
	}
}

/* A single dispatch (infinite) loop. This is a blocking call */
func (x *StorageCore) _dispatchLoop(c <-chan mq.MetricReading, offload chan<- mq.MetricReading, enforceLimits bool) {
	for {
		val := <-c
		x.checkedWrite(val, enforceLimits, offload)
	}
}

/* The write operation for a single data point

It is important to note that we should never log any ratelimit violations
These checks exist to reduce the impact of a DoS due to load; the last thing
we need is to log them furiously thus slowing things down. Errors are also
not recorded; we expect the underlying calls to do the needful
*/
func (x *StorageCore) checkedWrite(val mq.MetricReading, enforceLimits bool, offload chan<- mq.MetricReading) {
	audit := audit.GetMetrics()

	write_limit_exceeded := enforceLimits && (audit.Writer.Write_operations > max_write_rpm)

	if write_limit_exceeded {
		atomic.AddUint32(&audit.Writer.Write_ratelimit_exceeded, 1)
		return
	}

	var ref interface{}
	var ok bool

	// Check if this metric exists or if it needs to be indexed
	if ref, ok = x.adapter.GetMetric(val.Metric); !ok {
		/*
		Create is expensive, so we offload it given a chance
		However, the offload queue also invokes this function, so the
		way we detect that is by the availability of an offload channel
		Unavailability means that we are expected to work on it now
		*/
		if offload != nil {
			select {
			case offload <- val:
			}
		} else {
			x.createAndWrite(val, enforceLimits)
		}
	} else {
		x.writePostlookup(audit, val, ref)
	}
}

func (x *StorageCore) createAndWrite(val mq.MetricReading, enforceLimits bool) {
	audit := audit.GetMetrics()

	var ref interface{}
	var ok bool

	// Check if this metric exists or if it needs to be indexed
	if ref, ok = x.adapter.GetMetric(val.Metric); !ok {
		create_limit_exceeded := enforceLimits && (audit.Writer.Metrics_created > max_create_rpm)
		if create_limit_exceeded {
			atomic.AddUint32(&audit.Writer.Create_ratelimit_exceeded, 1)
			return
		} else {
			start := time.Now()
			ref, ok = x.adapter.CreateMetric(val.Metric)

			if ok {
				var t uint32 = uint32(time.Since(start) / 1000)
				audit.Writer.Create_microseconds.RecordMeasurement(t)
				atomic.AddUint32(&audit.Writer.Metrics_created, 1)
			} else {
				atomic.AddUint32(&audit.Writer.Metric_create_errors, 1)
				return
			}
		}
	}

	x.writePostlookup(audit, val, ref)
}

func (x *StorageCore) writePostlookup(audit *audit.CarbonStats, val mq.MetricReading, ref interface{}) {
	start := time.Now()
	write_outcome := x.adapter.UncheckedWrite(ref, val)

	if write_outcome {
		var t uint32 = uint32(time.Since(start) / 1000)
		audit.Writer.Write_microseconds.RecordMeasurement(t)
		atomic.AddUint32(&audit.Writer.Datapoints_written, 1)
		atomic.AddUint32(&audit.Writer.Write_operations, 1)
	} else {
		atomic.AddUint32(&audit.Writer.Write_errors, 1)
	}
}

func getValue(config map[string]string, key string) uint32 {
	val_str := config[key]
	val, err := strconv.ParseUint(val_str, 10, 32)
	if err != nil {
		panic("Error parsing value of '" + key + "'")
	}
	return uint32(val)
}
