package audit

import (
	"sync/atomic"
)

type MinAvgMax struct {
	Min   *uint32
	Max   *uint32
	Total uint64

	N uint32
}

type WriterStats struct {
	Write_microseconds   MinAvgMax
	Create_microseconds  MinAvgMax
	datapoints_per_write MinAvgMax // deprecated

	Cache_full_events uint32
	cache_queries     uint32 // deprecated
	cached_metrics    uint32 // unsupported

	Create_ratelimit_exceeded uint32

	Datapoints_written uint32

	Metric_create_errors uint32
	Metrics_created      uint32

	Write_errors             uint32
	Write_operations         uint32
	Write_ratelimit_exceeded uint32
}

type CarbonStats struct {
	Writer            WriterStats
	cpu_usage         uint32 // unsupported
	mem_usage         uint32 // unsupported
	Metrics_received  uint32
	Garbled_reception uint32 // our addition
}

func (stats *MinAvgMax) RecordMeasurement(val uint32) {
	atomic.AddUint32(&stats.N, 1)
	atomic.AddUint64(&stats.Total, uint64(val))
	if stats.Min == nil {
		stats.Min = &val
	} else if val < *stats.Min {
		stats.Min = &val
	}

	if stats.Max == nil {
		stats.Max = &val
	} else if val > *stats.Max {
		stats.Max = &val
	}
}

type StoragePipelineDepths struct {
	Bounded_main int
	Audit_stream int
	Create_offload int
}

type QueueDepths func() StoragePipelineDepths

func (x *StoragePipelineDepths) getUsage() int {
	return x.Bounded_main + x.Audit_stream + x.Create_offload
}
