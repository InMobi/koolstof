package assembly

import (
	"github.com/vaughan0/go-ini"
	"inmobi.com/graphite/carbon/audit"
	"inmobi.com/graphite/carbon/listener"
	"inmobi.com/graphite/carbon/mq"
	"inmobi.com/graphite/carbon/storage"
	"log"
	"runtime"
	"strconv"
)

type storagePipeline struct {
	bounded_main   chan mq.MetricReading
	audit_stream   chan mq.MetricReading
	create_offload chan mq.MetricReading
}

func makeListener(config map[string]string, c chan<- mq.MetricReading) *listener.PlaintextReceiver {
	port_str := config["port"]
	port, err := strconv.ParseUint(port_str, 10, 16)
	if err != nil {
		panic("Error parsing value of 'port'")
	}
	return listener.NewPlaintextReceiver(listener.PlaintextConfig{uint16(port)}, c)
}

func manageListener(l *listener.PlaintextReceiver) {
	l.Listen()
	go l.Run()
}

func manageStorageQueues(config map[string]string) storagePipeline {
	var retval storagePipeline

	retval.audit_stream = make(chan mq.MetricReading, 10000)

	queue_len_str := config["backlog"]
	queue_len, err := strconv.ParseUint(queue_len_str, 10, 32)
	if err != nil {
		panic("Error parsing value of 'backlog'")
	}

	retval.bounded_main = make(chan mq.MetricReading, queue_len)

	retval.create_offload = make(chan mq.MetricReading, 1000000)

	return retval
}

func manageStorageEngine(config1 map[string]string, config2 map[string]string) storage.StorageCore {
	return storage.BuildDispatcher(config1, config2)
}

func manageWritePipelines(q storagePipeline, s storage.StorageCore) {
	s.DispatchLoop(q.audit_stream, q.create_offload, false, 1)
	s.DispatchLoop(q.bounded_main, q.create_offload, true, 4)
	s.DispatchLoop(q.create_offload, nil, true, 1)
}

func manageAudit(c chan mq.MetricReading, f audit.QueueDepths) {
	audit.InitMetrics(c, f)
}

func concurrency(s string) {
	if c, err := strconv.ParseInt(s, 10, 16); err == nil {
		log.Printf("Concurrency level is %d\n", c)
		runtime.GOMAXPROCS(int(c))
	}
}

func BuildallAndRun(file ini.File) {
	gmp, _ := file.Get("", "GOMAXPROCS")
	concurrency(gmp)

	queues := manageStorageQueues(file.Section("storage"))

	storage := manageStorageEngine(file.Section("storage"), file.Section("storage-engine"))

	manageWritePipelines(queues, storage)

	depthCalculator := func() audit.StoragePipelineDepths {
		return audit.StoragePipelineDepths{
			len(queues.bounded_main), len(queues.audit_stream), len(queues.create_offload) }
	}

	manageAudit(queues.audit_stream, depthCalculator)

	listener := makeListener(file.Section("listener"), queues.bounded_main)
	manageListener(listener)
}
