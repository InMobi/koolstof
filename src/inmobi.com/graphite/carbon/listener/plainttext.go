package listener

import (
	"bufio"
	"fmt"
	"inmobi.com/graphite/carbon/audit"
	"inmobi.com/graphite/carbon/logging"
	"inmobi.com/graphite/carbon/mq"
	"log"
	"net"
	"sync/atomic"
)

var logger *log.Logger

type connnectionContext struct {
	conn net.Conn
	id   int64
}

func init() {
	logger = logging.MakeLogger("plaintext-listener: ")
}

type PlaintextConfig struct {
	Port uint16
}

type PlaintextReceiver struct {
	config  PlaintextConfig
	clients <-chan connnectionContext
	sink    chan<- mq.MetricReading
	active  bool
	server net.Listener
}

func (this *PlaintextReceiver) Listen() {
	server, err := net.Listen("tcp", fmt.Sprintf(":%d", this.config.Port))
	if err != nil {
		logger.Panicln(err)
	}
	this.server = server

	clients := make(chan connnectionContext)
	this.clients = clients
	this.active = true

	go func() {
		var i int64
		defer close(clients)

		for this.active {
			client, err := server.Accept()
			i++
			if err != nil {
				logger.Printf("connection(%010d) %v\n", err)
				continue
			}
			logger.Printf("connection(%010d) %v <-> %v\n", i, client.LocalAddr(), client.RemoteAddr())
			context := connnectionContext{client, i}
			clients <- context
		}
	}()
}

func (this *PlaintextReceiver) Run() {
	for {
		c, ok := <-this.clients
		if ok {
			go handleConn(c, this.sink)
		} else {
			break;
		}
	}
}

func (this *PlaintextReceiver) Close() {
	logger.Printf("Shutting down listener %v", this)
	this.active = false
	this.server.Close()
}

/* The plain text protocol implementation of the carbon server protocol */
func NewPlaintextReceiver(config PlaintextConfig, writer_queue chan<- mq.MetricReading) *PlaintextReceiver {
	retval := new(PlaintextReceiver)
	retval.config, retval.sink = config, writer_queue
	return retval
}

/* Per connection handler
Note that there is no concurrency in processing for a given connection
*/
func handleConn(context connnectionContext, writer_queue chan<- mq.MetricReading) {
	client := context.conn
	defer client.Close()

	b := bufio.NewReader(client)
	i := 0
	for {
		line, err := b.ReadString('\n')
		if err != nil {
			/* Any sort of a line read error causes us to exit
			If the last line of transmission lacks a newline, it is
			discarded*/
			logger.Printf("connection(%010d) received %d line(s); closing due to %v\n", context.id, i, err)
			break
		}
		var val mq.MetricReading
		var parts, _ = fmt.Sscanf(line, "%s %f %d", &val.Metric, &val.Val, &val.Time)
		i++

		audit := audit.GetMetrics()
		if parts == 3 {
			atomic.AddUint32(&audit.Metrics_received, 1)
			select {
			case writer_queue <- val:

			default:
				logger.Println("write buffer is full")
				atomic.AddUint32(&audit.Writer.Cache_full_events, 1)
			}
		} else {
			atomic.AddUint32(&audit.Garbled_reception, 1)
			logger.Printf("connection(%010d) Garbled message: %v", context.id, line)
		}
	}
}

// vim: noet ts=4 sw=4
