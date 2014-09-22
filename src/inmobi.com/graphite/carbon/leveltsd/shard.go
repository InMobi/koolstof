package leveltsd

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"inmobi.com/graphite/carbon/logging"
	"log"
	"os"
	"time"
)

const _BATCH_SIZE = 10000
const _DATA_CACHE_SIZE = (1 << 20) * 16 // 16 MB
const _BATCH_TIME_SECONDS = 10
const _CONCURRENT_WRITERS = 4

/*
Represents a single shard of timeseries data.

The shard does not carry any information regarding the subset of data that
it holds. This is by design. All such information is expected to be managed
at a higher level. This allows for flexibile migration of data if needed
*/
type shard struct {
	db      *levigo.DB
	writers *list.List
	ro      *levigo.ReadOptions
	wo      *levigo.WriteOptions
	filter  *levigo.FilterPolicy
	logger  *log.Logger
	wchan   chan<- triplet
	config  shard_config
	cache   *levigo.Cache
}

type shard_config struct {
	Data_cache               int
	Write_batch_size         uint
	Write_batch_fill_timeout time.Duration
	Write_concurrency        uint8
}

type shard_writer struct {
	s   *shard
	c   <-chan triplet
	end chan bool
	id  uint8
}

type Datapoint struct {
	Timestamp uint64
	Value     float64
}

type writeBatch struct {
	next uint
	data []msg
}

type triplet struct {
	key *metricIndex
	val Datapoint
}

type msg struct {
	key []byte
	val []byte
}

/* Opens an existing shard or creates it if absent */
func mkShard(fs_path string, createIfAbsent bool, config shard_config) (*shard, error) {
	iLogger.Printf("shard make request %s %t\n", fs_path, createIfAbsent)
	retval := new(shard)

	var exists bool
	var err error

	if _, err = os.Stat(fs_path); err == nil {
		exists = true
	}

	if !createIfAbsent && !exists {
		return nil, err
	}

	opts := levigo.NewOptions()
	defer opts.Close()

	opts.SetCreateIfMissing(createIfAbsent)

	cache := levigo.NewLRUCache(config.Data_cache)
	opts.SetCache(cache)
	filter := levigo.NewBloomFilter(10)
	opts.SetFilterPolicy(retval.filter)

	if retval.db, err = levigo.Open(fs_path, opts); err == nil {
		retval.cache = cache
		retval.ro = levigo.NewReadOptions()
		retval.wo = levigo.NewWriteOptions()
		retval.filter = filter

		logger_prefix := fmt.Sprintf("leveltsd-shard (%s): ", fs_path)
		retval.logger = logging.MakeLogger(logger_prefix)

		if exists {
			magic,_ := retval.db.Get(retval.ro, []byte(SCHEME_MAGIC_ID))
			if bytes.Compare(magic, []byte(SERIALIZATION_TECHNIQUE)) != 0 {
				error_msg := fmt.Sprintf("Expected magic code was %v but got %v", []byte(SERIALIZATION_TECHNIQUE), magic)
				retval.logger.Println(error_msg)
				errors.New(error_msg)
			}
		}

		wchan := make(chan triplet)
		retval.wchan = wchan
		retval.writers = list.New()
		retval.config = config

		for i := uint8(0); i < config.Write_concurrency; i++ {
			retval.newWriter(wchan, i)
		}
	} else {
		cache.Close()
		filter.Close()
		iLogger.Printf("died with %v\n", retval.db)
	}
	return retval, err
}

func (this *shard) release() {
	close(this.wchan)
	this.logger.Println("closed receive pipeline")

	for e := this.writers.Front(); e != nil; e = e.Next() {
		var w *shard_writer = e.Value.(*shard_writer)
		close(w.end)
	}
	this.db.Close()
	this.ro.Close()
	this.wo.Close()
	this.cache.Close()
	this.filter.Close()
	this.logger.Println("closed")
}

/* Drain the accumulated write commands
This does not look into the write queue; instead it expected values to be
passed in as a argument
*/
func (this *shard) _flush(batch *writeBatch, id uint8) {
	levelBatch := levigo.NewWriteBatch()
	defer levelBatch.Close()

	n := batch.next

	for i := uint(0); i < n; i++ {
		datum := batch.data[i]
		levelBatch.Put(datum.key, datum.val)
	}
	this.logger.Printf("(%02d) flushing %d Datapoint(s)\n", id, n)
	if n != 0 {
		if err := this.db.Write(this.wo, levelBatch); err != nil {
			this.logger.Println(err)
		}
	}

	batch.next = 0
}

/* Create a new background writer */
func (this *shard) newWriter(wchan <-chan triplet, id uint8) *shard_writer {
	retval := shard_writer{this, wchan, make(chan bool), id}
	go retval._write_loop()
	this.writers.PushBack(&retval)
	return &retval
}

/* Retrieve all sets of values associated with a given metric for a given
time range */
func (this *shard) dataScan(key *metricIndex, start uint64, end uint64) []Datapoint {
	start_key := keyString(key.key, start)
	end_key := keyString(key.key, end)

	it := this.db.NewIterator(this.ro)
	defer it.Close()

	retval := make([]Datapoint, 0, 100)

	/* Levigo lacks a range scan; it only has a full scan operation. However, it
	is possible to inexpensively seek to a given point, hence we use that to control
	the startig point of a scan
	*/
	it.Seek(start_key)
	for it = it; it.Valid(); it.Next() {
		k := it.Key()

		/* We all need to terminate the end of range scan; else it would proceeed
		till the end of the tree index */
		if bytes.Compare(k, end_key) > 0 {
			break
		}

		vb := it.Value()
		val := readVal(vb)
		retval = append(retval, Datapoint{extractTsFromFullKey(k), val})
	}

	return retval
}

/* Schedule a value for writing. This operation might fail if the writer pipelines
are closed at the time of invocation */
func (this *shard) insert(key *metricIndex, ts uint64, val float64) bool {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	this.wchan <- triplet{key, Datapoint{ts, val}}
	return true
}

/* The background writer function */
func (this *shard_writer) _write_loop() {
	foo := new(writeBatch)
	config := this.s.config
	foo.data = make([]msg, config.Write_batch_size)

	timeout := time.Tick(config.Write_batch_fill_timeout)

	for {
		select {
		case x, ok := <-this.c:
			if ok {
				var blob msg
				blob.key = keyString(x.key.key, rounder(x.val.Timestamp, x.key.step_in_seconds))
				blob.val = writeVal(x.val.Value)
				if foo.next == config.Write_batch_size {
					this.s._flush(foo, this.id)
				}
				foo.data[foo.next] = blob
				foo.next++
			}
		case <-timeout:
			this.s._flush(foo, this.id)
		case <-this.end:
			return
		}
	}
}

func defaultShardConfig() shard_config {
	return shard_config{_DATA_CACHE_SIZE, _BATCH_SIZE, time.Duration(_BATCH_TIME_SECONDS) * time.Second, _CONCURRENT_WRITERS}
}
