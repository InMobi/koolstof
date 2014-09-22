package leveltsd

import (
	"fmt"
	"github.com/extemporalgenome/epochdate"
	"inmobi.com/graphite/carbon/logging"
	"inmobi.com/graphite/carbon/mq"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var fLogger *log.Logger

var queryid uint32

const _MAX_OPEN_SHARDS = 23

func init() {
	fLogger = logging.MakeLogger("leveltsd-federator: ")
}

/*
Provides a unified view across multiple data shards. The exact partitioning
technique is never revealed to the caller. It also houses the various indices
needed for proper functioning of various operations
*/
type levelfederator struct {
	idx       *indices
	config     leveltsdConf
	shards    map[string]*shard
	writeLock sync.Locker
}

type leveltsdConf struct {
	Basedir string
	Sconfig shard_config
}

func buildStorage(configMap map[string]string) *levelfederator {
	retval := new(levelfederator)

	config := parseConfig(configMap)
	fLogger.Printf("leveltsd config is %v\n", logging.ObjectJsonifier(config))

	retval.config = config
	root := config.Basedir

	if stat, err := os.Stat(root); err != nil {
		fLogger.Panicln(err)
	} else if !stat.IsDir() {
		fLogger.Panicf("%s is not a directory", root)
	}
	retval.idx, _ = mkIndex(root)
	retval.shards = make(map[string]*shard)
	retval.writeLock = new(sync.Mutex)

	return retval
}

func (this *levelfederator) getMetric(metric string) (*metricIndex, bool) {
	return this.idx.getMetric(metric, false)
}

func (this *levelfederator) createMetric(metric string) (*metricIndex, bool) {
	return this.idx.getMetric(metric, true)
}

func (this *levelfederator) uncheckedWrite(key *metricIndex, x mq.MetricReading) bool {
	if s := this.getShard(x.Time, true); s != nil {
		return s.insert(key, x.Time, x.Val)
	} else {
		return false
	}
}

/* Returns handle to an open shard that would contain data for a given
time range */
func (this *levelfederator) getShard(ts uint64, createIfAbsent bool) *shard {
	t := time.Unix(int64(ts), 0).UTC()
	yyyymmdd := fmt.Sprintf("%4d%02d%02d", t.Year(), t.Month(), t.Day())

	return this._getShardFromDate(yyyymmdd, createIfAbsent)
}

func (this *levelfederator) release() {
	this.writeLock.Lock()
	defer this.writeLock.Unlock()

	this.idx.release()
	for _, s := range this.shards {
		s.release()
	}
	this.shards = nil
	this.idx = nil
}

func (this *levelfederator) _getShardFromDate(d string, createIfAbsent bool) *shard {
	if s, ok := this.shards[d]; ok {
		return s
	}
	return this._makeShardFromDate(d, createIfAbsent)
}

func (this *levelfederator) _makeShardFromDate(d string, createIfAbsent bool) *shard {
	this.writeLock.Lock()
	defer this.writeLock.Unlock()

	if s, ok := this.shards[d]; ok {
		return s
	}

	path := _shard_namer(this.config.Basedir, d)
	if s, err := mkShard(path, createIfAbsent, this.config.Sconfig); err != nil {
		fLogger.Printf("mkshard for %s failed\n", path)
		return nil
	} else {
		/* limit max open shards
		TODO: smarter, selective eviction logic */
		if len(this.shards) > _MAX_OPEN_SHARDS {
			for _, s := range this.shards {
				s.release()
			}
			this.shards = make(map[string]*shard)
		}
		this.shards[d] = s
		return s
	}
}

/* Finds a set of candidate shards for a given query */
func _rangeShards(start uint64, end uint64) []string {
	if start > end {
		fLogger.Panicf("Start is greater than end: %d %d\n", start, end)
	}
	sdate, _ := epochdate.NewFromUnix(int64(start))
	edate, _ := epochdate.NewFromUnix(int64(end))
	retval := make([]string, 0, edate-sdate+1)
	for i := sdate; i <= edate; i++ {
		dashDate := i.String()
		dashDate = strings.Replace(dashDate, "-", "", -1)
		retval = append(retval, dashDate)
	}
	return retval
}

/* Dispatches and federates a search query across all candidate shards */
func (this *levelfederator) dataScan(key *metricIndex, start uint64, end uint64) []Datapoint {
	atomic.AddUint32(&queryid, 1)
	queryLog := fmt.Sprintf("query(%010d)", queryid)

	shards := _rangeShards(start, end)
	retval := make([]Datapoint, 0, 1440)
	fLogger.Printf("%s shards to scan: %d\n", queryLog, len(shards))
	for _, s := range shards {
		shandler := this._getShardFromDate(s, false)
		if shandler != nil {
			partial_result := shandler.dataScan(key, start, end)
			fLogger.Printf("%s partial datapoints found %d\n", queryLog, len(partial_result))
			retval = append(retval, partial_result...)
		}
	}
	fLogger.Printf("%s total datapoints found %d\n", queryLog, len(retval))
	return retval
}

/* Make the fs name for a given shard */
func _shard_namer(baseDir string, shardId string) string {
	return fmt.Sprintf("%s/tsd-data-%s.db", baseDir, shardId)
}

func parseConfig(config map[string]string) leveltsdConf {
	var retval leveltsdConf

	retval.Basedir = config["root"]
	sconfig := defaultShardConfig()

	if val := _getInt(config, "write-batch-count", 32); val != nil {
		sconfig.Write_batch_size = uint(*val)
	}

	if val := _getInt(config, "write-batch-interval-seconds", 32); val != nil {
		x := uint32(*val)
		sconfig.Write_batch_fill_timeout = time.Duration(x) * time.Second
	}

	if val := _getInt(config, "write-concurrency", 8); val != nil {
		sconfig.Write_concurrency = uint8(*val)
	}

	if val := _getInt(config, "memory-cache", 64); val != nil {
		sconfig.Data_cache = int(*val)
	}

	retval.Sconfig = sconfig
	return retval
}

func _getInt(m map[string]string, key string, places int) *uint64 {
	if val, ok := m[key]; ok {
		if x, err := strconv.ParseUint(val, 10, places); err != nil {
			fLogger.Panicf("parse error in %s %v", key, err)
		} else {
			return &x
		}
	}
	return nil
}
