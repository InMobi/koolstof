package leveltsd

import (
	"bytes"
	"encoding/json"
	"github.com/jmhodges/levigo"
	"inmobi.com/graphite/carbon/logging"
	"log"
	"strings"
	"sync"
)

const INDEX_CACHE_SIZE = 128 << 20 // 128 MB
const _DEFAULT_METRIC_INTERVAL = 60

var iLogger *log.Logger

var _BANNED_BYTES []byte

var _EMPTY_JSON_ARRAY_BYTES = []byte("")

func init() {
	iLogger = logging.MakeLogger("leveltsd-index: ")
	_BANNED_BYTES = []byte{byte('?'), byte('*'), byte('['), byte(']'), byte('/')}
}

type indices struct {
	writeLock sync.Locker
	dir       *levigo.DB
	pkey      *levigo.DB
	wo        *levigo.WriteOptions
	ro        *levigo.ReadOptions
	cache     *levigo.Cache
}

type metricIndex struct {
	metric          string
	key             []byte
	step_in_seconds uint32
}

/* Lists the immediate descendants (children) of a give
metric path. Failure to find any match would return an empty
slice
*/
func (this *indices) listChildern(path string) []string {
	spath := scrubMetric(path)

	var val []byte
	if val = this._lsPath(spath); val == nil {
		return []string{}
	}
	var retval []string

	if err := json.Unmarshal(val, &retval); err != nil {
		return []string{}
	} else {
		return retval
	}
}

/*
Fetches the internal representation of given metric. It is
possible to request a creation of the metric if it is absent.
This function is threadsafe
*/
func (this *indices) getMetric(metric string, createIfAbsent bool) (*metricIndex, bool) {
	spath := scrubMetric(metric)
	if len(spath) == 0 {
		return nil, false
	}

	/* Mutual exclusion is deferred for 2 reasons
	(1) The read path does not need mutual exclusion
	(2) Scrubbing a metric can happen concurrenctly for all cases
	*/
	if createIfAbsent {
		this.writeLock.Lock()
		defer this.writeLock.Unlock()
	}

	val, err := this.pkey.Get(this.ro, spath)
	if err != nil {
		iLogger.Println(err)
		return nil, false
	}
	if val != nil {
		retval := metricIndex{metric, val, _DEFAULT_METRIC_INTERVAL}
		return &retval, true
	}
	if createIfAbsent {
		return this.unsafeCreateMetric(spath)
	}
	return nil, false
}

/*
Create a new metrix. This is an unsafe function

It neither checks if the metric already exists nor is threadsafe
*/
func (this *indices) unsafeCreateMetric(smetric []byte) (*metricIndex, bool) {
	metric := string(smetric)

	retval := true
	var idx *metricIndex

	parts := strings.Split(metric, ".")
	if retval = this._registerPath(parts); retval {
		idx, retval = this._recordId(metric, smetric)
	} else {
		return nil, false
	}

	return idx, retval
}

/*
Records a given metric's shortcode as part of the creation process

This is an unsafe method; to be called only inside the call stack of
a safe method
*/
func (this *indices) _recordId(metric string, bmetric []byte) (*metricIndex, bool) {
	shortCode := shortenMetricName(bmetric)
	retval := metricIndex{metric, shortCode, _DEFAULT_METRIC_INTERVAL}
	err := this.pkey.Put(this.wo, bmetric, shortCode)
	if err != nil {
		iLogger.Println(err)
	}
	return &retval, err == nil
}

/*
Makes the directory entries for a new metric.

This is an unsafe method; to be called only inside the call stack of
a safe method
*/
func (this *indices) _registerPath(parts []string) bool {
	n := len(parts)
	if n == 0 {
		return true
	}

	parent_chunk := parts[:n-1]
	parent := strings.Join(parent_chunk, ".")

	if siblings := this._lsPath([]byte(parent)); siblings == nil {
		if ok := this._registerPath(parent_chunk); !ok {
			return false
		} else {
			return this._addChild(parent, parts[n-1], siblings)
		}
	} else {
		return this._addChild(parent, parts[n-1], siblings)
	}
}

/*
Internal method for fetching uninterpreted bytes stored with a
directory node
*/
func (this *indices) _lsPath(spath []byte) []byte {
	val, err := this.dir.Get(this.ro, spath)
	if err != nil {
		iLogger.Println(err)
		return nil
	}
	return val
}

/*
Attaches a child node to a given directory path

This is an unsafe method; to be called only inside the call stack of
a safe method
*/
func (this *indices) _addChild(parent string, child string, siblings_as_bytes []byte) bool {
	var siblings []string
	if len(siblings_as_bytes) == 0 {
		siblings = []string{}
	} else {
		if err := json.Unmarshal(siblings_as_bytes, &siblings); err != nil {
			iLogger.Println(err, siblings_as_bytes, len(siblings_as_bytes))
			return false
		}
	}

	for _, c := range siblings {
		if c == child {
			return true
		}
	}
	siblings = append(siblings, child)
	return this._replaceChildren(parent, siblings)
}

/* Ultra low level function; super unsafe, used to do an inplace update of
data associated with a directory node */
func (this *indices) _replaceChildren(parent string, children []string) bool {
	var val []byte
	if children == nil || len(children) == 0 {
		val = _EMPTY_JSON_ARRAY_BYTES
	} else {
		val, _ = json.Marshal(children)
	}
	iLogger.Printf("child-setter #%s#  ----> %s aka %s\n", parent, children, val)
	err := this.dir.Put(this.wo, []byte(parent), val)
	if err != nil {
		iLogger.Println(err)
	}
	return err == nil
}

/* Create a directory entry for the root path */
func (this *indices) _mkRoot() {
	var empty []string
	if curr, _ := this.dir.Get(this.ro, nil); curr == nil {
		ok := this._replaceChildren("", empty)
		if !ok {
			iLogger.Panicln("Cannot initialize dir db")
		}
	}
}

func (this *indices) release() {
	this.dir.Close()
	this.pkey.Close()
	this.ro.Close()
	this.wo.Close()
	this.cache.Close()
}

/* Carbon daemon has a long history of characters unacceptable in an metric
name. This function strips all such characters. The list stems from the
early design of having each metric be recorded as a file on disk with the
dots being used as a delimiter for mapping directory names

We have thrown in the additional restriction of not having consequtive dots
in a metric name as the UI (graphite-web) cannot deal with it.

This method was origially implemented using 2 simpled regexs but they were
widly CPU intensive to the point wherein it was taking more that 50% of the
compute resources used by the entire runing program. Hence, it has been
replaced with hand-rolled code
*/
func scrubMetric(s string) []byte {
	retval := make([]byte, 0, len(s))
	i := 0
	b := []byte(s)
	for _, x := range b {
		if x == byte('.') {
			if i != 0 {
				if retval[i-1] == '.' {
					continue
				}
			}
		} else if bytes.IndexByte(_BANNED_BYTES, x) != -1 {
			continue
		}
		retval = append(retval, x)
		i++
	}
	r := retval[0:i]
	return r
}

/* Returns the parent path name of a given metric */
func getParent(path string) string {
	if i := strings.LastIndex(path, "."); i == -1 {
		return ""
	} else {
		return path[:i]
	}
}

/* The "mkfs" for a given directory db */
func mkIndex(root string) (*indices, error) {
	retval := new(indices)

	retval.writeLock = new(sync.Mutex)

	opts := levigo.NewOptions()
	defer opts.Close()

	opts.SetCreateIfMissing(true)

	cache := levigo.NewLRUCache(INDEX_CACHE_SIZE)
	opts.SetCache(cache)

	defer func() {
        if r := recover(); r != nil {
			if cache != nil {
				cache.Close()
			}
        }
    }()

	var err error

	if retval.dir, err = levigo.Open(root+"/tsd-dir.db", opts); err != nil {
		iLogger.Panicf("Error opening directory map %s", err.Error())
	}
	if retval.pkey, err = levigo.Open(root+"/tsd-map.db", opts); err != nil {
		iLogger.Panicf("Error opening index map %s", err.Error())
	}

	retval.wo = levigo.NewWriteOptions()
	retval.ro = levigo.NewReadOptions()

	retval._mkRoot()

	retval.cache = cache

	return retval, err
}
