package leveltsd

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"inmobi.com/graphite/carbon/logging"
	"log"
)

var bLogger *log.Logger

func init() {
	bLogger = logging.MakeLogger("leveltsd-marshall: ")
}

const SCHEME_MAGIC_ID = "__l3xedfRCTNUI7EFuFIw2CyffG7ggL7h8RE1VtBOrCvVvpdCORvCIRfSc49Zr"
const SERIALIZATION_TECHNIQUE = "struct pack <d"

/* Encode the measurement for writing to KV store */
func writeVal(x float64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, x)
	return buf.Bytes()
}

/* Decode a measurement from writing to KV store */
func readVal(b []byte) float64 {
	var x float64
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.LittleEndian, &x)
	return x
}

/* Build the key for the KV store index

The key is a logical composite of the metric and the time for which
the measurement needs to be recorded. It is also expected to exhibit
ordering capability i.e. compare(ts1, ts2) must be equal to the following:
lexicogrpahical_compare({metric, ts1}, {metric, ts2}) where the metric
is the same and ts1 & ts2 represent the timestamps

Another requirement is that the process of building the key be reversibile
as data (i.e. the timestamp) is actually a part of the key
*/
func keyString(metric_short_code []byte, unixTS uint64) []byte {
	n := len(metric_short_code)
	if n != 16 {
		bLogger.Panicf("Found a short code whose length is not 16; it was %d", n)
	}
	buf := new(bytes.Buffer)
	buf.Write(metric_short_code)
	binary.Write(buf, binary.BigEndian, unixTS)
	retval := buf.Bytes()
	n = len(retval)
	if n != 24 {
		bLogger.Panicf("Made a key whose length is not 24; it was %d", n)
	}
	return retval
}

/* Get a fixed length representation of a metric

A space (& read time) saving measure is to not store the metric name
as-is but instead store a fixed length representation of it. The current
choice is to use the MD5 checksum of the metric's byte representation in
UTF-8. This by definition produces a 16 byte value
*/
func shortenMetricName(s []byte) []byte {
	h := md5.New()
	h.Write(s)
	return h.Sum(nil)
}

func rounder(val uint64, r uint32) uint64 {
	return val / uint64(r) * uint64(r)
}

/* Extracts timestamp from a given key

This technique requires that the scheme used for construction of the key
be the reverse of this procedure. Callers are expected to ensure this
integrity
*/
func extractTsFromFullKey(fk []byte) uint64 {
	n := len(fk)
	if n != 24 {
		bLogger.Panicf("Got a key whose length is not 24; it was %d", n)
	}

	buf := new(bytes.Buffer)
	buf.Write(fk[16:])

	var retval uint64
	binary.Read(buf, binary.BigEndian, &retval)
	return retval
}
