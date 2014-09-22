package leveltsd

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var defcon = defaultShardConfig()

func TestSimpleWrite(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ok = s.insert(key, 1, 1)
	assert.True(t, ok)
}

func TestWritePostClose(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)

	ok = s.insert(key, 1, 1)
	assert.True(t, ok)

	s.release()

	ok = s.insert(key, 2, 2)
	assert.False(t, ok)
}

func TestOpenMissing(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	_, err := mkShard(dir+"/mt", false, defcon)
	assert.NotNil(t, err)
}

func TestOnePointScan(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := uint64(92)
	val := 3.4
	ok = s.insert(key, ts, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	start_time := uint64(1)
	end_time := uint64(1000)
	res := s.dataScan(key, start_time, end_time)
	assert.Equal(t, len(res), 1)

	adjustedTs := rounder(ts, _DEFAULT_METRIC_INTERVAL)

	assert.Equal(t, res[0].Timestamp, adjustedTs)
	assert.Equal(t, res[0].Value, val)
	assert.True(t, adjustedTs >= start_time, "lower bound check")
	assert.True(t, adjustedTs <= end_time, "upper bound check")
}

func TestNonePointScan(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := uint64(1)
	val := 3.4
	ok = s.insert(key, ts, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	start_time := uint64(1)
	end_time := uint64(1000)
	res := s.dataScan(key, start_time, end_time)
	assert.Equal(t, len(res), 0)
}

func TestPointSqueeze(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := uint64(92)
	val := 3.4
	ok = s.insert(key, ts, val)
	ok = s.insert(key, ts+1, val)
	ok = s.insert(key, ts+2, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	res := s.dataScan(key, 1, 1000)
	assert.Equal(t, len(res), 1)

	assert.Equal(t, res[0].Timestamp, rounder(ts, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[0].Value, val)
}

func TestFarFutureSqueeze(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := uint64(254054185267)
	val := 3.4
	ok = s.insert(key, ts, val)
	ok = s.insert(key, ts+1, val)
	ok = s.insert(key, ts+2, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	start_time := ts - _DEFAULT_METRIC_INTERVAL*3
	end_time := ts + _DEFAULT_METRIC_INTERVAL*3
	res := s.dataScan(key, start_time, end_time)
	assert.Equal(t, len(res), 1)

	assert.Equal(t, res[0].Timestamp, rounder(ts, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[0].Value, val)
}

func TestShardEdgeScan(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := rounder(uint64(63432328978), _DEFAULT_METRIC_INTERVAL)
	val := 3.4
	ok = s.insert(key, ts, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	res := s.dataScan(key, ts, ts)
	assert.Equal(t, len(res), 1)

	assert.Equal(t, res[0].Timestamp, ts)
	assert.Equal(t, res[0].Value, val)
}

func TestWideScan(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts := uint64(238923)
	val := 3.4
	ok = s.insert(key, ts, val)
	assert.True(t, ok)

	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	res := s.dataScan(key, 1, 1388134161)
	assert.Equal(t, len(res), 1)

	assert.Equal(t, res[0].Timestamp, rounder(ts, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[0].Value, val)
}

func TestMultiQuantumWrite(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.bar"

	key, ok := index.getMetric(new_metric, true)
	assert.True(t, ok)

	s, err := mkShard(dir+"/mt", true, defcon)
	assert.Nil(t, err)
	defer s.release()

	ts1 := uint64(92347893)
	ts2 := ts1 + 3*_DEFAULT_METRIC_INTERVAL
	ts3 := ts1 - 2*_DEFAULT_METRIC_INTERVAL

	val1 := 3.4
	val2 := 92.1
	val3 := 532.132

	ok = s.insert(key, ts1, val1)
	assert.True(t, ok)
	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	ok = s.insert(key, ts2, val2)
	assert.True(t, ok)
	time.Sleep(defcon.Write_batch_fill_timeout / 2) // This is a bit hokey

	ok = s.insert(key, ts3, val3)
	assert.True(t, ok)
	time.Sleep(defcon.Write_batch_fill_timeout * 3 / 2) // This is a bit hokey

	res := s.dataScan(key, 1, 2000000000)
	assert.Equal(t, len(res), 3)

	assert.Equal(t, res[0].Timestamp, rounder(ts3, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[0].Value, val3)
	assert.Equal(t, res[1].Timestamp, rounder(ts1, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[1].Value, val1)
	assert.Equal(t, res[2].Timestamp, rounder(ts2, _DEFAULT_METRIC_INTERVAL))
	assert.Equal(t, res[2].Value, val2)
}

func TestCreateFail(t *testing.T) {
	for i := 0; i < 1000 * 1000; i++ {
		_, err := mkShard("/this-should-not-work/foo/bar", true, defcon)
		assert.NotNil(t, err)
	}
}
