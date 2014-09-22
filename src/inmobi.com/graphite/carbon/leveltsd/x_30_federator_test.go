package leveltsd

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"inmobi.com/graphite/carbon/mq"
	"os"
	"regexp"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	config := make(map[string]string)
	config["root"] = dir

	f := buildStorage(config)
	assert.NotNil(t, f)

	f.release()
}

func TestShardCountLongshot(t *testing.T) {
	t1 := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	t2 := time.Date(2019, time.March, 1, 23, 0, 0, 0, time.UTC)

	s := _rangeShards(uint64(t1.Unix()), uint64(t2.Unix()))
	assert.Equal(t, len(s), 3399)

	var validID = regexp.MustCompile(`^[0-9]{8}$`)
	for _, x := range s {
		assert.Equal(t, len(x), 8)
		assert.True(t, validID.MatchString(x))
	}
}

func TestShardCountIntraday(t *testing.T) {
	t1 := time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2009, time.November, 10, 23, 59, 59, 0, time.UTC)

	s := _rangeShards(uint64(t1.Unix()), uint64(t2.Unix()))
	assert.Equal(t, len(s), 1)
}

func TestFederatedQuery(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	new_metric := "École.polytechnique.fédérale.de.Lausanne"

	config := make(map[string]string)
	config["root"] = dir

	federator := buildStorage(config)

	_, ok := federator.createMetric(new_metric)
	assert.True(t, ok)

	key, ok := federator.getMetric(new_metric)
	assert.True(t, ok)

	var datum mq.MetricReading

	datum.Metric = new_metric

	datum.Val = 32.23
	datum.Time = 75
	assert.True(t, federator.uncheckedWrite(key, datum))

	datum.Val = 90232.2
	datum.Time = 1000
	assert.True(t, federator.uncheckedWrite(key, datum))

	datum.Val = 8734.343
	datum.Time = 5000000
	assert.True(t, federator.uncheckedWrite(key, datum))

	time.Sleep(_BATCH_TIME_SECONDS * time.Second * 3 / 2) // This is a bit hokey

	scanStart := uint64(1)
	scanEnd := uint64(1388134161)

	scanned := federator.dataScan(key, scanStart, scanEnd)
	assert.Equal(t, len(scanned), 3)
}

func TestDiscoverExistingShard(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	new_metric := "École.polytechnique.fédérale.de.Lausanne"

	config := make(map[string]string)
	config["root"] = dir

	federator := buildStorage(config)

	key, ok := federator.createMetric(new_metric)
	assert.True(t, ok)

	var datum mq.MetricReading

	datum.Metric = new_metric

	datum.Val = 32.23
	datum.Time = 65

	assert.Nil(t, federator.getShard(datum.Time, false))

	assert.True(t, federator.uncheckedWrite(key, datum))
	time.Sleep(_BATCH_TIME_SECONDS * time.Second * 3 / 2) // This is a bit hokey

	assert.NotNil(t, federator.getShard(datum.Time, false))

	federator.release()

	federator2 := buildStorage(config)
	assert.NotNil(t, federator2.getShard(datum.Time, false), "Checking after re-open")

	scanStart := uint64(1)
	scanEnd := uint64(1388134161)

	scanned := federator2.dataScan(key, scanStart, scanEnd)
	assert.Equal(t, len(scanned), 1)
}

func TestDirListReopen(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	new_metric := "École.polytechnique.fédérale.de.Lausanne"

	config := make(map[string]string)
	config["root"] = dir

	federator := buildStorage(config)

	_, ok := federator.createMetric(new_metric)
	assert.True(t, ok)

	federator.release()

	federator2 := buildStorage(config)
	var c []string

	c = federator2.idx.listChildern("")
	assert.Equal(t, len(c), 1)
	assert.Equal(t, c[0], "École")

	c = federator2.idx.listChildern("École")
	assert.Equal(t, len(c), 1)
	assert.Equal(t, c[0], "polytechnique")
}

func TestShardNocreate(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	new_metric := "École.polytechnique.fédérale.de.Lausanne"

	config := make(map[string]string)
	config["root"] = dir

	federator := buildStorage(config)

	key, ok := federator.createMetric(new_metric)
	assert.True(t, ok)

	t1 := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	t2 := time.Date(2019, time.March, 1, 23, 0, 0, 0, time.UTC)

	start_time := uint64(t1.Unix())
	end_time := uint64(t2.Unix())

	s := _rangeShards(start_time, end_time)
	assert.Equal(t, len(s), 3399)

	for _, x := range s {
		shard_path := _shard_namer(dir, x)
		_, err := os.Stat(shard_path)
		assert.NotNil(t, err)
		assert.True(t, os.IsNotExist(err))
	}

	federator.dataScan(key, start_time, end_time)

	for _, x := range s {
		shard_path := _shard_namer(dir, x)
		_, err := os.Stat(shard_path)
		assert.NotNil(t, err)
		assert.True(t, os.IsNotExist(err))
	}
}

func TestShardOpenfileDos(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	new_metric := "École.polytechnique.fédérale.de.Lausanne"

	config := make(map[string]string)
	config["root"] = dir

	federator := buildStorage(config)

	key, ok := federator.createMetric(new_metric)
	assert.True(t, ok)

	var datum mq.MetricReading

	datum.Metric = new_metric

	datum.Val = 32.23

	// Create years worth of data
	years := 4
	step := 60 * 60
	for i, j := 0, 0; i <= years * 365 * 24 * 60 * 60; i,j = i+step, j+1 {
		datum.Time = uint64(i)
		assert.True(t, federator.uncheckedWrite(key, datum))
		if ((j % 1000) == 0) {
			fmt.Println("iteration ----------------->", j)
		}
	}
}
