package leveltsd

import (
	"github.com/stretchr/testify/assert"
	"inmobi.com/graphite/carbon/mq"
	"testing"
)

func TestPluginWrite(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()

	var plugin LevelDbStorage
	config := make(map[string]string)

	config["root"] = dir

	plugin.Init(config)
	m := "foo.baz"
	x, ok := plugin.CreateMetric(m)
	assert.True(t, ok)

	ok = plugin.UncheckedWrite(x, mq.MetricReading{m, float64(1324.12), uint64(324323)})
	assert.True(t, ok)
}
