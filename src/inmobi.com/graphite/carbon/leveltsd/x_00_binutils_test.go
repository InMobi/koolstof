package leveltsd

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValReversibility(t *testing.T) {
	x := 1.5
	assert.Equal(t, readVal(writeVal(x)), x)
}

func TestTimestampReverisibility(t *testing.T) {
	x := shortenMetricName([]byte("foo.bar"))
	var ts uint64
	ts = 1242
	assert.Equal(t, extractTsFromFullKey(keyString(x, ts)), ts)
}

func TestOrderability(t *testing.T) {
	// values are carefully chosen for endianness breakage
	x := uint64(256*23423932*6 + 67)
	y := uint64(256*23423932*5 + 67)

	m := shortenMetricName([]byte("foo.bar"))

	x1 := keyString(m, x)
	y1 := keyString(m, y)

	assert.Equal(t, bytes.Compare(x1, y1), 1)
}
