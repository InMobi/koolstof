package leveltsd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleOpen(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	t.Log("Test dir is " + dir)

	mkIndex(dir)
}

func TestInexistantParentsChild(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	children := index.listChildern("foobar")
	n := len(children)
	assert.Equal(t, n, 0, "child count mismatch")
}

func TestToplevelCreate(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo"
	var ok bool

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric, false)
	assert.True(t, ok, "fetch post create failed")

	children := index.listChildern("")
	n := len(children)
	assert.Equal(t, n, 1, "child count mismatch")
}

func TestSecondlevelOneshotCreate(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "aieee.panic"
	var ok bool
	var n int
	var children []string

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric, false)
	assert.True(t, ok, "fetch post create failed")

	children = index.listChildern("")
	n = len(children)
	assert.Equal(t, n, 1, "child count mismatch")

	children = index.listChildern("aieee")
	n = len(children)
	assert.Equal(t, n, 1, "child count mismatch")

	children = index.listChildern("aie")
	n = len(children)
	assert.Equal(t, n, 0, "child count mismatch")
}

func TestSecondlevelTwoshotCreate(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric1 := "animal.cat"
	new_metric2 := "animal.dog"
	var ok bool
	var n int
	var children []string

	_, ok = index.getMetric(new_metric1, true)
	assert.True(t, ok, "new metric creation failed")
	_, ok = index.getMetric(new_metric2, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric2, false)
	assert.True(t, ok, "fetch post create failed")

	children = index.listChildern("")
	n = len(children)
	assert.Equal(t, n, 1, "child count mismatch")

	children = index.listChildern("animal")
	n = len(children)
	assert.Equal(t, n, 2, "child count mismatch")

	children = index.listChildern(new_metric1)
	n = len(children)
	assert.Equal(t, n, 0, "child count mismatch")

	children = index.listChildern(new_metric2)
	n = len(children)
	assert.Equal(t, n, 0, "child count mismatch")
}

func TestCreationIdempotence(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "idem.pot.ence"
	var ok bool

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric, false)
	assert.True(t, ok, "fetch post create failed")

	children := index.listChildern("")
	n := len(children)
	assert.Equal(t, n, 1, "child count mismatch")

	children = index.listChildern("idem")
	n = len(children)
	assert.Equal(t, n, 1, "child count mismatch")

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric fetch failed")

	children = index.listChildern("idem")
	n = len(children)
	assert.Equal(t, n, 1, "child count mismatch")
}

func TestMultidot2(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo..bar"
	var ok bool

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric, false)
	assert.True(t, ok, "dot scrub failed; unscrubbed value found")

	_, ok = index.getMetric("foo.bar", false)
	assert.True(t, ok, "dot scrub failed; scrubbed value not found")

	children := index.listChildern("foo")
	n := len(children)
	assert.Equal(t, n, 1, "child count mismatch")
}

func TestMultidot5(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "foo.....bar"
	var ok bool

	_, ok = index.getMetric(new_metric, true)
	assert.True(t, ok, "new metric creation failed")

	_, ok = index.getMetric(new_metric, false)
	assert.True(t, ok, "dot scrub failed; unscrubbed value found")

	_, ok = index.getMetric("foo.bar", false)
	assert.True(t, ok, "dot scrub failed; scrubbed value not found")

	children := index.listChildern("foo")
	n := len(children)
	assert.Equal(t, n, 1, "child count mismatch")
}

func TestBan(t *testing.T) {
	dir, cleanup := _makeDir()
	defer cleanup()
	index, err := mkIndex(dir)
	assert.Nil(t, err)

	new_metric := "/"
	var ok bool

	_, ok = index.getMetric(new_metric, true)
	assert.False(t, ok, "new metric creation succeeded when it was supposed to fail")

	children := index.listChildern("")
	n := len(children)
	assert.Equal(t, n, 0, "child count mismatch")
}
