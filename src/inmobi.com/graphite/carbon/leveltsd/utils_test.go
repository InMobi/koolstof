package leveltsd

import (
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

type finally func()

func _makeDir() (string, finally) {
	xxx, _, _, _ := runtime.Caller(1)
	f := runtime.FuncForPC(xxx)
	name := strings.Replace(f.Name(), "/", ".", -1)

	dir, err := ioutil.TempDir("", name)
	if err != nil {
		panic("Error creating directory " + name)
	}
	return dir, func() { os.RemoveAll(dir) }
}
