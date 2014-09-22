package main

import (
	"flag"
	"github.com/vaughan0/go-ini"

	"inmobi.com/graphite/carbon/assembly"

	// import storage modules
	_ "inmobi.com/graphite/carbon/devnull"
	_ "inmobi.com/graphite/carbon/leveltsd"
	_ "net/http/pprof"
)

func main() {
	config := flag.String("c", "", "config file path")
	flag.Parse()

	if *config == "" {
		panic("Config file is unspecified")
	}

	file, err := ini.LoadFile(*config)
	if err != nil {
		panic("Error reading config file " + err.Error())
	}

	assembly.BuildallAndRun(file)

	// dummy wait
	e := make(chan bool)
	<-e
}
