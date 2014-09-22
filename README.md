# koolstof

golang implementation of the carbon server. Koolstof is the [Dutch word for carbon](http://nl.wikipedia.org/wiki/Koolstof) and coincidentally also has a nice sounding ring to it in English.

## Genesis
The storage format using leveldb as seen [here](https://github.com/InMobi/level-tsd) served us well until we ran into the limitations of cpython as a runtime; most notably the poor support for concurrency and also the inherent limination in not being able to use all CPU cores from within the same \*nix process. Hence, the solution was to retain the carbon protocol, the storage layer schema as seen in the python+leveldb plugin and just have the carbon daemon written in a systems programming language.

## Dependencies
We rely on the following golang modules
* github.com/jmhodges/levigo
* github.com/gorilla/rpc
* github.com/extemporalgenome/epochdate
* github.com/vaughan0/go-ini

The preferred way to use leveldb is by using a statically linked version of levigo. The script for the static build can be found [here](https://github.com/inmobi/static-levigo)

## Limitations
* Does not support aggregatation of metrics over time intervals. This will not be fixed by design as loss of accurrary of data due to aggregations caused by timeseries systems is a bad thing.
* Assumes that the resolution of all metrics is multiples of 60 seconds
* Supports only plaintext format
* Supports only TCP listener

## Configuration example
```ini
GOMAXPROCS=2


[listener]
; TCP listen port for carbon plaintext format data
port = 3540


[storage]
; Per minute rate limit on number of metrics that can be recorded
max_write_rpm = 3000000 

; Per minute rate limit on number of metrics that can be create
max_create_rpm = 100000

; In memory buffer size of the number of metrics that can be enqueued when storage media is busy
; Exceeding this limit results in metrics being discarded
backlog = 10000000

; Use the leveldb based timeseries storage.
engine = leveltsd


; storage engine specific configuration
[storage-engine]
; Filesystem root where all metrics related data is stored
root = /home2/tmp/carbon

; RPC for the graphite finder to communicate. See https://github.com/InMobi/level-tsd-finder
reader-port = 8080

; OPTIONAL VALUES
; Batch writer size for leveldb
write-batch-count = 20000

; data partition cache size in bytes
memory-cache = 134217728

; max number of channels that are enqueing microbatch writes
write-concurrency = 3

; idle time after which a micro-batch is flushed
write-batch-interval-seconds = 11
```

## How to run it
You can either run from source or use the binary build
### Run from source
go run inmobi.com/graphite/carbon -c _path-to-config_
### Binary build
go install inmobi.com/graphite/carbon
bin/carbon -c _path-to-config_

## Meta-metrics compatibility
* CPU & memory usage on the host running the daemon is _not_ recorded
* *cache_queries* and *datapoints_per_write* are discontinued as they do not make sense for this implementation
* We have introduced a new value known as *garbled_reception* to capture malformed lines received by the daemon
