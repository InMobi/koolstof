package leveltsd

import (
	"fmt"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"net"
	"net/http"
)

func make_rpc_server(federator *levelfederator, port uint16) func() {
	readService := new(ReaderService)
	readService.federator = federator

	s := rpc.NewServer()
	codec := json.NewCodec()
	s.RegisterCodec(codec, "application/json")
	s.RegisterCodec(codec, "application/json-rpc")
	s.RegisterService(readService, "")
	http.Handle("/", s)

	l, e := net.Listen("tcp", fmt.Sprintf(":%d", int(port)))
	if e != nil {
		panic(e.Error())
	}

	anon := func() {
		http.Serve(l, nil)
	}
	return anon
}
