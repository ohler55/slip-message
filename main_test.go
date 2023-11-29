// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

var (
	jetstreamURL    string
	jetstreamServer *server.Server
)

func TestMain(m *testing.M) {
	status := wrapRun(m)

	os.Exit(status)
}

func wrapRun(m *testing.M) (status int) {
	var jss *server.Server
	jss, jetstreamURL = startJetStreamServer()
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("*-*-* panic: %s\n", rec)
			status = 1
		}
		if jss != nil {
			defer jss.Shutdown()
		}
	}()
	fmt.Printf("*** %s\n", jetstreamURL)
	time.Sleep(time.Minute * 5)
	status = m.Run()

	return
}

func startJetStreamServer() (jss *server.Server, ju string) {
	var (
		err     error
		options = server.Options{
			Host:     "127.0.0.1",
			Port:     availablePort(),
			NoLog:    true,
			NoSigs:   true,
			Users:    []*server.User{{Username: "foo", Password: "bar"}},
			Username: "foo",
			Password: "bar",
			// NoAuthUser:  "foo",
			AllowNonTLS: true,
		}
	)
	if jss, err = server.NewServer(&options); err != nil {
		panic(err)
	}
	if err = jss.EnableJetStream(nil); err != nil {
		panic(err)
	}
	jss.Start()
	ju = jss.ClientURL()

	if !jss.ReadyForConnections(time.Second * 30) {
		panic(fmt.Sprintf("failed to connect to JetStream server on %s", ju))
	}
	return
}

func availablePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
