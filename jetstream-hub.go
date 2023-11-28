// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

// The jetstream-hub is primarily for testing. As such no effort has been made to
// optimize encoding and decoding but instead encoding is setup to be similar
// if not the same as remote message hubs but always encoding as a string.

var (
	jetstreamHubFlavor *flavors.Flavor
)

type jsHub struct {
	js   nats.JetStream
	nc   *nats.Conn
	subs []*jsSub
	mu   sync.Mutex // for subs list
}

func init() {
	jetstreamHubFlavor = flavors.DefFlavor("jetstream-hub-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":init-keywords"),
				slip.Symbol(":url"),
				slip.Symbol(":credentials"),
				slip.Symbol(":tls-ca"),
				slip.Symbol(":tls-cert"),
				slip.Symbol(":tls-key"),
			},
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A jetstream-hub is connection to a JetStream server.`),
			},
		},
	)
	jetstreamHubFlavor.DefMethod(":init", "", jetstreamHubInitCaller{})
	jetstreamHubFlavor.DefMethod(":subscribe", "", jetstreamHubSubscribeCaller{})
	jetstreamHubFlavor.DefMethod(":unsubscribe", "", jetstreamHubUnsubscribeCaller{})
	jetstreamHubFlavor.DefMethod(":subscribers", "", jetstreamHubSubscribersCaller{})
	jetstreamHubFlavor.DefMethod(":publish", "", jetstreamHubPublishCaller{})
	jetstreamHubFlavor.DefMethod(":request", "", jetstreamHubRequestCaller{})
	jetstreamHubFlavor.DefMethod(":close", "", jetstreamHubCloseCaller{})
	jetstreamHubFlavor.DefMethod(":add-queue", "", jetstreamHubAddQueueCaller{})
	jetstreamHubFlavor.DefMethod(":close-queue", "", jetstreamHubCloseQueueCaller{})
	jetstreamHubFlavor.DefMethod(":queues", "", jetstreamHubQueuesCaller{})
	jetstreamHubFlavor.DefMethod(":next", "", jetstreamHubNextCaller{})
	jetstreamHubFlavor.DefMethod(":ack", "", jetstreamHubAckCaller{})
}

type jetstreamHubInitCaller struct{}

func (caller jetstreamHubInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	nu := nats.DefaultURL
	var (
		options []nats.Option
		tlsCert string
		tlsKey  string
	)
	for i := 0; i < len(args)-1; i += 2 {
		key, _ := args[i].(slip.Symbol)
		value, ok := args[i+1].(slip.String)
		if !ok {
			slip.PanicType(string(key), args[i+1], "string")
		}
		switch string(key) {
		case ":url":
			nu = string(value)
		case ":credentials":
			options = append(options, nats.UserCredentials(string(value)))
		case ":tls-ca":
			options = append(options, nats.RootCAs(string(value)))
		case ":tls-cert":
			tlsCert = string(value)
		case ":tls-key":
			tlsKey = string(value)
		default:
			slip.PanicType("initializer key", args[i], ":url", ":credentials", ":tls-ca", ":tls-cert", ":tls-key")
		}
	}
	if 0 < len(tlsCert) || 0 < len(tlsKey) {
		options = append(options, nats.ClientCert(tlsCert, tlsKey))
	}
	var (
		jh  jsHub
		err error
	)
	if jh.nc, err = nats.Connect(nu, options...); err != nil {
		panic(err)
	}
	if jh.js, err = jh.nc.JetStream(); err != nil {
		panic(err)
	}
	self.Any = &jh

	return nil
}

func (caller jetstreamHubInitCaller) Docs() string {
	return `__:init__ &key _url_ _credentials_ _tls-ca_ _tls-cert_ _tls-key_
   _:url_ of the jetstream server
   _:credentials_ for the jetstream connection
   _:tls-ca_ for the jetstream connection
   _:tls-cert_ for the jetstream connection
   _:tls-key_ for the jetstream connection


Sets the initial values when _make-instance_ is called.
`
}

type jetstreamHubSubscribeCaller struct{}

func (caller jetstreamHubSubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)

	var (
		jsub jsSub
		err  error
	)
	subscriber, jsub.sub = subscriberFromArgs(self, args)
	jsub.filter = strings.Split(jsub.sub.subject, ".")
	jh := self.Any.(*jsHub)
	jh.mu.Lock()
	jh.subs = append(jh.subs, &jsub)
	jsub.nsub, err = jh.nc.Subscribe(jsub.sub.subject, func(m *nats.Msg) {
		msg := decodeMessage(slip.String(m.Data), jsub.sub.contentType)
		if jsub.sub.callback != nil {
			_ = jsub.sub.callback.Call(s, slip.List{msg}, 0)
		}
	})
	jh.mu.Unlock()
	if err != nil {
		panic(err)
	}
	return
}

func (caller jetstreamHubSubscribeCaller) Docs() string {
	return subscribeDocs
}

type jetstreamHubUnsubscribeCaller struct{}

func (caller jetstreamHubUnsubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)
	var removed []*jsSub
	switch ts := args[0].(type) {
	case slip.String:
		var subs []*jsSub
		subject := strings.Split(string(ts), ".")
		jh.mu.Lock()
		for _, jsub := range jh.subs {
			if subjectMatch(jsub.filter, subject) {
				removed = append(removed, jsub)
				continue
			}
			subs = append(subs, jsub)
		}
		jh.subs = subs
		jh.mu.Unlock()
	case *flavors.Instance:
		jh.mu.Lock()
		for i, jsub := range jh.subs {
			if jsub.sub.self == ts {
				copy(jh.subs[i:], jh.subs[i+1:])
				jh.subs = jh.subs[:len(jh.subs)-1]
				removed = append(removed, jsub)
				break
			}
		}
		jh.mu.Unlock()
	}
	for _, jsub := range removed {
		_ = jsub.nsub.Unsubscribe()
	}
	return slip.Fixnum(len(removed))
}

func (caller jetstreamHubUnsubscribeCaller) Docs() string {
	return unsubscribeDocs
}

type jetstreamHubSubscribersCaller struct{}

func (caller jetstreamHubSubscribersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)
	var (
		subs    slip.List
		subject []string
	)
	if 0 < len(args) {
		if ss, ok := args[0].(slip.String); ok {
			subject = strings.Split(string(ss), ".")
		} else {
			slip.PanicType("subject", args[0], "string")
		}
	}
	jh.mu.Lock()
	for _, jsub := range jh.subs {
		if len(subject) == 0 || subjectMatch(subject, jsub.filter) {
			subs = append(subs, jsub.sub.self)
		}
	}
	jh.mu.Unlock()

	return subs
}

func (caller jetstreamHubSubscribersCaller) Docs() string {
	return subscribersDocs
}

type jetstreamHubPublishCaller struct{}

func (caller jetstreamHubPublishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 2 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	if len(args) < 2 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 2 or 3 but got %d.", len(args))
	}
	var (
		subject string
		msg     slip.Object
	)
	if ss, ok := args[0].(slip.String); ok {
		subject = string(ss)
	} else {
		slip.PanicType("subject", args[0], "string")
	}
	msg = encodeMsg(args[1], 2 < len(args) && args[2] == slip.Symbol(":sen"))
	if err := jh.nc.Publish(subject, []byte(msg.(slip.String))); err != nil {
		panic(err)
	}
	return nil
}

func (caller jetstreamHubPublishCaller) Docs() string {
	return publishDocs
}

type jetstreamHubRequestCaller struct{}

func (caller jetstreamHubRequestCaller) Call(s *slip.Scope, args slip.List, _ int) (reply slip.Object) {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected at least 2 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD jh.nc.Request(subject, msg, timeout)
	fmt.Printf("*** js: %v\n", jh)

	return
}

func (caller jetstreamHubRequestCaller) Docs() string {
	return requestDocs
}

type jetstreamHubCloseCaller struct{}

func (caller jetstreamHubCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any.(*jsHub).nc.Close()

	return nil
}

func (caller jetstreamHubCloseCaller) Docs() string {
	return closeDocs
}

type jetstreamHubAddQueueCaller struct{}

func (caller jetstreamHubAddQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 3 || 4 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD
	fmt.Printf("*** js: %v\n", jh)

	return nil
}

func (caller jetstreamHubAddQueueCaller) Docs() string {
	return addQueueDocs
}

type jetstreamHubQueuesCaller struct{}

func (caller jetstreamHubQueuesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD
	fmt.Printf("*** js: %v\n", jh)

	return nil
}

func (caller jetstreamHubQueuesCaller) Docs() string {
	return queuesDocs
}

type jetstreamHubCloseQueueCaller struct{}

func (caller jetstreamHubCloseQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD
	fmt.Printf("*** js: %v\n", jh)

	return nil
}

func (caller jetstreamHubCloseQueueCaller) Docs() string {
	return closeQueueDocs
}

type jetstreamHubNextCaller struct{}

func (caller jetstreamHubNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 1 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 1 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD
	fmt.Printf("*** js: %v\n", jh)

	return nil
}

func (caller jetstreamHubNextCaller) Docs() string {
	return nextDocs
}

type jetstreamHubAckCaller struct{}

func (caller jetstreamHubAckCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected 2 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	// TBD
	fmt.Printf("*** js: %v\n", jh)

	return nil
}

func (caller jetstreamHubAckCaller) Docs() string {
	return ackDocs
}
