// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

// The jetstream-hub is primarily for testing. As such no effort has been made to
// optimize encoding and decoding but instead encoding is setup to be similar
// if not the same as remote message hubs but always encoding as a string.

var (
	jetstreamHubFlavor *flavors.Flavor
)

type jsHub struct {
	js      nats.JetStreamContext
	nc      *nats.Conn
	subs    []*jsSub
	mu      sync.Mutex // for subs list
	errCb   slip.Caller
	lastID  int64
	pending map[int64]*nats.Msg
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
	jetstreamHubFlavor.DefMethod(":set-error-handler", "", jetstreamHubSetErrorHandlerCaller{})
}

func (jh *jsHub) addMsgPending(m *nats.Msg) int64 {
	id := time.Now().UnixNano()
	jh.mu.Lock()
	if id <= jh.lastID {
		id = jh.lastID + 1
	}
	jh.lastID = id
	jh.pending[id] = m
	jh.mu.Unlock()

	return id
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
	jh := jsHub{pending: map[int64]*nats.Msg{}}
	var err error
	jh.nc, err = nats.Connect(nu, options...)
	checkError("nats.Connect", err)

	jh.js, err = jh.nc.JetStream()
	checkError("nats.Conn.JetStream", err)

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
		jsub    jsSub
		err     error
		subject string
	)
	subscriber, jsub.sub, subject = subscriberFromArgs(self, args)
	jh := self.Any.(*jsHub)
	jh.mu.Lock()
	jh.subs = append(jh.subs, &jsub)
	if jsub.sub.callback == nil {
		var opts []nats.SubOpt
		opts = append(opts, nats.AckExplicit())
		opts = append(opts, nats.ConsumerName(jsub.sub.name))
		if si, err := jh.js.StreamInfo(subject); err != nil && si != nil {
			opts = append(opts, nats.BindStream(si.Config.Name))
		}
		jsub.nsub, err = jh.js.PullSubscribe(subject, jsub.sub.name, opts...)
	} else {
		jsub.nsub, err = jh.nc.Subscribe(subject, func(m *nats.Msg) {
			if serr := callMsgCallback(s, m, &jsub); serr != nil && jh.errCb != nil {
				_ = safeCall(s, jh.errCb, slip.List{serr})
			}
		})
	}
	jh.mu.Unlock()
	checkError("jetstream hub :subscribe", err)

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
			if subjectMatch(jsub.sub.subject, subject) {
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
		if len(subject) == 0 || subjectMatch(subject, jsub.sub.subject) {
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
	err := jh.nc.Publish(subject, []byte(msg.(slip.String)))
	checkError("jetstream hub :publish", err)

	return nil
}

func (caller jetstreamHubPublishCaller) Docs() string {
	return publishDocs
}

type jetstreamHubRequestCaller struct{}

func (caller jetstreamHubRequestCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, subject, msg, timeout := getRequestMsg(s, args)
	jh := self.Any.(*jsHub)

	m, err := jh.nc.Request(subject, []byte(msg.(slip.String)), timeout)
	checkError("jetstream hub :request", err)

	return decodeMessage(slip.String(m.Data), nil)
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
	self, name, all, maxMsgs, consumers, subjects := getAddQueueArgs(s, args)
	jh := self.Any.(*jsHub)

	cfg := nats.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: nats.WorkQueuePolicy,
	}
	if all {
		cfg.Retention = nats.InterestPolicy
	}
	if 0 < maxMsgs {
		cfg.MaxMsgs = int64(maxMsgs)
	}
	_, err := jh.js.AddStream(&cfg)
	checkError("jetstream hub :add-queue", err)

	for _, cn := range consumers {
		_, err := jh.js.AddConsumer(name,
			&nats.ConsumerConfig{Durable: cn, Name: cn, AckPolicy: nats.AckExplicitPolicy},
		)
		checkError("jetstream hub :add-queue consumer", err)
	}
	return nil
}

func (caller jetstreamHubAddQueueCaller) Docs() string {
	return addQueueDocs
}

type jetstreamHubQueuesCaller struct{}

func (caller jetstreamHubQueuesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)

	var list slip.List
top:
	for stream := range jh.js.Streams() {
		var (
			sa  slip.List
			ret slip.Object
		)
		switch stream.Config.Retention {
		case nats.InterestPolicy:
			ret = slip.Symbol(":all")
		case nats.WorkQueuePolicy:
			ret = slip.Symbol(":work")
		default:
			continue top
		}
		sa = append(sa, slip.List{slip.Symbol("name"), slip.Tail{Value: slip.String(stream.Config.Name)}})
		sa = append(sa, slip.List{slip.Symbol("retention"), slip.Tail{Value: ret}})
		sa = append(sa, slip.List{slip.Symbol("queued"), slip.Tail{Value: slip.Fixnum(stream.State.Msgs)}})
		var ca slip.List
		ca = append(ca, slip.Symbol("consumers"))
		for c := range jh.js.ConsumerNames(stream.Config.Name) {
			ca = append(ca, slip.String(c))
		}
		sa = append(sa, ca)
		var subjects slip.List
		subjects = append(subjects, slip.Symbol("subjects"))
		for _, subj := range stream.Config.Subjects {
			subjects = append(subjects, slip.String(subj))
		}
		sa = append(sa, subjects)

		list = append(list, sa)
	}
	return list
}

func (caller jetstreamHubQueuesCaller) Docs() string {
	return queuesDocs
}

type jetstreamHubCloseQueueCaller struct{}

func (caller jetstreamHubCloseQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)
	if ss, ok := args[0].(slip.String); ok {
		_ = jh.js.DeleteStream(string(ss))
	} else {
		slip.PanicType("name", args[0], "string")
	}
	return nil
}

func (caller jetstreamHubCloseQueueCaller) Docs() string {
	return closeQueueDocs
}

type jetstreamHubNextCaller struct{}

func (caller jetstreamHubNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, sub, timeout := getNextArgs(s, args)
	jh := self.Any.(*jsHub)

	var jsub *jsSub
	jh.mu.Lock()
	for _, sb := range jh.subs {
		if sub == sb.sub {
			jsub = sb
			break
		}
	}
	jh.mu.Unlock()
	ma, err := jsub.nsub.Fetch(1, nats.MaxWait(timeout))
	checkError("jetstream hub :next", err)

	if 0 < len(ma) {
		mid := jh.addMsgPending(ma[0])
		return slip.Values{decodeMessage(slip.String(ma[0].Data), jsub.sub.contentType), slip.Fixnum(mid)}
	}
	return nil
}

func (caller jetstreamHubNextCaller) Docs() string {
	return nextDocs
}

type jetstreamHubAckCaller struct{}

func (caller jetstreamHubAckCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, _, mid := getAckArgs(s, args)
	jh := self.Any.(*jsHub)
	jh.mu.Lock()
	m, has := jh.pending[mid]
	if has {
		delete(jh.pending, mid)
	}
	jh.mu.Unlock()
	if m != nil {
		err := m.Ack()
		checkError("jetstream hub :ack", err)
	}
	return nil
}

func (caller jetstreamHubAckCaller) Docs() string {
	return ackDocs
}

type jetstreamHubSetErrorHandlerCaller struct{}

func (caller jetstreamHubSetErrorHandlerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) != 1 {
		slip.NewPanic("Incorrect argument count. Expected 1 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	jh := self.Any.(*jsHub)
	jh.errCb = cl.ResolveToCaller(s, args[0], 0)

	return nil
}

func (caller jetstreamHubSetErrorHandlerCaller) Docs() string {
	return `__:set-error-handler__ _handler__ => _nil_
   _handler_ is the function to call when an out of band error occurrs.


Sets the error handler for message processing errors.
`
}
