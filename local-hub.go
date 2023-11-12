// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"strings"
	"sync"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	localHubFlavor *flavors.Flavor
)

type localSub struct {
	filter []string
	sub    *subscription
	// TBD maybe channel for messages
}

type localHub struct {
	subs []*localSub
	moo  sync.Mutex // for subs list as well as distribution
}

func init() {
	localHubFlavor = flavors.DefFlavor("local-hub-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A local-hub is an in-memory message distribution hub.`),
			},
		},
	)
	localHubFlavor.DefMethod(":init", "", localHubInitCaller{})
	localHubFlavor.DefMethod(":subscribe", "", localHubSubscribeCaller{})
	// localHubFlavor.DefMethod(":unsubscribe", "", localHubUnsubscribeCaller{})
	// localHubFlavor.DefMethod(":subscribers", "", localHubSubscribersCaller{})
	// localHubFlavor.DefMethod(":publish", "", localHubPublishCaller{})
	// localHubFlavor.DefMethod(":request", "", localHubRequestCaller{})
	// localHubFlavor.DefMethod(":configure-subject", "", localHubConfigureSubjectCaller{})
	// localHubFlavor.DefMethod(":close", "", localHubCloseCaller{})
}

type localHubInitCaller struct{}

func (caller localHubInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any = &localHub{}

	return nil
}

func (caller localHubInitCaller) Docs() string {
	return `__:init__

Sets the initial values when _make-instance_ is called.
`
}

type localHubSubscribeCaller struct{}

func (caller localHubSubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	lh := self.Any.(*localHub)
	var ct slip.Object
	if 2 < len(args) {
		ct = args[2]
	}
	var sub *subscription
	subscriber, sub = makeSubscriber(self, args[0], args[1], ct)
	ls := localSub{filter: strings.Split(sub.subject, ".")}
	lh.moo.Lock()
	lh.subs = append(lh.subs, &ls)
	lh.moo.Unlock()

	return
}

func (caller localHubSubscribeCaller) Docs() string {
	return `__:subscribe__ _subject_ _callback_ &optional _content-type_ => _instance_
   _subject_ to listen on.
   _callback_ can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.
   _content-type_is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.

Returns a _subscriber-flavor_ instance that represents a subscription on the _subject_.
`
}
