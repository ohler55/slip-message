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
	localHubFlavor.DefMethod(":subscribers", "", localHubSubscribersCaller{})
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
	ls := localSub{filter: strings.Split(sub.subject, "."), sub: sub}
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

type localHubUnsubscribeCaller struct{}

func (caller localHubUnsubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	lh := self.Any.(*localHub)
	var cnt slip.Fixnum
	switch ts := args[0].(type) {
	case slip.String:
		var subs []*localSub
		subject := strings.Split(string(ts), ".")
		lh.moo.Lock()
		for _, ls := range lh.subs {
			if subjectMatch(ls.filter, subject) {
				cnt++
				continue
			}
			subs = append(subs, ls)
		}
		lh.subs = subs
		lh.moo.Unlock()
	case *flavors.Instance:
		lh.moo.Lock()
		for i, ls := range lh.subs {
			if ls.sub.self == ts {
				copy(lh.subs[i:], lh.subs[i+1:])
				lh.subs = lh.subs[:len(lh.subs)-1]
				cnt = 1
				break
			}
		}
		lh.moo.Unlock()
	}
	return cnt
}

func (caller localHubUnsubscribeCaller) Docs() string {
	return `__:unsubscribe__ _subscriber_ => _fixnum_
   __subscriber_ can be either a subject or a specific subscriber instance.


Returns the number of instances unsubscribed.
`
}

type localHubSubscribersCaller struct{}

func (caller localHubSubscribersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	lh := self.Any.(*localHub)
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
	lh.moo.Lock()
	for _, ls := range lh.subs {
		if len(subject) == 0 || subjectMatch(subject, ls.filter) {
			subs = append(subs, ls.sub.self)
		}
	}
	lh.moo.Unlock()

	return subs
}

func (caller localHubSubscribersCaller) Docs() string {
	return `__:subscribers__  &optional _subject_ => _list_
   _subject_ to filter the subscriber list.


Returns a list of _subscriber-flavor_ instances that have subscribed to _subject_.
A _nil_ _subject_ matches any subscriber..
`
}

func subjectMatch(subject, filter []string) bool {
top:
	for i, f := range filter {
		if len(subject) <= i {
			return false
		}
		switch f {
		case "*":
			// match anything
		case ">":
			break top
		default:
			if subject[i] != f {
				return false
			}
		}
	}
	return true
}
