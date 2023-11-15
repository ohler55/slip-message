// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/ohler55/ojg/oj"
	"github.com/ohler55/ojg/sen"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"
	"github.com/ohler55/slip/pkg/gi"
)

// The app-hub is primarily for testing. As such no effort has been made to
// optimize encoding and decoding but instead encoding is setup to be similar
// if not the same as remote message hubs but always encoding as a string.

var (
	appHubFlavor *flavors.Flavor
	encoder      = slip.Printer{
		ANSI:        false,
		Array:       false,
		Base:        10,
		Case:        slip.Symbol(":downcase"),
		Circle:      false,
		Escape:      true,
		Gensym:      true,
		Lambda:      false,
		Length:      math.MaxInt,
		Level:       math.MaxInt,
		Lines:       math.MaxInt,
		Prec:        -1,
		MiserWidth:  0,
		Pretty:      false,
		Radix:       false,
		Readably:    false,
		RightMargin: 10000,
	}
)

type appSub struct {
	filter []string
	sub    *subscription
	queue  chan slip.Object
}

func (as *appSub) loop(s *slip.Scope) {
	for {
		msg := <-as.queue
		if msg == nil {
			break
		}

		// TBD convert to expected type then callback

		if as.sub.callback != nil {
			_ = as.sub.callback.Call(s, slip.List{msg}, 0)
		} else {
			// TBD if caller is nil then keep on queue or maybe keep a list
			fmt.Printf("*** %s msg %s\n", as.sub.subject, msg)
		}
	}
}

type appHub struct {
	subs []*appSub
	mu   sync.Mutex // for subs list as well as distribution
}

func init() {
	appHubFlavor = flavors.DefFlavor("app-hub-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A app-hub is an in-memory message distribution hub.`),
			},
		},
	)
	appHubFlavor.DefMethod(":init", "", appHubInitCaller{})
	appHubFlavor.DefMethod(":subscribe", "", appHubSubscribeCaller{})
	appHubFlavor.DefMethod(":unsubscribe", "", appHubUnsubscribeCaller{})
	appHubFlavor.DefMethod(":subscribers", "", appHubSubscribersCaller{})
	appHubFlavor.DefMethod(":publish", "", appHubPublishCaller{})
	// appHubFlavor.DefMethod(":request", "", appHubRequestCaller{})
	// appHubFlavor.DefMethod(":configure-subject", "", appHubConfigureSubjectCaller{})
	// appHubFlavor.DefMethod(":close", "", appHubCloseCaller{})
}

type appHubInitCaller struct{}

func (caller appHubInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any = &appHub{}

	return nil
}

func (caller appHubInitCaller) Docs() string {
	return `__:init__


Sets the initial values when _make-instance_ is called.
`
}

type appHubSubscribeCaller struct{}

func (caller appHubSubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var (
		ct   slip.Object
		name slip.Object
	)
	for i := 2; i < len(args); i += 2 {
		switch args[i] {
		case slip.Symbol(":content-type"):
			ct = args[i+1]
		case slip.Symbol(":name"):
			name = args[i+1]
		default:
			slip.PanicType("&key", args[i], ":name", ":content-type")
		}
	}
	var sub *subscription
	subscriber, sub = makeSubscriber(self, args[0], args[1], ct, name)
	as := appSub{filter: strings.Split(sub.subject, "."), sub: sub, queue: make(gi.Channel, 32)}
	go as.loop(s)
	ah.mu.Lock()
	ah.subs = append(ah.subs, &as)
	ah.mu.Unlock()

	return
}

func (caller appHubSubscribeCaller) Docs() string {
	return `__:subscribe__ _subject_ _callback_ &optional _content-type_ => _instance_
   _subject_ to listen on.
   _callback_ can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.
   _content-type_is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.


Returns a _subscriber-flavor_ instance that represents a subscription on the _subject_.
`
}

type appHubUnsubscribeCaller struct{}

func (caller appHubUnsubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var removed []*appSub
	switch ts := args[0].(type) {
	case slip.String:
		var subs []*appSub
		subject := strings.Split(string(ts), ".")
		ah.mu.Lock()
		for _, as := range ah.subs {
			if subjectMatch(as.filter, subject) {
				removed = append(removed, as)
				continue
			}
			subs = append(subs, as)
		}
		ah.subs = subs
		ah.mu.Unlock()
	case *flavors.Instance:
		ah.mu.Lock()
		for i, as := range ah.subs {
			if as.sub.self == ts {
				copy(ah.subs[i:], ah.subs[i+1:])
				ah.subs = ah.subs[:len(ah.subs)-1]
				removed = append(removed, as)
				break
			}
		}
		ah.mu.Unlock()
	}
	for _, as := range removed {
		as.queue <- nil
	}
	return slip.Fixnum(len(removed))
}

func (caller appHubUnsubscribeCaller) Docs() string {
	return `__:unsubscribe__ _subscriber_ => _fixnum_
   __subscriber_ can be either a subject or a specific subscriber instance.


Returns the number of instances unsubscribed.
`
}

type appHubSubscribersCaller struct{}

func (caller appHubSubscribersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
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
	ah.mu.Lock()
	for _, ls := range ah.subs {
		if len(subject) == 0 || subjectMatch(subject, ls.filter) {
			subs = append(subs, ls.sub.self)
		}
	}
	ah.mu.Unlock()

	return subs
}

func (caller appHubSubscribersCaller) Docs() string {
	return `__:subscribers__  &optional _subject_ => _list_
   _subject_ to filter the subscriber list.


Returns a list of _subscriber-flavor_ instances that have subscribed to _subject_.
A _nil_ _subject_ matches any subscriber..
`
}

type appHubPublishCaller struct{}

func (caller appHubPublishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected  2 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var (
		subject []string
		msg     slip.Object
	)
	if ss, ok := args[0].(slip.String); ok {
		subject = strings.Split(string(ss), ".")
	} else {
		slip.PanicType("subject", args[0], "string")
	}
	switch tm := args[1].(type) {
	case slip.String:
		msg = tm
	case *flavors.Instance:
		if tm.Flavor == bag.Flavor() {
			if 2 < len(args) && args[2] == slip.Symbol(":sen") {
				msg = slip.String(sen.String(tm.Any))
			} else {
				msg = slip.String(oj.JSON(tm.Any))
			}
		} else {
			slip.PanicType("message", args[0], "string", "bag-flavor instance", "lisp data object")
		}
	default:
		msg = slip.String(encoder.Append(nil, tm, 0))
	}
	ah.mu.Lock()
	for _, as := range ah.subs {
		if len(subject) == 0 || subjectMatch(subject, as.filter) {
			as.queue <- msg
		}
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubPublishCaller) Docs() string {
	return `__:publish__ _subject_ _message_ &optional _content-type_
   _subject_ to publish the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.


Published a message which is delivered to any _subscribers_ matching the _subject_.
`
}

func subjectMatch(subject, filter []string) bool {
	var (
		i int
		f string
	)
	for i, f = range filter {
		if len(subject) <= i {
			return false
		}
		switch f {
		case "*":
			// match anything
		case ">":
			return true
		default:
			if subject[i] != f {
				return false
			}
		}
	}
	return i+1 == len(subject)
}
