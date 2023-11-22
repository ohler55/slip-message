// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

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

type appHub struct {
	subs   []*appSub
	queues map[string]queue
	mu     sync.Mutex // for subs list as well as distribution
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
	appHubFlavor.DefMethod(":request", "", appHubRequestCaller{})
	appHubFlavor.DefMethod(":close", "", appHubCloseCaller{})
	appHubFlavor.DefMethod(":add-queue", "", appHubAddQueueCaller{})
	appHubFlavor.DefMethod(":close-queue", "", appHubCloseQueueCaller{})
	appHubFlavor.DefMethod(":queues", "", appHubQueuesCaller{})
	appHubFlavor.DefMethod(":next", "", appHubNextCaller{})
}

type appHubInitCaller struct{}

func (caller appHubInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any = &appHub{queues: map[string]queue{}}

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
	return `__:subscribe__ _subject_ _callback_ &key _content-type_ _name_ => _instance_
   _subject_ to listen on.
   _callback_ can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.
   _:content-type_ is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.
   _:name_ of the subscriber is used with work queues.


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
A _nil_ _subject_ matches any subscriber.
`
}

type appHubPublishCaller struct{}

func (caller appHubPublishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 2 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var (
		subject string
		subj    []string
		msg     slip.Object
	)
	if ss, ok := args[0].(slip.String); ok {
		subject = string(ss)
		subj = strings.Split(subject, ".")
	} else {
		slip.PanicType("subject", args[0], "string")
	}
	msg = encodeMsg(args[1], 2 < len(args) && args[2] == slip.Symbol(":sen"))
	ah.mu.Lock()
	for _, as := range ah.subs {
		if len(subject) == 0 || subjectMatch(subj, as.filter) {
			as.queue <- msg
		}
	}
	q := ah.queues[subject]
	ah.mu.Unlock()
	if q != nil {
		q.push(msg)
	}
	return nil
}

func (caller appHubPublishCaller) Docs() string {
	return `__:publish__ _subject_ _message_ &optional _content-type_ => _nil_
   _subject_ to publish the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.


Publish a message which is delivered to any _subscribers_ matching the _subject_.
`
}

type appHubRequestCaller struct{}

func (caller appHubRequestCaller) Call(s *slip.Scope, args slip.List, _ int) (reply slip.Object) {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected at least 2 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var (
		subject []string
		msg     slip.Object
		useSen  bool
	)
	timeout := time.Second
	if ss, ok := args[0].(slip.String); ok {
		subject = strings.Split(string(ss), ".")
	} else {
		slip.PanicType("subject", args[0], "string")
	}
	for i := 2; i < len(args); i += 2 {
		switch args[i] {
		case slip.Symbol(":content-type"):
			useSen = args[i+1] == slip.Symbol(":sen")
		case slip.Symbol(":timeout"):
			if rn, ok := args[i+1].(slip.Real); ok {
				timeout = time.Duration(rn.RealValue() * float64(time.Second))
			} else {
				slip.PanicType("timeout", args[i+1], "real")
			}
		default:
			slip.PanicType("&key", args[i], ":timeout", ":content-type")
		}
	}
	msg = encodeMsg(args[1], useSen)
	replies := make(gi.Channel, 1)
	defer close(replies)

	// The first subscriber to reply is the return value. Others are ignored.
	ah.mu.Lock()
	for _, as := range ah.subs {
		if len(subject) == 0 || subjectMatch(subject, as.filter) {
			as.queue <- slip.Values{msg, replies}
		}
	}
	ah.mu.Unlock()
	select {
	case reply = <-replies:
		// got a reply
	case <-time.After(timeout):
		slip.NewPanic("request to %s timed out after %s", strings.Join(subject, "."), timeout)
	}
	return
}

func (caller appHubRequestCaller) Docs() string {
	return `__:request__ _subject_ _message_ &key _content-type_ _timeout_
   _subject_ to request the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _:content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Send a request message on _subject_ and wait for a reply.
`
}

type appHubCloseCaller struct{}

func (caller appHubCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	ah.mu.Lock()
	for _, as := range ah.subs {
		as.queue <- nil
	}
	for _, q := range ah.queues {
		q.shutdown()
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubCloseCaller) Docs() string {
	return `__:close__ => _nil_


Close the hub.
`
}

type appHubAddQueueCaller struct{}

func (caller appHubAddQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 3 || 4 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var (
		name      string
		all       bool
		maxMsgs   int
		consumers []string
	)
	if ss, ok := args[0].(slip.String); ok {
		name = string(ss)
	} else {
		slip.PanicType("name", args[0], "string")
	}
	switch args[1] {
	case slip.Symbol(":work"):
		// all remains false
	case slip.Symbol(":all"):
		all = true
	default:
		slip.PanicType("retention", args[1], ":work", ":all")
	}
	if list, ok := args[2].(slip.List); ok {
		consumers = make([]string, len(list))
		for i, v := range list {
			if ss, ok2 := v.(slip.String); ok2 {
				consumers[i] = string(ss)
			} else {
				slip.PanicType("consumers element", v, "string")
			}
		}
	} else {
		slip.PanicType("consumers", args[2], "list of strings")
	}
	if 3 < len(args) {
		if num, ok := args[3].(slip.Fixnum); ok {
			maxMsgs = int(num)
		} else {
			slip.PanicType("max-messages", args[3], "fixnum")
		}
	}
	ah.mu.Lock()
	if all {
		ah.queues[name] = newAllQueue(name, consumers)
	} else {
		ah.queues[name] = newWorkQueue(name, maxMsgs, consumers)
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubAddQueueCaller) Docs() string {
	return `__:add-queue__ _name_ _retention_ _consumers_ &optional _max-messages_ => _nil_
   _name_ of the queue.
   _retention_ either _:work_ for a work queue or _:all_ for a queue that provides for all consumers.
   _consumers_ a list of consumer names.
   _max-messages_ maximum number of messages to queue before blocking


Add a queue with the provided parameters.
`
}

type appHubQueuesCaller struct{}

func (caller appHubQueuesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	list := make(slip.List, 0, len(ah.queues))
	ah.mu.Lock()
	keys := make([]string, 0, len(ah.queues))
	for k := range ah.queues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		list = append(list, ah.queues[k].asLisp())
	}
	ah.mu.Unlock()

	return list
}

func (caller appHubQueuesCaller) Docs() string {
	return `__:queues__ => _list_


Returns a list of queue descriptions consisting the queue name, retention, and the consumers.
`
}

type appHubCloseQueueCaller struct{}

func (caller appHubCloseQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	var q queue
	if ss, ok := args[0].(slip.String); ok {
		ah.mu.Lock()
		q = ah.queues[string(ss)]
		delete(ah.queues, string(ss))
		ah.mu.Unlock()
		if q != nil {
			q.shutdown()
		}
	} else {
		slip.PanicType("name", args[0], "string")
	}
	return nil
}

func (caller appHubCloseQueueCaller) Docs() string {
	return `__:close-queue__ _name_ => _nil_


Close a queue.
`
}

type appHubNextCaller struct{}

func (caller appHubNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 1 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 1 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	inst, ok := args[0].(*flavors.Instance)
	if !ok || inst.Flavor != subscriberFlavor {
		slip.PanicType("subscriber", args[0], "subscriber-flavor instance")
	}
	sub := inst.Any.(*subscription)
	var timeout time.Duration
	for i := 1; i < len(args); i += 2 {
		if args[i] == slip.Symbol(":timeout") {
			if rn, ok := args[i+1].(slip.Real); ok {
				timeout = time.Duration(rn.RealValue() * float64(time.Second))
			} else {
				slip.PanicType("timeout", args[i+1], "real")
			}
		} else {
			slip.PanicType("&key", args[i], ":timeout")
		}
	}
	ah.mu.Lock()
	q := ah.queues[sub.subject]
	ah.mu.Unlock()
	if q != nil {
		if msg, mid := q.next(sub.name, sub.contentType, timeout); msg != nil {
			return slip.Values{msg, slip.Fixnum(mid)}
		}
	}
	return nil
}

func (caller appHubNextCaller) Docs() string {
	return `__:next__ _subscriber_ &key _timeout_ => _object_, _fixnum_
   _subscriber_ must be a queue subscriber.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Get the next message on a queue and return the message and message identifier.
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

func encodeMsg(m slip.Object, useSen bool) (msg slip.Object) {
	switch tm := m.(type) {
	case slip.String:
		msg = tm
	case *flavors.Instance:
		if tm.Flavor == bag.Flavor() {
			if useSen {
				msg = slip.String(sen.String(tm.Any))
			} else {
				msg = slip.String(oj.JSON(tm.Any))
			}
		} else {
			slip.PanicType("message", m, "string", "bag-flavor instance", "lisp data object")
		}
	default:
		msg = slip.String(encoder.Append(nil, tm, 0))
	}
	return
}
