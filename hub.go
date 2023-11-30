// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ohler55/ojg/oj"
	"github.com/ohler55/ojg/sen"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"
)

const (
	subscribeDocs = `__:subscribe__ _subject_ _callback_ &key _content-type_ _name_ => _instance_
   _subject_ to listen on.
   _callback_ can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.
   _:content-type_ is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.
   _:name_ of the subscriber is used with work queues.


Returns a _subscriber-flavor_ instance that represents a subscription on the _subject_.
`

	unsubscribeDocs = `__:unsubscribe__ _subscriber_ => _fixnum_
   __subscriber_ can be either a subject or a specific subscriber instance.


Returns the number of instances unsubscribed.
`

	subscribersDocs = `__:subscribers__  &optional _subject_ => _list_
   _subject_ to filter the subscriber list.


Returns a list of _subscriber-flavor_ instances that have subscribed to _subject_.
A _nil_ _subject_ matches any subscriber.
`

	publishDocs = `__:publish__ _subject_ _message_ &optional _content-type_ => _nil_
   _subject_ to publish the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.


Publish a message which is delivered to any _subscribers_ matching the _subject_.
`

	requestDocs = `__:request__ _subject_ _message_ &key _content-type_ _timeout_
   _subject_ to request the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _:content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Send a request message on _subject_ and wait for a reply.
`

	closeDocs = `__:close__ => _nil_


Close the hub.
`

	addQueueDocs = `__:add-queue__ _name_ _retention_ _consumers_ &key _max-messages_ _subjects_ => _nil_
   _name_ of the queue.
   _retention_ either _:work_ for a work queue or _:all_ for a queue that provides for all consumers.
   _consumers_ a list of consumer names. For work queues there can only be one consumer name.
   _max-messages_ maximum number of messages to queue before blocking
   _subjects_ to listen on. If none are provided then the queue _name_ is used as the only subject.


Add a queue with the provided parameters.
`

	queuesDocs = `__:queues__ => _list_


Returns a list of queue descriptions consisting the queue name, retention, and the consumers.
`

	closeQueueDocs = `__:close-queue__ _name_ => _nil_


Close a queue.
`

	nextDocs = `__:next__ _subscriber_ &key _timeout_ => _object_, _fixnum_
   _subscriber_ must be a queue subscriber.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Get the next message on a queue and return the message and message identifier.
`

	ackDocs = `__:ack__ _subscriber_ _message-id_ => _nil_
   _subscriber_ must be a queue subscriber.
   _message-id_ is the identifier for the message to ACK.


ACK a message for the subscriber.
`
)

func subscriberFromArgs(self *flavors.Instance, args slip.List) (
	subscriber slip.Object, sub *subscription, subject string) {
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
	return makeSubscriber(self, args[0], args[1], ct, name)
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

func getRequestMsg(s *slip.Scope, args slip.List) (
	self *flavors.Instance, subject string, msg slip.Object, timeout time.Duration) {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected at least 2 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
	var (
		useSen bool
	)
	timeout = time.Second
	if ss, ok := args[0].(slip.String); ok {
		subject = string(ss)
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

	return
}

func getAddQueueArgs(s *slip.Scope, args slip.List) (
	self *flavors.Instance, name string, all bool, maxMsgs int, consumers, subjects []string) {

	if len(args) < 3 || 7 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 3 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
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
	for i := 3; i < len(args); i += 2 {
		switch args[i] {
		case slip.Symbol(":max-messages"):
			if num, ok := args[i+1].(slip.Fixnum); ok && 0 < num {
				maxMsgs = int(num)
			} else {
				slip.PanicType(":max-messages", args[i+1], "fixnum")
			}
		case slip.Symbol(":subjects"):
			if sa, ok := args[i+1].(slip.List); ok {
				for _, v := range sa {
					if ss, ok2 := v.(slip.String); ok2 {
						subjects = append(subjects, string(ss))
					} else {
						slip.PanicType(":subjects elements", v, "string")
					}
				}
			} else {
				slip.PanicType(":subjects", args[i+1], "list")
			}
		default:
			slip.PanicType("&key", args[i], ":timeout", ":content-type")
		}
	}
	if len(subjects) == 0 {
		subjects = []string{name}
	}
	return
}

func callMsgCallback(s *slip.Scope, m *nats.Msg, jsub *jsSub) (result slip.Object) {
	defer func() {
		switch rec := recover().(type) {
		case nil, slip.Object:
			// leave as is
		default:
			result = slip.NewError("%s", rec)
		}
	}()
	msg := decodeMessage(slip.String(m.Data), jsub.sub.contentType)
	if jsub.sub.callback != nil {
		reply := jsub.sub.callback.Call(s, slip.List{msg}, 0)
		if 0 < len(m.Reply) {
			if err := m.Respond([]byte(encodeMsg(reply, false).(slip.String))); err != nil {
				panic(err)
			}
		}
	}
	return
}

func safeCall(s *slip.Scope, caller slip.Caller, args slip.List) (result slip.Object) {
	defer func() {
		switch rec := recover().(type) {
		case nil, slip.Object:
			// leave as is
		default:
			result = slip.NewError("%s", rec)
		}
	}()
	return caller.Call(s, args, 0)
}
