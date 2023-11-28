// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
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

	addQueueDocs = `__:add-queue__ _name_ _retention_ _consumers_ &optional _max-messages_ => _nil_
   _name_ of the queue.
   _retention_ either _:work_ for a work queue or _:all_ for a queue that provides for all consumers.
   _consumers_ a list of consumer names.
   _max-messages_ maximum number of messages to queue before blocking


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

func subscriberFromArgs(self *flavors.Instance, args slip.List) (subscriber slip.Object, sub *subscription) {
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
