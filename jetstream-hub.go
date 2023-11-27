// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

// The jetstream-hub is primarily for testing. As such no effort has been made to
// optimize encoding and decoding but instead encoding is setup to be similar
// if not the same as remote message hubs but always encoding as a string.

var (
	jetstreamHubFlavor *flavors.Flavor
)

func init() {
	jetstreamHubFlavor = flavors.DefFlavor("jetstream-hub-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A jetstream-hub is an in-memory message distribution hub.`),
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

	// TBD get optional url
	//  also tls or credentials or jwt and nkeys
	//  tls-ca
	//  tls-cert
	//  tls-key
	//  credentials
	//  nkey
	//  jwt
	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := jetstream.New(nc)

	self.Any = js

	return nil
}

func (caller jetstreamHubInitCaller) Docs() string {
	return `__:init__


Sets the initial values when _make-instance_ is called.
`
}

type jetstreamHubSubscribeCaller struct{}

func (caller jetstreamHubSubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return
}

func (caller jetstreamHubSubscribeCaller) Docs() string {
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

type jetstreamHubUnsubscribeCaller struct{}

func (caller jetstreamHubUnsubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return slip.Fixnum(0)
}

func (caller jetstreamHubUnsubscribeCaller) Docs() string {
	return `__:unsubscribe__ _subscriber_ => _fixnum_
   __subscriber_ can be either a subject or a specific subscriber instance.


Returns the number of instances unsubscribed.
`
}

type jetstreamHubSubscribersCaller struct{}

func (caller jetstreamHubSubscribersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubSubscribersCaller) Docs() string {
	return `__:subscribers__  &optional _subject_ => _list_
   _subject_ to filter the subscriber list.


Returns a list of _subscriber-flavor_ instances that have subscribed to _subject_.
A _nil_ _subject_ matches any subscriber.
`
}

type jetstreamHubPublishCaller struct{}

func (caller jetstreamHubPublishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 2 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubPublishCaller) Docs() string {
	return `__:publish__ _subject_ _message_ &optional _content-type_ => _nil_
   _subject_ to publish the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.


Publish a message which is delivered to any _subscribers_ matching the _subject_.
`
}

type jetstreamHubRequestCaller struct{}

func (caller jetstreamHubRequestCaller) Call(s *slip.Scope, args slip.List, _ int) (reply slip.Object) {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected at least 2 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return
}

func (caller jetstreamHubRequestCaller) Docs() string {
	return `__:request__ _subject_ _message_ &key _content-type_ _timeout_
   _subject_ to request the message on
   _message_ either a _string_ for :raw content, a _bag_ for JSON or SEN format, or an sexpression for _lisp_ content.
   _:content-type_ of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Send a request message on _subject_ and wait for a reply.
`
}

type jetstreamHubCloseCaller struct{}

func (caller jetstreamHubCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubCloseCaller) Docs() string {
	return `__:close__ => _nil_


Close the hub.
`
}

type jetstreamHubAddQueueCaller struct{}

func (caller jetstreamHubAddQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 3 || 4 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubAddQueueCaller) Docs() string {
	return `__:add-queue__ _name_ _retention_ _consumers_ &optional _max-messages_ => _nil_
   _name_ of the queue.
   _retention_ either _:work_ for a work queue or _:all_ for a queue that provides for all consumers.
   _consumers_ a list of consumer names.
   _max-messages_ maximum number of messages to queue before blocking


Add a queue with the provided parameters.
`
}

type jetstreamHubQueuesCaller struct{}

func (caller jetstreamHubQueuesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubQueuesCaller) Docs() string {
	return `__:queues__ => _list_


Returns a list of queue descriptions consisting the queue name, retention, and the consumers.
`
}

type jetstreamHubCloseQueueCaller struct{}

func (caller jetstreamHubCloseQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubCloseQueueCaller) Docs() string {
	return `__:close-queue__ _name_ => _nil_


Close a queue.
`
}

type jetstreamHubNextCaller struct{}

func (caller jetstreamHubNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 1 || 3 < len(args) {
		slip.NewPanic("Incorrect argument count. Expected 1 or 3 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubNextCaller) Docs() string {
	return `__:next__ _subscriber_ &key _timeout_ => _object_, _fixnum_
   _subscriber_ must be a queue subscriber.
   _:timeout_ is a real number denoting the seconds to wait for a reply before a timeout panic.


Get the next message on a queue and return the message and message identifier.
`
}

type jetstreamHubAckCaller struct{}

func (caller jetstreamHubAckCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	if len(args) < 2 {
		slip.NewPanic("Incorrect argument count. Expected 2 but got %d.", len(args))
	}
	self := s.Get("self").(*flavors.Instance)
	js := self.Any.(jetstream.JetStream)

	// TBD
	fmt.Printf("*** js: %v\n", js)

	return nil
}

func (caller jetstreamHubAckCaller) Docs() string {
	return `__:ack__ _subscriber_ _message-id_ => _nil_
   _subscriber_ must be a queue subscriber.
   _message-id_ is the identifier for the message to ACK.


ACK a message for the subscriber.
`
}
