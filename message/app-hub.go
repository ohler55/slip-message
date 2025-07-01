// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ohler55/slip"
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
	queues []queue
	mu     sync.Mutex // for subs list as well as distribution
}

func defAppHubFlavor() {
	appHubFlavor = flavors.DefFlavor("app-hub-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A app-hub is an in-memory message distribution hub.`),
			},
		},
		&Pkg,
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
	appHubFlavor.DefMethod(":ack", "", appHubAckCaller{})
}

type appHubInitCaller struct{}

func (caller appHubInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any = &appHub{}

	return nil
}

func (caller appHubInitCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":init",
		Text: "Sets the initial value when _make-instance_ is called.",
	}
}

type appHubSubscribeCaller struct{}

func (caller appHubSubscribeCaller) Call(s *slip.Scope, args slip.List, _ int) (subscriber slip.Object) {
	self := s.Get("self").(*flavors.Instance)

	var sub *subscription
	subscriber, sub, _ = subscriberFromArgs(self, args)
	as := appSub{sub: sub, queue: make(gi.Channel, 32), done: make(chan struct{}, 1)}
	go as.loop(s)

	ah := self.Any.(*appHub)
	ah.mu.Lock()
	ah.subs = append(ah.subs, &as)
	ah.mu.Unlock()

	return
}

func (caller appHubSubscribeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":subscribe",
		Text: `Returns a _subscriber-flavor_ instance that represents a subscription on the _subject_.`,
		Args: []*slip.DocArg{
			{
				Name: "subject",
				Type: "string",
				Text: "Subject to listen on or queue to be a consumer of.",
			},
			{
				Name: "callback",
				Type: "function|nil",
				Text: `Can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.`,
			},
			{Name: "&key"},
			{
				Name: ":content-type",
				Type: "symbol",
				Text: `An optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.`,
			},
			{
				Name: ":name",
				Type: "string",
				Text: `Name of the subscriber is used with work queues.`,
			},
		},
		Return: "subscriber-flavor",
	}
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
			if subjectMatch(as.sub.subject, subject) {
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

func (caller appHubUnsubscribeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":subscribe",
		Text: `Returns the number of instances unsubscribed.`,
		Args: []*slip.DocArg{
			{
				Name: "subscriber",
				Type: "string|subscriber-flavor",
				Text: "Either a subject or a specific subscriber instance.",
			},
		},
		Return: "fixnum",
	}
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
		if len(subject) == 0 || subjectMatch(subject, ls.sub.subject) {
			subs = append(subs, ls.sub.self)
		}
	}
	ah.mu.Unlock()

	return subs
}

func (caller appHubSubscribersCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":subscribers",
		Text: `Returns a list of _subscriber-flavor_ instances that have subscribed to _subject_.
A _nil_ _subject_ matches any subscriber.`,
		Args: []*slip.DocArg{
			{
				Name: "subject",
				Type: "string",
				Text: "Subject to filter the subscriber list.",
			},
		},
		Return: "list",
	}
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
		if len(subject) == 0 || subjectMatch(subj, as.sub.subject) {
			as.queue <- msg
		}
	}
	for _, q := range ah.queues {
		if q.subjectMatch(subj) {
			q.push(msg)
		}
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubPublishCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":publish",
		Text: `Publish a message which is delivered to any _subscribers_ matching the _subject_.`,
		Args: []*slip.DocArg{
			{
				Name: "subject",
				Type: "string",
				Text: "Subject to publish the message on.",
			},
			{
				Name: "message",
				Type: "string|bag|object",
				Text: `Either a _string_ for :raw content, a _bag_ for JSON or SEN format, or
an sexpression for _lisp_ content.`,
			},
			{Name: "&optional"},
			{
				Name: "content-type",
				Type: "symbol",
				Text: `Content type of the message which is in effect for encoding instances of the
_bag-flavor_ and can be _:json_ or _:sen_.`,
			},
		},
	}
}

type appHubRequestCaller struct{}

func (caller appHubRequestCaller) Call(s *slip.Scope, args slip.List, _ int) (reply slip.Object) {
	self, subject, msg, timeout := getRequestMsg(s, args)
	subj := strings.Split(subject, ".")

	ah := self.Any.(*appHub)

	replies := make(gi.Channel, 1)
	defer close(replies)

	// The first subscriber to reply is the return value. Others are ignored.
	ah.mu.Lock()
	for _, as := range ah.subs {
		if len(subj) == 0 || subjectMatch(subj, as.sub.subject) {
			as.queue <- slip.Values{msg, replies}
		}
	}
	ah.mu.Unlock()
	select {
	case reply = <-replies:
		// got a reply
	case <-time.After(timeout):
		slip.NewPanic("request to %s timed out after %s", subject, timeout)
	}
	return
}

func (caller appHubRequestCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":request",
		Text: `Send a request message on _subject_ and wait for a reply.`,
		Args: []*slip.DocArg{
			{
				Name: "subject",
				Type: "string",
				Text: "Subject to publish the request message on.",
			},
			{
				Name: "message",
				Type: "string|bag|object",
				Text: `Either a _string_ for :raw content, a _bag_ for JSON or SEN format, or
an sexpression for _lisp_ content.`,
			},
			{Name: "&key"},
			{
				Name: ":content-type",
				Type: "symbol",
				Text: `Content type of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.`,
			},
			{
				Name: ":timeout",
				Type: "real",
				Text: `A number denoting the seconds to wait for a reply before a timeout panic.`,
			},
		},
		Return: "bag",
	}
}

type appHubCloseCaller struct{}

func (caller appHubCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	ah.mu.Lock()
	for _, as := range ah.subs {
		as.shutdown()
	}
	for _, q := range ah.queues {
		q.shutdown()
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubCloseCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":close",
		Text: `Closes the hub.`,
	}
}

type appHubAddQueueCaller struct{}

func (caller appHubAddQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, name, all, maxMsgs, consumers, subjects := getAddQueueArgs(s, args)
	ah := self.Any.(*appHub)

	ah.mu.Lock()
	if all {
		ah.queues = append(ah.queues, newAllQueue(name, maxMsgs, consumers, subjects))
	} else {
		ah.queues = append(ah.queues, newWorkQueue(name, maxMsgs, consumers, subjects))
	}
	ah.mu.Unlock()

	return nil
}

func (caller appHubAddQueueCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":add-queue",
		Text: `Add a queue with the provided parameters.`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: "Name of the queue.",
			},
			{
				Name: "retention",
				Type: "symbol",
				Text: `Either _:work_ for a work queue or _:all_ for a queue that provides for all consumers.`,
			},
			{Name: "&key"},
			{
				Name: ":max-messages",
				Type: "fixnum",
				Text: `The maximum number of messages to queue before blocking.`,
			},
			{
				Name: ":subjects",
				Type: "list",
				Text: `Subjects to listen on. If none are provided then the queue _name_ is used as the only subject.`,
			},
		},
	}
}

type appHubQueuesCaller struct{}

func (caller appHubQueuesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	list := make(slip.List, 0, len(ah.queues))
	ah.mu.Lock()
	for _, q := range ah.queues {
		list = append(list, q.appendAssoc(nil))
	}
	ah.mu.Unlock()

	return list
}

func (caller appHubQueuesCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":queues",
		Text:   `Returns a list of queue descriptions consisting the queue name, retention, and the consumers.`,
		Return: "list",
	}
}

type appHubCloseQueueCaller struct{}

func (caller appHubCloseQueueCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ah := self.Any.(*appHub)
	if ss, ok := args[0].(slip.String); ok {
		name := string(ss)
		ah.mu.Lock()
		var found queue
		for i, q := range ah.queues {
			if name == q.qname() {
				found = q
				if i < len(ah.queues)-1 {
					copy(ah.queues[i:], ah.queues[i+1:])
				}
				ah.queues = ah.queues[:len(ah.queues)-1]
				break
			}
		}
		ah.mu.Unlock()
		if found != nil {
			found.shutdown()
		}
	} else {
		slip.PanicType("name", args[0], "string")
	}
	return nil
}

func (caller appHubCloseQueueCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":close-queue",
		Text: `Close a queue.`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: "Name of the queue to close.",
			},
		},
	}
}

type appHubNextCaller struct{}

func (caller appHubNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, sub, timeout := getNextArgs(s, args)
	ah := self.Any.(*appHub)
	var found queue
	ah.mu.Lock()
	for _, q := range ah.queues {
		if q.subjectMatch(sub.subject) {
			found = q
			break
		}
	}
	ah.mu.Unlock()
	if found != nil {
		if msg, mid := found.next(sub.name, sub.contentType, timeout); msg != nil {
			return slip.Values{msg, slip.Fixnum(mid)}
		}
	}
	return nil
}

func (caller appHubNextCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":next",
		Text: `Get the next message on a queue and return the message and message identifier.`,
		Args: []*slip.DocArg{
			{
				Name: "subscriber",
				Type: "subscriber-flavor",
				Text: "Must be a queue subscriber.",
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `A number denoting the seconds to wait for a reply before a timeout panic.`,
			},
		},
		Return: "bag, fixnum",
	}
}

type appHubAckCaller struct{}

func (caller appHubAckCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self, sub, mid := getAckArgs(s, args)
	ah := self.Any.(*appHub)

	var found queue
	ah.mu.Lock()
	for _, q := range ah.queues {
		if q.subjectMatch(sub.subject) {
			found = q
			break
		}
	}
	ah.mu.Unlock()
	if found != nil {
		found.ack(sub.name, mid)
	}
	return nil
}

func (caller appHubAckCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":ack",
		Text: `ACK a message for the subscriber.`,
		Args: []*slip.DocArg{
			{
				Name: "subscriber",
				Type: "subscriber-flavor",
				Text: "Must be a queue subscriber.",
			},
			{
				Name: "message-id",
				Type: "fixnum",
				Text: `The identifier for the message to ACK.`,
			},
		},
	}
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
