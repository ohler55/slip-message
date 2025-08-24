// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ohler55/ojg/oj"
	"github.com/ohler55/ojg/sen"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	subscribeFuncDoc = slip.FuncDoc{
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

	unsubscribeFuncDoc = slip.FuncDoc{
		Name: ":unsubscribe",
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

	subscribersFuncDoc = slip.FuncDoc{
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

	publishFuncDoc = slip.FuncDoc{
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

	requestFuncDoc = slip.FuncDoc{
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

	closeFuncDoc = slip.FuncDoc{
		Name: ":close",
		Text: `Closes the hub.`,
	}

	addQueueFuncDoc = slip.FuncDoc{
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

	queuesFuncDoc = slip.FuncDoc{
		Name:   ":queues",
		Text:   `Returns a list of queue descriptions consisting the queue name, retention, and the consumers.`,
		Return: "list",
	}

	closeQueueFuncDoc = slip.FuncDoc{
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

	nextFuncDoc = slip.FuncDoc{
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

	ackFuncDoc = slip.FuncDoc{
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
)

func subscriberFromArgs(s *slip.Scope, self *flavors.Instance, args slip.List, depth int) (
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
			slip.TypePanic(s, depth, "&key", args[i], ":name", ":content-type")
		}
	}
	return makeSubscriber(s, self, args[0], args[1], ct, name, depth)
}

func encodeMsg(s *slip.Scope, m slip.Object, useSen bool, depth int) (msg slip.Object) {
	switch tm := m.(type) {
	case slip.String:
		msg = tm
	case *flavors.Instance:
		if tm.Type == bag.Flavor() {
			if useSen {
				msg = slip.String(sen.String(tm.Any))
			} else {
				msg = slip.String(oj.JSON(tm.Any))
			}
		} else {
			slip.TypePanic(s, depth, "message", m, "string", "bag-flavor instance", "lisp data object")
		}
	default:
		msg = slip.String(encoder.Append(nil, tm, 0))
	}
	return
}

func getRequestMsg(s *slip.Scope, args slip.List, depth int) (
	self *flavors.Instance, subject string, msg slip.Object, timeout time.Duration) {
	if len(args) < 2 {
		slip.ErrorPanic(s, depth, "Incorrect argument count. Expected at least 2 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
	var (
		useSen bool
	)
	timeout = time.Second
	if ss, ok := args[0].(slip.String); ok {
		subject = string(ss)
	} else {
		slip.TypePanic(s, depth, "subject", args[0], "string")
	}
	for i := 2; i < len(args); i += 2 {
		switch args[i] {
		case slip.Symbol(":content-type"):
			useSen = args[i+1] == slip.Symbol(":sen")
		case slip.Symbol(":timeout"):
			if rn, ok := args[i+1].(slip.Real); ok {
				timeout = time.Duration(rn.RealValue() * float64(time.Second))
			} else {
				slip.TypePanic(s, depth, "timeout", args[i+1], "real")
			}
		default:
			slip.TypePanic(s, depth, "&key", args[i], ":timeout", ":content-type")
		}
	}
	msg = encodeMsg(s, args[1], useSen, depth)

	return
}

func getAddQueueArgs(s *slip.Scope, args slip.List, depth int) (
	self *flavors.Instance, name string, all bool, maxMsgs int, consumers, subjects []string) {

	if len(args) < 3 || 7 < len(args) {
		slip.ErrorPanic(s, depth, "Incorrect argument count. Expected 3 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
	if ss, ok := args[0].(slip.String); ok {
		name = string(ss)
	} else {
		slip.TypePanic(s, depth, "name", args[0], "string")
	}
	switch args[1] {
	case slip.Symbol(":work"):
		// all remains false
	case slip.Symbol(":all"):
		all = true
	default:
		slip.TypePanic(s, depth, "retention", args[1], ":work", ":all")
	}
	if list, ok := args[2].(slip.List); ok {
		consumers = make([]string, len(list))
		for i, v := range list {
			if ss, ok2 := v.(slip.String); ok2 {
				consumers[i] = string(ss)
			} else {
				slip.TypePanic(s, depth, "consumers element", v, "string")
			}
		}
	} else {
		slip.TypePanic(s, depth, "consumers", args[2], "list of strings")
	}
	for i := 3; i < len(args); i += 2 {
		switch args[i] {
		case slip.Symbol(":max-messages"):
			if num, ok := args[i+1].(slip.Fixnum); ok && 0 < num {
				maxMsgs = int(num)
			} else {
				slip.TypePanic(s, depth, ":max-messages", args[i+1], "fixnum")
			}
		case slip.Symbol(":subjects"):
			if sa, ok := args[i+1].(slip.List); ok {
				for _, v := range sa {
					if ss, ok2 := v.(slip.String); ok2 {
						subjects = append(subjects, string(ss))
					} else {
						slip.TypePanic(s, depth, ":subjects elements", v, "string")
					}
				}
			} else {
				slip.TypePanic(s, depth, ":subjects", args[i+1], "list")
			}
		default:
			slip.TypePanic(s, depth, "&key", args[i], ":timeout", ":content-type")
		}
	}
	if len(subjects) == 0 {
		subjects = []string{name}
	}
	return
}

func getNextArgs(
	s *slip.Scope,
	args slip.List,
	depth int) (self *flavors.Instance, sub *subscription, timeout time.Duration) {

	if len(args) < 1 || 3 < len(args) {
		slip.ErrorPanic(s, depth, "Incorrect argument count. Expected 1 or 3 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
	inst, ok := args[0].(*flavors.Instance)
	if !ok || inst.Type != subscriberFlavor {
		slip.TypePanic(s, depth, "subscriber", args[0], "subscriber-flavor instance")
	}
	sub = inst.Any.(*subscription)
	for i := 1; i < len(args); i += 2 {
		if args[i] == slip.Symbol(":timeout") {
			if rn, ok := args[i+1].(slip.Real); ok {
				timeout = time.Duration(rn.RealValue() * float64(time.Second))
			} else {
				slip.TypePanic(s, depth, "timeout", args[i+1], "real")
			}
		} else {
			slip.TypePanic(s, depth, "&key", args[i], ":timeout")
		}
	}
	return
}

func getAckArgs(s *slip.Scope, args slip.List, depth int) (self *flavors.Instance, sub *subscription, msgID int64) {
	if len(args) < 2 {
		slip.ErrorPanic(s, depth, "Incorrect argument count. Expected 2 but got %d.", len(args))
	}
	self = s.Get("self").(*flavors.Instance)
	inst, ok := args[0].(*flavors.Instance)
	if !ok || inst.Type != subscriberFlavor {
		slip.TypePanic(s, depth, "subscriber", args[0], "subscriber-flavor instance")
	}
	sub = inst.Any.(*subscription)
	if num, ok2 := args[1].(slip.Fixnum); ok2 {
		msgID = int64(num)
	} else {
		slip.TypePanic(s, depth, "message-id", args[1], "fixnum")
	}
	return
}

func callMsgCallback(s *slip.Scope, m *nats.Msg, jsub *jsSub, depth int) (result slip.Object) {
	defer func() {
		switch rec := recover().(type) {
		case nil:
			// leave as is
		case slip.Object:
			result = rec
		default:
			result = slip.ErrorNew(s, depth, "%s", rec)
		}
	}()
	msg := decodeMessage(slip.String(m.Data), jsub.sub.contentType)
	reply := jsub.sub.callback.Call(s, slip.List{msg}, 0)
	if 0 < len(m.Reply) {
		if err := m.Respond([]byte(encodeMsg(s, reply, false, depth).(slip.String))); err != nil {
			panic(err)
		}
	}
	return
}

func safeCall(s *slip.Scope, caller slip.Caller, args slip.List, depth int) (result slip.Object) {
	defer func() {
		switch rec := recover().(type) {
		case nil, slip.Object:
			// leave as is
		default:
			result = slip.ErrorNew(s, depth, "%s", rec)
		}
	}()
	return caller.Call(s, args, 0)
}

func checkError(s *slip.Scope, where string, err error, depth int) {
	if err != nil {
		slip.ErrorPanic(s, depth, "%s: %s", where, err)
	}
}
